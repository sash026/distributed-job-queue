import argparse
import asyncio
import contextlib
import signal
import socket
import time
import traceback

from core import metrics
from core.redis_client import RedisClient
from core.schemas import Job, JobStatus
from worker.tasks import get_task_handler

PROCESSING_QUEUE_KEY = "queue:processing"
DEAD_LETTER_QUEUE_KEY = "queue:dead_letter"
WORKERS_ONLINE_KEY = "workers:online"
JOB_TTL_SECONDS = 60 * 60
PROCESSING_TTL_SECONDS = 30
HEARTBEAT_INTERVAL_SECONDS = 10
PER_QUEUE_POLL_TIMEOUT_SECONDS = 1
WORKER_REGISTRATION_INTERVAL_SECONDS = 10

redis_client = RedisClient()
WORKER_ID = socket.gethostname()


async def execute_task(job: Job) -> dict:
    handler = get_task_handler(job.name)
    if handler is None:
        raise ValueError(f"No task handler registered for job name '{job.name}'")
    return await handler(job.payload)


async def heartbeat(job_id: str) -> None:
    await redis_client.redis.set(f"processing_ttl:{job_id}", "1", ex=PROCESSING_TTL_SECONDS)


async def _heartbeat_loop(job_id: str) -> None:
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)
        await heartbeat(job_id)


def parse_queue_keys(queues_arg: str) -> list[str]:
    queue_names = [name.strip() for name in queues_arg.split(",") if name.strip()]
    return [f"queue:{name}" for name in queue_names]


async def _register_worker_loop() -> None:
    while True:
        await redis_client.redis.sadd(WORKERS_ONLINE_KEY, WORKER_ID)
        await asyncio.sleep(WORKER_REGISTRATION_INTERVAL_SECONDS)


async def worker_loop(queue_keys: list[str], shutdown_event: asyncio.Event) -> None:
    while not shutdown_event.is_set():
        # BLMOVE pops from one key and pushes to queue:processing atomically,
        # closing the gap BLPOP+LPUSH had. It only takes a single source key,
        # so priority order is done by trying queue_keys in order each round;
        # an empty higher-priority queue blocks for up to the poll timeout
        # before the next one is checked.
        job_json = None
        source_queue_key = None
        for queue_key in queue_keys:
            consume_started_at = time.perf_counter()
            job_json = await redis_client.redis.blmove(
                queue_key,
                PROCESSING_QUEUE_KEY,
                timeout=PER_QUEUE_POLL_TIMEOUT_SECONDS,
                src="LEFT",
                dest="LEFT",
            )
            if job_json is not None:
                # Elapsed time of the BLMOVE call that actually returned a job.
                # If the queue was empty and this call blocked waiting for a
                # producer, that wait is included - this is "how long did
                # picking a job up take", not a pure Redis round-trip.
                consume_latency = time.perf_counter() - consume_started_at
                await metrics.record_sample(
                    redis_client.redis, metrics.CONSUME_LATENCY_SAMPLES_KEY, consume_latency
                )
                source_queue_key = queue_key
                break

        if job_json is None:
            await asyncio.sleep(0.1)
            continue

        job = Job.model_validate_json(job_json)

        queue_wait_time = time.time() - job.created_at
        await metrics.record_sample(
            redis_client.redis, metrics.QUEUE_WAIT_SAMPLES_KEY, queue_wait_time
        )

        job.status = JobStatus.RUNNING
        await redis_client.redis.set(f"job:{job.id}", job.model_dump_json(), ex=JOB_TTL_SECONDS)
        await heartbeat(job.id)

        # Extends processing_ttl:{id} every 10s so long-running jobs aren't
        # mistaken for a dead worker by monitor.py while still executing.
        heartbeat_task = asyncio.create_task(_heartbeat_loop(job.id))
        execution_started_at = time.perf_counter()
        try:
            result = await execute_task(job)
        except Exception:
            job.error = traceback.format_exc()
            job.retries += 1
            job.status = JobStatus.PENDING if job.retries < job.max_retries else JobStatus.FAILED
            await metrics.increment_counter(redis_client.redis, metrics.RETRIES_TOTAL_KEY)
        else:
            job.status = JobStatus.COMPLETED
            job.result = result
        finally:
            execution_time = time.perf_counter() - execution_started_at
            await metrics.record_sample(
                redis_client.redis, metrics.EXECUTION_TIME_SAMPLES_KEY, execution_time
            )
            heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat_task

        updated_job_json = job.model_dump_json()
        await redis_client.redis.set(f"job:{job.id}", updated_job_json, ex=JOB_TTL_SECONDS)

        if job.status == JobStatus.PENDING:
            # Requeue onto the same queue it came from, so a retried
            # high-priority job doesn't get demoted to a lower-priority one.
            await redis_client.redis.rpush(source_queue_key, updated_job_json)
        elif job.status == JobStatus.FAILED:
            await redis_client.redis.rpush(DEAD_LETTER_QUEUE_KEY, updated_job_json)
            await metrics.increment_counter(redis_client.redis, metrics.JOBS_FAILED_TOTAL_KEY)

        if job.status in (JobStatus.COMPLETED, JobStatus.FAILED):
            job_latency = time.time() - job.created_at
            await metrics.record_sample(
                redis_client.redis, metrics.JOB_LATENCY_SAMPLES_KEY, job_latency
            )
            await metrics.record_completion_event(redis_client.redis)
            if job.status == JobStatus.COMPLETED:
                await metrics.increment_counter(redis_client.redis, metrics.JOBS_COMPLETED_TOTAL_KEY)

        # Job reached a terminal state or was requeued for retry on its own, so
        # it's no longer at risk of being mistaken for a dead worker's abandoned job.
        await redis_client.redis.lrem(PROCESSING_QUEUE_KEY, 0, job_json)
        await redis_client.redis.delete(f"processing_ttl:{job.id}")

        await asyncio.sleep(0.1)


async def main(queue_keys: list[str]) -> None:
    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown_event.set)

    await redis_client.redis.sadd(WORKERS_ONLINE_KEY, WORKER_ID)
    await metrics.ensure_system_start_time(redis_client.redis)
    registration_task = asyncio.create_task(_register_worker_loop())

    try:
        # Stops picking up new jobs once shutdown_event is set, but a job
        # already in flight runs to completion and its final state is saved
        # before this returns.
        await worker_loop(queue_keys, shutdown_event)
    finally:
        registration_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await registration_task
        await redis_client.redis.srem(WORKERS_ONLINE_KEY, WORKER_ID)
        await redis_client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--queues",
        default="default",
        help="Comma-separated queue names in priority order, e.g. high,default,low",
    )
    args = parser.parse_args()
    asyncio.run(main(parse_queue_keys(args.queues)))
