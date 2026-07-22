import asyncio

from core.redis_client import RedisClient
from core.schemas import Job

QUEUE_KEY = "queue:default"
PROCESSING_QUEUE_KEY = "queue:processing"
MONITOR_INTERVAL_SECONDS = 10

redis_client = RedisClient()


async def requeue_expired_jobs() -> None:
    job_jsons = await redis_client.redis.lrange(PROCESSING_QUEUE_KEY, 0, -1)

    for job_json in job_jsons:
        job = Job.model_validate_json(job_json)
        ttl_key = f"processing_ttl:{job.id}"

        if not await redis_client.redis.exists(ttl_key):
            # No live heartbeat for this job - its worker is presumed dead.
            # Requeue it, then remove the stale copy so it isn't re-pushed
            # again on the next sweep.
            await redis_client.redis.rpush(QUEUE_KEY, job_json)
            await redis_client.redis.lrem(PROCESSING_QUEUE_KEY, 0, job_json)


async def monitor_loop() -> None:
    while True:
        await requeue_expired_jobs()
        await asyncio.sleep(MONITOR_INTERVAL_SECONDS)


if __name__ == "__main__":
    asyncio.run(monitor_loop())
