import asyncio

from fastapi import APIRouter
from pydantic import BaseModel

from core import metrics
from core.redis_client import RedisClient

# Fixed rather than discovered via SCAN: an idle queue has 0 items and
# therefore no key in Redis at all, so scanning would silently omit it.
KNOWN_QUEUE_NAMES = ["default", "high", "low"]
WORKERS_ONLINE_KEY = "workers:online"
PROCESSING_QUEUE_KEY = "queue:processing"
DEAD_LETTER_QUEUE_KEY = "queue:dead_letter"

redis_client = RedisClient()

router = APIRouter(prefix="/api/v1", tags=["metrics"])


class MetricsResponse(BaseModel):
    queue_depth: dict[str, int]
    active_workers: int
    worker_utilization_percent: float
    jobs_processed_per_sec: float
    jobs_completed: int
    jobs_failed: int
    retry_count: int
    dead_letter_queue_size: int
    avg_queue_wait_time_seconds: float
    avg_execution_time_seconds: float
    p95_job_latency_seconds: float
    throughput_jobs_per_min: float
    avg_queue_publish_latency_seconds: float
    avg_queue_consume_latency_seconds: float
    system_uptime_seconds: float


@router.get("/metrics", response_model=MetricsResponse)
async def get_metrics() -> MetricsResponse:
    depths = await asyncio.gather(
        *(redis_client.redis.llen(f"queue:{name}") for name in KNOWN_QUEUE_NAMES)
    )
    queue_depth = dict(zip(KNOWN_QUEUE_NAMES, depths))

    active_workers = await redis_client.redis.scard(WORKERS_ONLINE_KEY)
    processing_count = await redis_client.redis.llen(PROCESSING_QUEUE_KEY)
    # Approximation: one worker processes at most one job at a time, so the
    # number of in-flight jobs is a proxy for busy workers.
    worker_utilization_percent = (
        min(processing_count / active_workers, 1.0) * 100 if active_workers else 0.0
    )

    dead_letter_queue_size = await redis_client.redis.llen(DEAD_LETTER_QUEUE_KEY)

    jobs_completed = await metrics.get_counter(redis_client.redis, metrics.JOBS_COMPLETED_TOTAL_KEY)
    jobs_failed = await metrics.get_counter(redis_client.redis, metrics.JOBS_FAILED_TOTAL_KEY)
    retry_count = await metrics.get_counter(redis_client.redis, metrics.RETRIES_TOTAL_KEY)

    jobs_per_sec, jobs_per_min = await metrics.get_throughput(redis_client.redis)

    queue_wait_samples = await metrics.get_samples(redis_client.redis, metrics.QUEUE_WAIT_SAMPLES_KEY)
    execution_time_samples = await metrics.get_samples(
        redis_client.redis, metrics.EXECUTION_TIME_SAMPLES_KEY
    )
    job_latency_samples = await metrics.get_samples(redis_client.redis, metrics.JOB_LATENCY_SAMPLES_KEY)
    publish_latency_samples = await metrics.get_samples(
        redis_client.redis, metrics.PUBLISH_LATENCY_SAMPLES_KEY
    )
    consume_latency_samples = await metrics.get_samples(
        redis_client.redis, metrics.CONSUME_LATENCY_SAMPLES_KEY
    )

    uptime_seconds = await metrics.get_uptime_seconds(redis_client.redis)

    return MetricsResponse(
        queue_depth=queue_depth,
        active_workers=active_workers,
        worker_utilization_percent=round(worker_utilization_percent, 2),
        jobs_processed_per_sec=round(jobs_per_sec, 4),
        jobs_completed=jobs_completed,
        jobs_failed=jobs_failed,
        retry_count=retry_count,
        dead_letter_queue_size=dead_letter_queue_size,
        avg_queue_wait_time_seconds=round(metrics.mean(queue_wait_samples), 4),
        avg_execution_time_seconds=round(metrics.mean(execution_time_samples), 4),
        p95_job_latency_seconds=round(metrics.percentile(job_latency_samples, 0.95), 4),
        throughput_jobs_per_min=round(jobs_per_min, 2),
        avg_queue_publish_latency_seconds=round(metrics.mean(publish_latency_samples), 6),
        avg_queue_consume_latency_seconds=round(metrics.mean(consume_latency_samples), 6),
        system_uptime_seconds=round(uptime_seconds, 1),
    )
