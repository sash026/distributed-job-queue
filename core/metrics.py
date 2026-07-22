import time
import uuid

JOBS_COMPLETED_TOTAL_KEY = "metrics:jobs_completed_total"
JOBS_FAILED_TOTAL_KEY = "metrics:jobs_failed_total"
RETRIES_TOTAL_KEY = "metrics:retries_total"
COMPLETIONS_TIMESERIES_KEY = "metrics:completions_timeseries"
QUEUE_WAIT_SAMPLES_KEY = "metrics:queue_wait_samples"
EXECUTION_TIME_SAMPLES_KEY = "metrics:execution_time_samples"
JOB_LATENCY_SAMPLES_KEY = "metrics:job_latency_samples"
PUBLISH_LATENCY_SAMPLES_KEY = "metrics:publish_latency_samples"
CONSUME_LATENCY_SAMPLES_KEY = "metrics:consume_latency_samples"
SYSTEM_START_TIME_KEY = "metrics:system_start_time"

MAX_SAMPLES = 1000
THROUGHPUT_WINDOW_SECONDS = 60


async def increment_counter(redis, key: str) -> None:
    await redis.incr(key)


async def get_counter(redis, key: str) -> int:
    value = await redis.get(key)
    return int(value) if value is not None else 0


async def record_sample(redis, key: str, value: float, cap: int = MAX_SAMPLES) -> None:
    await redis.lpush(key, value)
    await redis.ltrim(key, 0, cap - 1)


async def get_samples(redis, key: str) -> list[float]:
    raw_samples = await redis.lrange(key, 0, -1)
    return [float(sample) for sample in raw_samples]


async def record_completion_event(redis, timestamp: float | None = None) -> None:
    timestamp = timestamp if timestamp is not None else time.time()
    await redis.zadd(COMPLETIONS_TIMESERIES_KEY, {str(uuid.uuid4()): timestamp})


async def get_throughput(
    redis, window_seconds: int = THROUGHPUT_WINDOW_SECONDS
) -> tuple[float, float]:
    now = time.time()
    await redis.zremrangebyscore(COMPLETIONS_TIMESERIES_KEY, 0, now - window_seconds)
    count = await redis.zcard(COMPLETIONS_TIMESERIES_KEY)
    jobs_per_sec = count / window_seconds
    jobs_per_min = jobs_per_sec * 60
    return jobs_per_sec, jobs_per_min


async def ensure_system_start_time(redis) -> None:
    await redis.set(SYSTEM_START_TIME_KEY, time.time(), nx=True)


async def get_uptime_seconds(redis) -> float:
    start = await redis.get(SYSTEM_START_TIME_KEY)
    return time.time() - float(start) if start is not None else 0.0


def mean(samples: list[float]) -> float:
    return sum(samples) / len(samples) if samples else 0.0


def percentile(samples: list[float], p: float) -> float:
    if not samples:
        return 0.0
    ordered = sorted(samples)
    index = min(int(len(ordered) * p), len(ordered) - 1)
    return ordered[index]
