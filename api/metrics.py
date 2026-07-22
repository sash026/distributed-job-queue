import asyncio

from fastapi import APIRouter
from pydantic import BaseModel

from core.redis_client import RedisClient

# Fixed rather than discovered via SCAN: an idle queue has 0 items and
# therefore no key in Redis at all, so scanning would silently omit it.
KNOWN_QUEUE_NAMES = ["default", "high", "low"]
WORKERS_ONLINE_KEY = "workers:online"

redis_client = RedisClient()

router = APIRouter(prefix="/api/v1", tags=["metrics"])


class MetricsResponse(BaseModel):
    queues: dict[str, int]
    workers_active: int


@router.get("/metrics", response_model=MetricsResponse)
async def get_metrics() -> MetricsResponse:
    depths = await asyncio.gather(
        *(redis_client.redis.llen(f"queue:{name}") for name in KNOWN_QUEUE_NAMES)
    )
    queues = dict(zip(KNOWN_QUEUE_NAMES, depths))
    workers_active = await redis_client.redis.scard(WORKERS_ONLINE_KEY)

    return MetricsResponse(queues=queues, workers_active=workers_active)
