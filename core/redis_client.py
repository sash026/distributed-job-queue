from pydantic_settings import BaseSettings
from redis.asyncio import Redis


class RedisSettings(BaseSettings):
    redis_url: str = "redis://localhost:6379/0"


class RedisClient:
    def __init__(self, redis_url: str | None = None) -> None:
        self.redis_url = redis_url or RedisSettings().redis_url
        self.redis: Redis = Redis.from_url(self.redis_url, decode_responses=True)

    async def ping(self) -> bool:
        return await self.redis.ping()

    async def close(self) -> None:
        await self.redis.aclose()
