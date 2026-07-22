import asyncio
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from api.admin import router as admin_router
from api.metrics import router as metrics_router
from core import metrics
from core.redis_client import RedisClient
from core.schemas import Job, JobStatus

JOB_TTL_SECONDS = 60 * 60
STREAM_POLL_INTERVAL_SECONDS = 0.5

redis_client = RedisClient()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await metrics.ensure_system_start_time(redis_client.redis)
    yield
    await redis_client.close()


app = FastAPI(lifespan=lifespan)
app.include_router(admin_router)
app.include_router(metrics_router)


class JobCreateRequest(BaseModel):
    name: str
    payload: dict
    queue_name: str = "default"


class JobCreateResponse(BaseModel):
    job_id: str
    status: JobStatus


@app.post("/api/v1/jobs", response_model=JobCreateResponse)
async def create_job(request: JobCreateRequest) -> JobCreateResponse:
    job = Job(
        id=str(uuid.uuid4()),
        name=request.name,
        status=JobStatus.PENDING,
        payload=request.payload,
        created_at=time.time(),
    )
    job_json = job.model_dump_json()
    queue_key = f"queue:{request.queue_name}"

    publish_started_at = time.perf_counter()
    await redis_client.redis.rpush(queue_key, job_json)
    publish_latency = time.perf_counter() - publish_started_at
    await metrics.record_sample(redis_client.redis, metrics.PUBLISH_LATENCY_SAMPLES_KEY, publish_latency)

    await redis_client.redis.set(f"job:{job.id}", job_json, ex=JOB_TTL_SECONDS)

    return JobCreateResponse(job_id=job.id, status=job.status)


async def _stream_job_status(job_id: str) -> AsyncIterator[str]:
    last_status: JobStatus | None = None

    while True:
        job_json = await redis_client.redis.get(f"job:{job_id}")
        if job_json is None:
            # Job already gone (TTL expired) - nothing further to stream.
            return

        job = Job.model_validate_json(job_json)
        if job.status != last_status:
            yield f"data: {job.model_dump_json()}\n\n"
            last_status = job.status

        if job.status in (JobStatus.COMPLETED, JobStatus.FAILED):
            return

        await asyncio.sleep(STREAM_POLL_INTERVAL_SECONDS)


@app.get("/api/v1/jobs/{job_id}", response_model=None)
async def get_job(job_id: str, stream: bool = False) -> Job | StreamingResponse:
    job_json = await redis_client.redis.get(f"job:{job_id}")
    if job_json is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    if not stream:
        return Job.model_validate_json(job_json)

    return StreamingResponse(_stream_job_status(job_id), media_type="text/event-stream")
