from fastapi import APIRouter, HTTPException

from core.redis_client import RedisClient
from core.schemas import Job, JobStatus

QUEUE_KEY = "queue:default"
DEAD_LETTER_QUEUE_KEY = "queue:dead_letter"
JOB_TTL_SECONDS = 60 * 60

redis_client = RedisClient()

router = APIRouter(prefix="/api/v1/admin", tags=["admin"])


@router.get("/dead-letter", response_model=list[Job])
async def list_dead_letter_jobs() -> list[Job]:
    job_jsons = await redis_client.redis.lrange(DEAD_LETTER_QUEUE_KEY, 0, 9)
    return [Job.model_validate_json(job_json) for job_json in job_jsons]


@router.delete("/dead-letter/{job_id}")
async def retry_dead_letter_job(job_id: str) -> dict:
    dead_jobs = await redis_client.redis.lrange(DEAD_LETTER_QUEUE_KEY, 0, -1)

    for job_json in dead_jobs:
        job = Job.model_validate_json(job_json)
        if job.id != job_id:
            continue

        removed = await redis_client.redis.lrem(DEAD_LETTER_QUEUE_KEY, 1, job_json)
        if not removed:
            continue

        job.status = JobStatus.PENDING
        updated_job_json = job.model_dump_json()
        await redis_client.redis.rpush(QUEUE_KEY, updated_job_json)
        await redis_client.redis.set(f"job:{job.id}", updated_job_json, ex=JOB_TTL_SECONDS)
        return {"job_id": job.id, "status": job.status}

    raise HTTPException(status_code=404, detail=f"Job {job_id} not found in dead-letter queue")
