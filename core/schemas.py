from __future__ import annotations

from enum import Enum

from pydantic import BaseModel


class JobStatus(str, Enum):
    PENDING = "Pending"
    RUNNING = "Running"
    COMPLETED = "Completed"
    FAILED = "Failed"


class Job(BaseModel):
    id: str
    name: str
    status: JobStatus
    payload: dict
    result: dict | None = None
    error: str | None = None
    retries: int = 0
    max_retries: int = 3
    created_at: float
