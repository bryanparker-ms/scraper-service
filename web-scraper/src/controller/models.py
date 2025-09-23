from datetime import datetime
from pydantic import BaseModel, Field
from typing import Literal, Optional

from shared.models import Job, JobStatus

class ErrorResponse(BaseModel):
    status: int
    message: str



# class CreateJobRequest(BaseModel):
#     job_id: Optional[str] = None
#     items: list[JobItem]
#     # defaults: dict[str, Any] = Field(default_factory=dict)
#     defaults: JobDefaults = Field(default_factory=JobDefaults)


# class CreateJobResponse(BaseModel):
#     job_id: str
#     seeded_count: int


# class JobStatus(BaseModel):
#     job_id: str
#     totals: ItemSummaryStats


# class PauseResumeResponse(BaseModel):
#     job_id: str
#     status: Literal['paused', 'running']


# class FinalizeResponse(BaseModel):
#     job_id: str
#     manifest_key: str
#     total_items: int

class JobSummary(BaseModel):
    job_id: str
    status: JobStatus
    created_at: datetime
    updated_at: datetime
    total_items: int

class GetJobsResponse(BaseModel):
    jobs: list[JobSummary]
