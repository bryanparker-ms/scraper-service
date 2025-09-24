from pydantic import BaseModel
from typing import Any, Optional

from src.shared.models import ExecutionPolicy, JobItem, JobItemSummary, JobStatus

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
    created_at: str
    updated_at: str
    total_items: int


class GetJobsResponse(BaseModel):
    jobs: list[JobSummary]


class CreateJobItemRequest(BaseModel):
    item_id: str
    input: dict[str, Any]


class CreateJobRequest(BaseModel):
    job_id: Optional[str] = None
    job_name: Optional[str] = None
    scraper_id: Optional[str] = None
    items: list[CreateJobItemRequest]
    execution_policy: Optional[ExecutionPolicy] = None


class CreateJobResponse(BaseModel):
    job_id: str
    seeded_count: int


class JobStatusResponse(BaseModel):
    status: JobStatus
    summary: JobItemSummary
