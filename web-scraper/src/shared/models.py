from typing import Any, Literal, Optional, get_args
from pydantic import BaseModel
from datetime import datetime


RetryableError = Literal['timeout', 'server_error', 'network_error', 'proxy_error', 'blocked', 'unexpected']
NonRetryableError = Literal['no_results', 'not_found', 'invalid_input']
JobStatus = Literal['created', 'queued', 'in_progress', 'paused', 'completed', 'failed']
JobItemStatus = Literal['queued', 'in_progress', 'success', 'error', 'retrying']


class JobItemInput(BaseModel):
    data: dict[str, Any]


class JobItemOutput(BaseModel):
    extracted_data: Optional[dict[str, Any]] = None
    screenshot_key: Optional[str] = None


class JobItem(BaseModel):
    item_id: str
    job_id: str
    status: JobItemStatus
    input: JobItemInput
    output: JobItemOutput
    error_type: Optional[RetryableError | NonRetryableError] = None
    retry_count: int = 0
    created_at: datetime
    updated_at: datetime

    @classmethod
    def is_retryable_error(cls, error_type: Optional[RetryableError | NonRetryableError]) -> bool:
        return error_type in get_args(RetryableError)

    @classmethod
    def is_non_retryable_error(cls, error_type: Optional[RetryableError | NonRetryableError]) -> bool:
        return error_type in get_args(NonRetryableError)


class Job(BaseModel):
    job_id: str
    items: list[JobItem]
    status: JobStatus
    created_at: datetime
    updated_at: datetime
