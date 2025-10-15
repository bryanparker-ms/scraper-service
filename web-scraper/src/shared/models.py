from typing import Any, Literal, Optional, get_args
from pydantic import BaseModel, Field
from src.shared.utils import now_iso


RetryableError = Literal['timeout', 'server_error', 'network_error', 'proxy_error', 'blocked', 'unexpected']
NonRetryableError = Literal['no_results', 'not_found', 'invalid_input']

JobStatus = Literal['created', 'queued', 'in_progress', 'paused', 'completed', 'failed']
JobItemStatus = Literal['pending', 'queued', 'in_progress', 'success', 'failed']

"""
Job models
"""

class JobItemOutput(BaseModel):
    screenshot_key: Optional[str] = None
    storage_keys: Optional[dict[str, str]] = None


class JobItem(BaseModel):
    job_id: str
    item_id: str
    status: JobItemStatus = 'pending'  # Default: pending (in DynamoDB, waiting for scheduler)
    input: dict[str, Any]
    output: Optional[JobItemOutput] = None
    error_type: Optional[RetryableError | NonRetryableError] = None
    retry_count: int = 0
    first_attempt_at: Optional[str] = None
    last_attempt_at: Optional[str] = None
    created_at: str = Field(default_factory=now_iso)
    updated_at: str = Field(default_factory=now_iso)

    @classmethod
    def is_retryable_error(cls, error_type: Optional[RetryableError | NonRetryableError]) -> bool:
        return error_type in get_args(RetryableError)

    @classmethod
    def is_non_retryable_error(cls, error_type: Optional[RetryableError | NonRetryableError]) -> bool:
        return error_type in get_args(NonRetryableError)


class ThrottlingPolicy(BaseModel):
    """
    Concurrency control policy to prevent overwhelming target websites.

    Controls the maximum number of workers that can process items from
    the same job simultaneously. This prevents too many concurrent connections
    to the same website, reducing the risk of being blocked or banned.
    """
    max_concurrent_workers: int = 3  # Maximum workers processing this job at once


class ProxyGeolocationPolicy(BaseModel):
    state: Optional[str] = None
    city: Optional[str] = None


class ProxyPolicy(BaseModel):
    type: Literal['datacenter', 'residential', 'web-unlocker'] = 'datacenter'
    geo_target: Optional[ProxyGeolocationPolicy] = None


class RetryPolicy(BaseModel):
    max_retries: int = 3
    backoff_strategy: Literal['linear', 'exponential'] = 'linear'
    backoff_factor: float = 1.0


class TimeoutPolicy(BaseModel):
    connect_timeout_seconds: int = 10
    request_timeout_seconds: int = 30


class CircuitBreakerPolicy(BaseModel):
    """
    Circuit breaker policy to stop jobs that are failing.

    Once tripped, the job stays paused permanently and must be manually
    resumed or restarted.
    """
    min_requests: int = 50
    failure_threshold_percentage: float = 0.25


class ExecutionPolicy(BaseModel):
    throttling: ThrottlingPolicy = ThrottlingPolicy()
    proxy: ProxyPolicy = ProxyPolicy()
    retries: RetryPolicy = RetryPolicy()
    timeouts: TimeoutPolicy = TimeoutPolicy()
    circuit_breaker: CircuitBreakerPolicy = CircuitBreakerPolicy()


class Job(BaseModel):
    job_id: str
    job_name: Optional[str] = None
    scraper_id: Optional[str] = None  # Explicitly specify which scraper to use
    status: JobStatus = 'created'
    execution_policy: ExecutionPolicy = ExecutionPolicy()
    total_items: int = 0
    created_at: str = Field(default_factory=now_iso)
    updated_at: str = Field(default_factory=now_iso)


class JobItemSummary(BaseModel):
    pending: int = 0
    queued: int = 0
    in_progress: int = 0
    success: int = 0
    failed: int = 0
    max_retries: int = 0
    skipped: int = 0


"""
Storage models
"""

class StorageKeys(BaseModel):
    """References to stored artifacts in storage backend."""
    html: Optional[str] = None
    data: Optional[str] = None
    metadata: Optional[str] = None
    screenshot: Optional[str] = None
    document: Optional[str] = None


class ItemMetadata(BaseModel):
    """Metadata about a scrape operation."""
    job_id: str
    item_id: str
    status: Literal['success', 'error']
    scraper_name: str
    scraper_version: str = "1.0.0"
    attempt_number: int
    started_at: str
    completed_at: str
    duration_ms: int
    storage_keys: StorageKeys
    http_metadata: Optional[dict[str, Any]] = None
    error: Optional[dict[str, Any]] = None


class ManifestItem(BaseModel):
    """Entry in a job manifest."""
    item_id: str
    status: Literal['success', 'error']
    storage_keys: StorageKeys
    size_bytes: int = 0
    completed_at: str = Field(default_factory=now_iso)
