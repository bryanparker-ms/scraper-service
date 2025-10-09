from typing import Any, Generator, Literal, Optional, Protocol, Sequence, Tuple

from src.shared.models import ItemMetadata, Job, JobItem, JobItemOutput, JobItemSummary, StorageKeys, RetryableError, NonRetryableError
from src.worker.models import ScrapeResult

class DatabaseService(Protocol):
    def create_job_if_not_exists(self, job: Job):
        ...

    def get_job(self, job_id: str) -> Job:
        ...

    def get_job_status(self, job_id: str) -> JobItemSummary:
        ...

    def get_jobs(self) -> list[Job]:
        ...

    def job_exists(self, job_id: str) -> bool:
        ...

    def create_job_item(self, job_id: str, job_item: JobItem):
        ...

    def update_job_item_success(
        self,
        job_id: str,
        item_id: str,
        output: JobItemOutput,
        completed_at: str
    ) -> None:
        """Update job item with success status and output."""
        ...

    def update_job_item_error(
        self,
        job_id: str,
        item_id: str,
        error_type: RetryableError | NonRetryableError,
        error_message: str,
        retry_count: int,
        last_attempt_at: str
    ) -> None:
        """Update job item with error status and details."""
        ...

    def get_job_item(self, job_id: str, item_id: str) -> JobItem:
        """Retrieve a specific job item."""
        ...

    def get_job_failure_metrics(self, job_id: str) -> dict[str, int]:
        """
        Get failure metrics for circuit breaker.

        Returns:
            Dict with keys:
            - success_count: Number of successful items
            - error_count: Number of failed items
            - consecutive_errors: Number of consecutive errors from most recent items
        """
        ...

    def update_job_status(self, job_id: str, status: Literal['created', 'queued', 'in_progress', 'paused', 'completed', 'failed']) -> None:
        """
        Update job status.

        Args:
            job_id: Job ID
            status: New status
        """
        ...

    def get_all_job_items(self, job_id: str, batch_size: int = 100) -> Generator[list[JobItem], Any, None]:
        """
        Get all job items for a job as an iterator (for manifest generation).

        Yields items in batches to avoid loading everything into memory.

        Args:
            job_id: Job ID
            batch_size: Number of items to yield at once

        Yields:
            Batches of job items
        """
        ...


QueueResponse = Tuple[JobItem, str, str]

class QueueService(Protocol):
    def enqueue(self, job_items: Sequence[JobItem]) -> None:
        ...

    def dequeue(self) -> Optional[QueueResponse]:
        ...

    def get_length(self) -> int:
        ...

    def purge(self) -> None:
        ...


class ResultStorage(Protocol):
    """Generic interface for storing scrape results."""

    async def store_result(
        self,
        job_item: JobItem,
        result: ScrapeResult,
        metadata: ItemMetadata
    ) -> StorageKeys:
        """
        Store all artifacts for a scrape result.

        Args:
            job_item: The job item being processed
            result: The scrape result containing HTML, data, screenshots
            metadata: Metadata about the scraping operation

        Returns:
            StorageKeys with references to stored artifacts

        Raises:
            StorageError: If all artifact uploads fail
        """
        ...

    async def get_html(self, job_id: str, item_id: str) -> str:
        """Retrieve HTML for an item."""
        ...

    async def get_data(self, job_id: str, item_id: str) -> dict[str, Any]:
        """Retrieve extracted data for an item."""
        ...

    async def get_metadata(self, job_id: str, item_id: str) -> ItemMetadata:
        """Retrieve metadata for an item."""
        ...

    async def update_manifest(
        self,
        job_id: str,
        item_id: str,
        storage_keys: StorageKeys,
        status: Literal['success', 'error']
    ) -> None:
        """Update job manifest with completed item."""
        ...
