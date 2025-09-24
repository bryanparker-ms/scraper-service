from typing import Optional, Protocol, Sequence, Tuple

from src.shared.models import Job, JobItem, JobItemSummary

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


QueueResponse = Tuple[JobItem, str, str]

class QueueService(Protocol):
    def enqueue(self, job_items: Sequence[JobItem]) -> None:
        ...

    def dequeue(self) -> Optional[QueueResponse]:
        ...
