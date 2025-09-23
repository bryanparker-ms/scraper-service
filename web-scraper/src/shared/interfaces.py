from typing import Protocol

from shared.models import Job

class DatabaseService(Protocol):
    def create_job_if_not_exists(self):
        ...

    def get_jobs(self) -> list[Job]:
        ...
