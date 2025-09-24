from typing import Protocol
from src.shared.models import JobItem
from src.worker.models import ScrapeResult


class BaseScraper(Protocol):
    def run(self, job_item: JobItem) -> ScrapeResult:
        ...
