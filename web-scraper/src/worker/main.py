import asyncio
import logging
import signal
import time
from types import FrameType
from typing import Optional

from src.shared.db import DynamoDBDatabaseService
from src.shared.interfaces import ResultStorage
from src.shared.models import ItemMetadata, JobItem, JobItemOutput, StorageKeys
from src.shared.queue import SqsQueue
from src.shared.settings import Settings
from src.shared.storage import LocalFilesystemStorage, S3ResultStorage
from src.shared.utils import now_iso
from src.worker.registry import registry
from src.worker.scraper import BaseScraper, ScraperError

# import scrapers package to register all scrapers
import src.scrapers  # type:ignore

logger = logging.getLogger(__name__)


class Worker:
    def __init__(self):
        self.settings = Settings()
        self.queue = SqsQueue(self.settings)
        self.storage = _get_storage(self.settings)
        self.db = DynamoDBDatabaseService(self.settings)
        self.running = True

    async def run(self):
        """Worker loop that processes job items from the queue"""
        logger.info('Worker started and listening for messages...')

        # graceful shutdown signals
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

        while self.running:
            try:
                queue_response = self.queue.dequeue()

                if queue_response is None:
                    await asyncio.sleep(0.1)
                    continue

                job_item, receipt_handle, message_id = queue_response

                await self._process_job_item(job_item, receipt_handle, message_id)
            except Exception as e:
                logger.exception(f'Unexpected error in worker loop: {e}')
                # continue running even if individual message processing fails
                await asyncio.sleep(1)

        logger.info('Worker shutting down...')

    async def _process_job_item(
        self,
        job_item: JobItem,
        receipt_handle: str,
        message_id: str
    ) -> None:
        logger.info(f'Processing job item - Job: {job_item.job_id}, Item: {job_item.item_id}, Status: {job_item.status}')
        logger.debug(f'Job item inputs: {job_item.input}')

        # Update status: queued -> in_progress
        self.db.update_item_status(job_item.job_id, job_item.item_id, 'in_progress')

        started_at = now_iso()
        start_time = time.time()

        try:
            # try to get a scraper for the job item. if a scraper is not found, a ValueError will be raised
            scraper = self._get_scraper_for_item(job_item)
            result = await scraper.scrape(job_item)

            # Calculate duration
            duration_ms = int((time.time() - start_time) * 1000)
            completed_at = now_iso()

            logger.info(
                f'Scraper completed - Item: {job_item.item_id}, '
                f'HTML size: {len(result.html)} chars, '
                f'Data keys: {list(result.data.keys())}, '
                f'Duration: {duration_ms}ms'
            )

            # metadata about the job item
            metadata = ItemMetadata(
                job_id=job_item.job_id,
                item_id=job_item.item_id,
                status='success',
                scraper_name=scraper.__class__.__name__,
                attempt_number=job_item.retry_count + 1,
                started_at=started_at,
                completed_at=completed_at,
                duration_ms=duration_ms,
                storage_keys=StorageKeys(),  # Will be populated by storage
            )

            # pointers to the stored artifacts. stored in the manifest and back in the DB
            storage_keys = await self.storage.store_result(
                job_item,
                result,
                metadata
            )

            logger.info(
                f'Stored results for {job_item.item_id}: '
                f'HTML={bool(storage_keys.html)}, '
                f'Data={bool(storage_keys.data)}, '
                f'Screenshot={bool(storage_keys.screenshot)}'
            )

            await self.storage.update_manifest(
                job_item.job_id,
                job_item.item_id,
                storage_keys,
                'success'
            )

            # update db with success status and storage keys
            output = JobItemOutput(
                screenshot_key=storage_keys.screenshot,
                storage_keys=storage_keys.model_dump(exclude_none=True)
            )

            self.db.update_job_item_success(
                job_id=job_item.job_id,
                item_id=job_item.item_id,
                output=output,
                completed_at=completed_at
            )

            logger.info(f'Updated DB status to success for {job_item.item_id}')

            # delete the message to prevent reprocessing
            self._delete_message(receipt_handle)

            logger.debug(f'Deleted message {message_id}')
        except ScraperError as e:
            # scraping failed. let's see if we should retry
            completed_at = now_iso()
            duration_ms = int((time.time() - start_time) * 1000)

            logger.error(f'Scraper error for item {job_item.item_id}: {e.error_type} - {e}')

            # Get max_retries from job's execution policy
            job = self.db.get_job(job_item.job_id)
            max_retries = job.execution_policy.retries.max_retries if job.execution_policy and job.execution_policy.retries else 3

            # update db with error details
            self.db.update_job_item_error(
                job_id=job_item.job_id,
                item_id=job_item.item_id,
                error_type=e.error_type,
                error_message=str(e),
                retry_count=job_item.retry_count + 1,
                last_attempt_at=completed_at,
                max_retries=max_retries
            )

            logger.info(f'Updated DB with error status for {job_item.item_id}: {e.error_type}')

            # Delete the message from SQS since we've exhausted retries
            # Note: The scraper already handles retries internally, so by the time
            # we get here, all retry attempts have failed. We should not put the
            # message back in the queue.
            self._delete_message(receipt_handle)
            logger.info(f'Deleted message {message_id} after exhausting retries (error: {e.error_type})')

        except Exception as e:
            # oops... this isn't expected. report it and try again (maybe we shouldn't try unexpected errors again)
            completed_at = now_iso()

            logger.exception(f'Unexpected error processing item {job_item.item_id}: {e}')

            # Get max_retries from job's execution policy
            job = self.db.get_job(job_item.job_id)
            max_retries = job.execution_policy.retries.max_retries if job.execution_policy and job.execution_policy.retries else 3

            # log the error back to the DB
            self.db.update_job_item_error(
                job_id=job_item.job_id,
                item_id=job_item.item_id,
                error_type='unexpected',
                error_message=str(e),
                retry_count=job_item.retry_count + 1,
                last_attempt_at=completed_at,
                max_retries=max_retries
            )

    def _get_scraper_for_item(self, job_item: JobItem) -> BaseScraper:
        """
        Select the appropriate scraper for the given job item. Looks up scraper_id from the job in in the registry and returns an instance. If the scraper is not found, a ValueError will be raised.
        """
        # fetch the job to get the scraper_id and execution_policy
        job = self.db.get_job(job_item.job_id)

        if not job.scraper_id:
            raise ValueError('No scraper_id specified for job')

        scraper_class = registry.get(job.scraper_id)
        if scraper_class:
            logger.debug(f'Using scraper "{job.scraper_id}" for item {job_item.item_id}')
            return scraper_class(execution_policy=job.execution_policy)
        else:
            logger.error(f'Scraper "{job.scraper_id}" not found in registry')
            raise ValueError(f'Unknown scraper_id: {job.scraper_id}')

    def _delete_message(self, receipt_handle: str) -> None:
        """Delete the processed message from the queue"""
        try:
            self.queue.sqs_client.delete_message(
                QueueUrl=self.settings.queue_url,
                ReceiptHandle=receipt_handle
            )
        except Exception as e:
            logger.error(f'Failed to delete message: {e}')
            # don't raise - we don't want to crash the worker over cleanup failures

    def _signal_handler(self, signum: int, frame: Optional[FrameType]) -> None:
        """Handle graceful shutdown"""
        logger.info(f'Received signal {signum}, initiating graceful shutdown...')
        self.running = False


def _get_storage(settings: Settings) -> ResultStorage:
    if settings.use_local_storage:
        logger.info(f'Using local filesystem storage at {settings.local_storage_path}')
        return LocalFilesystemStorage(settings.local_storage_path)
    else:
        logger.info(f'Using S3 storage with bucket {settings.bucket_name}')
        return S3ResultStorage(settings)


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    worker = Worker()
    await worker.run()


if __name__ == '__main__':
    asyncio.run(main())
