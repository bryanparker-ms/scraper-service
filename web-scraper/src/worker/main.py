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

# Import scrapers package to register all scrapers
import src.scrapers  # noqa: F401

logger = logging.getLogger(__name__)


class Worker:
    def __init__(self):
        self.settings = Settings()
        self.queue = SqsQueue(self.settings)
        self.storage = _get_storage(self.settings)
        self.db = DynamoDBDatabaseService(self.settings)
        self.running = True

    async def run(self):
        """Main worker loop that processes messages from SQS."""
        logger.info('Worker started and listening for messages...')

        # Set up signal handling for graceful shutdown
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
        """Process a single job item, handling success and error cases."""
        logger.info(f'Processing job item - Job: {job_item.job_id}, Item: {job_item.item_id}, Status: {job_item.status}')
        logger.debug(f'Job item inputs: {job_item.input}')

        started_at = now_iso()
        start_time = time.time()

        try:
            # Get scraper and process the item
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

            # Build metadata for storage
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

            # Store result artifacts in S3
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

            # Update manifest with completed item
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
            # Scraping failed with a classified error
            completed_at = now_iso()
            duration_ms = int((time.time() - start_time) * 1000)

            logger.error(f'Scraper error for item {job_item.item_id}: {e.error_type} - {e}')

            # update db with error details
            self.db.update_job_item_error(
                job_id=job_item.job_id,
                item_id=job_item.item_id,
                error_type=e.error_type,
                error_message=str(e),
                retry_count=job_item.retry_count + 1,
                last_attempt_at=completed_at
            )

            logger.info(f'Updated DB with error status for {job_item.item_id}: {e.error_type}')

            # - for non-retryable errors, delete the message
            # - for retryable errors, let it return to queue (visibility timeout expires)
            if not JobItem.is_retryable_error(e.error_type):
                self._delete_message(receipt_handle)
                logger.info(f'Deleted message {message_id} (non-retryable error)')
            else:
                logger.info(f'Message {message_id} will return to queue for retry (retryable error: {e.error_type})')

        except Exception as e:
            # Unexpected error
            completed_at = now_iso()

            logger.exception(f'Unexpected error processing item {job_item.item_id}: {e}')

            # Update DynamoDB with unexpected error
            self.db.update_job_item_error(
                job_id=job_item.job_id,
                item_id=job_item.item_id,
                error_type='unexpected',
                error_message=str(e),
                retry_count=job_item.retry_count + 1,
                last_attempt_at=completed_at
            )

            logger.info(f'Updated DynamoDB with unexpected error for {job_item.item_id}')

            # Let message return to queue for retry
            logger.info(f'Message {message_id} will return to queue for retry')

    def _get_scraper_for_item(self, job_item: JobItem) -> BaseScraper:
        """
        Select the appropriate scraper for the given job item.
        Looks up scraper_id from the job in the registry.
        """
        # Get job to check for scraper_id
        job = self.db.get_job(job_item.job_id)

        if not job.scraper_id:
            raise ValueError('No scraper_id specified for job')

        scraper_class = registry.get(job.scraper_id)
        if scraper_class:
            logger.debug(f'Using scraper "{job.scraper_id}" for item {job_item.item_id}')
            return scraper_class()
        else:
            logger.error(f'Scraper "{job.scraper_id}" not found in registry')
            raise ValueError(f'Unknown scraper_id: {job.scraper_id}')

    def _delete_message(self, receipt_handle: str) -> None:
        """Delete processed message from SQS."""
        try:
            self.queue.sqs_client.delete_message(
                QueueUrl=self.settings.queue_url,
                ReceiptHandle=receipt_handle
            )
        except Exception as e:
            logger.error(f'Failed to delete message: {e}')
            # Don't raise - we don't want to crash the worker over cleanup failures

    def _signal_handler(self, signum: int, frame: Optional[FrameType]) -> None:
        """Handle shutdown signals gracefully."""
        logger.info(f'Received signal {signum}, initiating graceful shutdown...')
        self.running = False


def _get_storage(settings: Settings) -> ResultStorage:
    if settings.use_local_storage:
        logger.info(f"Using local filesystem storage at {settings.local_storage_path}")
        return LocalFilesystemStorage(settings.local_storage_path)
    else:
        logger.info(f"Using S3 storage with bucket {settings.bucket_name}")
        return S3ResultStorage(settings)


async def main():
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    worker = Worker()
    await worker.run()


if __name__ == '__main__':
    asyncio.run(main())
