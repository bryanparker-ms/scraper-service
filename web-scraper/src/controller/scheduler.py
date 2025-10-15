import asyncio
import time
import logging
from typing import Any, Dict
from src.controller.circuit_breaker import CircuitBreaker
from src.shared.db import DynamoDBDatabaseService
from src.shared.queue import SqsQueue
from src.shared.settings import Settings
from src.shared.models import Job, JobItem
from src.shared.storage import S3ResultStorage

logger = logging.getLogger(__name__)


class JobScheduler:
    """
    Background scheduler that reads pending items from DynamoDB
    and pushes them to SQS while enforcing concurrency limits.

    This runs as a background task in the controller service, providing
    centralized concurrency control without requiring a separate deployment.

    Concurrency control limits how many workers can process items from the
    same job simultaneously, preventing overwhelming target websites.
    """

    def __init__(self, settings: Settings, poll_interval_seconds: float = 1.0):
        """
        Initialize the job scheduler.

        Args:
            settings: Application settings
            poll_interval_seconds: How often to check for pending items (default: 1.0 second)
        """
        self.settings = settings
        self.db = DynamoDBDatabaseService(settings)
        self.queue = SqsQueue(settings)
        self.storage = S3ResultStorage(settings)
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        self.running = False
        self.poll_interval = poll_interval_seconds

        logger.info(f'JobScheduler initialized (poll interval: {poll_interval_seconds}s)')

    async def start(self):
        """Start the scheduler background task"""
        self.running = True
        logger.info("Starting job scheduler loop")

        # Run scheduler loop
        while self.running:
            try:
                await self.schedule_cycle()
                await asyncio.sleep(self.poll_interval)
            except Exception as e:
                logger.error(f"Scheduler error: {e}", exc_info=True)
                await asyncio.sleep(self.poll_interval)  # Back off on error

        logger.info("Job scheduler loop stopped")

    async def stop(self):
        """Stop the scheduler gracefully"""
        logger.info("Stopping job scheduler")
        self.running = False

    async def schedule_cycle(self):
        """Single scheduling cycle - process all active jobs"""
        # Get jobs that have queued items
        active_jobs = self.db.get_active_jobs()

        if not active_jobs:
            # no active jobs, nothing to do
            return

        logger.info(f'Found {len(active_jobs)} active job(s): {[j.job_id for j in active_jobs]}')

        # Process each job
        for job in active_jobs:
            try:
                await self.schedule_job(job)
            except Exception as e:
                logger.error(f"Error scheduling job {job.job_id}: {e}", exc_info=True)

        # Periodically cleanup finished jobs (every ~10 seconds)
        if int(time.time()) % 10 == 0:
            self.cleanup_finished_jobs()

    async def schedule_job(self, job: Job):
        """
        Schedule items for a single job.

        Enforces concurrency control by limiting how many workers can process
        items from this job simultaneously. Checks circuit breaker to prevent
        queueing items for failing jobs.

        Args:
            job: Job to schedule items for
        """
        logger.info(f'Scheduling job {job.job_id} (status: {job.status})')

        # get or create circuit breaker for this job
        circuit = self.get_circuit_breaker(job)

        # check circuit breaker status
        if not circuit.should_allow_request():
            logger.info(f'Circuit breaker OPEN for job {job.job_id}, skipping scheduling')
            return

        # update circuit breaker with latest failure metrics
        failure_metrics = self.db.get_job_failure_metrics(job.job_id)
        circuit_ok = circuit.check_and_update(
            success_count=failure_metrics['success_count'],
            error_count=failure_metrics['error_count'],
            consecutive_errors=failure_metrics['consecutive_errors']
        )

        if not circuit_ok:
            # circuit just tripped, stop scheduling and fail job
            logger.warning(f'Stopping scheduling for job {job.job_id} (circuit breaker tripped)')
            self.db.update_job_status(job.job_id, 'failed')
            return

        # Get concurrency limit from execution policy
        max_concurrent = job.execution_policy.throttling.max_concurrent_workers if job.execution_policy and job.execution_policy.throttling else 3

        # Check how many items are currently in-flight (queued or in_progress)
        status_summary = self.db.get_job_status(job.job_id)
        in_flight = status_summary.queued + status_summary.in_progress

        logger.info(f'Job {job.job_id}: {in_flight} in-flight items (max: {max_concurrent})')

        # If we're at or over the limit, don't queue more items
        if in_flight >= max_concurrent:
            logger.info(f'Job {job.job_id} at concurrency limit ({in_flight}/{max_concurrent}), skipping')
            return

        # Calculate how many items we can queue based on concurrency limit
        slots_available = max_concurrent - in_flight

        # Also consider circuit breaker limits - don't queue more items than needed to reach min_requests
        # This prevents queueing large batches when we're close to tripping the circuit
        circuit_policy = job.execution_policy.circuit_breaker if job.execution_policy and job.execution_policy.circuit_breaker else None
        if circuit_policy:
            completed_count = status_summary.success + status_summary.failed
            min_requests = circuit_policy.min_requests

            # If we haven't reached min_requests yet, limit batch size to avoid overshooting
            if completed_count < min_requests:
                remaining_until_min = min_requests - completed_count
                slots_available = min(slots_available, remaining_until_min)
                logger.info(f'Job {job.job_id}: {completed_count}/{min_requests} requests completed, limiting batch to {remaining_until_min} to avoid overshooting circuit breaker threshold')

        # Cap at 10 for SQS batch limit
        max_batch_size = min(slots_available, 10)

        logger.info(f'Job {job.job_id}: {slots_available} slots available, queueing up to {max_batch_size} items')

        # Collect items to batch
        items_to_queue: list[JobItem] = []

        for i in range(max_batch_size):
            # re-check circuit breaker before each item to catch recent failures
            failure_metrics = self.db.get_job_failure_metrics(job.job_id)
            circuit_ok = circuit.check_and_update(
                success_count=failure_metrics['success_count'],
                error_count=failure_metrics['error_count'],
                consecutive_errors=failure_metrics['consecutive_errors']
            )

            if not circuit_ok:
                # circuit just tripped during this batch, stop immediately
                logger.warning(f'Circuit breaker tripped mid-batch for job {job.job_id}, stopping after {i} items')
                self.db.update_job_status(job.job_id, 'failed')
                break

            # get next pending item
            item = self.db.get_next_pending_item(job.job_id)

            if not item:
                # no more pending items - check if job is complete
                logger.info(f'No more pending items for job {job.job_id}')
                await self.check_and_finalize_job(job)
                break

            logger.info(f'Got pending item: {item.item_id} for job {job.job_id}')

            # update status IMMEDIATELY to prevent duplicate queueing
            # This must happen before we add to items_to_queue
            self.db.update_item_status(job.job_id, item.item_id, 'queued')

            items_to_queue.append(item)

        # if we have items to queue, push them in batch to SQS
        if items_to_queue:
            # Push to SQS using enqueue method (takes sequence of JobItem)
            self.queue.enqueue(items_to_queue)

            logger.info(f'Scheduled {len(items_to_queue)} item(s) for job {job.job_id}')

    def get_circuit_breaker(self, job: Job) -> CircuitBreaker:
        """
        Get or create circuit breaker for job.

        Args:
            job: Job to get circuit breaker for

        Returns:
            CircuitBreaker for this job
        """
        if job.job_id not in self.circuit_breakers:
            # Read circuit breaker policy from ExecutionPolicy
            if job.execution_policy and job.execution_policy.circuit_breaker:
                policy = job.execution_policy.circuit_breaker
                self.circuit_breakers[job.job_id] = CircuitBreaker(
                    job_id=job.job_id,
                    min_requests=policy.min_requests,
                    failure_threshold_percentage=policy.failure_threshold_percentage,
                    consecutive_failure_threshold=20  # hardcoded for now
                )
            else:
                # Default circuit breaker settings
                self.circuit_breakers[job.job_id] = CircuitBreaker(
                    job_id=job.job_id,
                    min_requests=50,
                    failure_threshold_percentage=0.5,
                    consecutive_failure_threshold=20
                )

            logger.info(f"Created circuit breaker for job {job.job_id}")

        return self.circuit_breakers[job.job_id]

    def cleanup_finished_jobs(self):
        """Remove circuit breakers for completed jobs to free memory"""
        # Get list of active job IDs
        active_jobs = self.db.get_active_jobs()
        active_job_ids = {job.job_id for job in active_jobs}

        # Remove circuit breakers for jobs that are no longer active
        removed = 0
        for job_id in list(self.circuit_breakers.keys()):
            if job_id not in active_job_ids:
                del self.circuit_breakers[job_id]
                removed += 1

        if removed > 0:
            logger.info(f'Cleaned up {removed} circuit breaker(s) for finished jobs')

    async def check_and_finalize_job(self, job: Job) -> bool:
        """
        Check if job is complete and generate manifests if so.

        Args:
            job: Job to check

        Returns:
            True if job was finalized, False otherwise
        """
        # Check if job has any incomplete items
        status_summary = self.db.get_job_status(job.job_id)

        # Count incomplete items
        incomplete = status_summary.pending + status_summary.queued + status_summary.in_progress

        if incomplete > 0:
            # Job still has items to process
            return False

        # Job is complete! Generate manifests
        logger.info(f"Job {job.job_id} is complete, generating manifests")
        await self.generate_manifests(job)

        # Update job status to 'completed'
        self.db.update_job_status(job.job_id, 'completed')
        logger.info(f"Job {job.job_id} marked as completed")

        return True

    async def generate_manifests(self, job: Job):
        """
        Generate manifest files for a completed job.

        Creates:
        - {job_id}/manifests/full/part-{N}.json - all items (chunked)
        - {job_id}/manifests/success/part-{N}.json - successful items only (chunked)
        - {job_id}/manifests/errors/part-{N}.json - failed items only (chunked)
        - {job_id}/job_metadata.json - summary statistics

        Uses streaming to handle large jobs without loading everything into memory.

        Args:
            job: Job to generate manifests for
        """
        # Helper to convert item to manifest entry
        def item_to_manifest_entry(item: JobItem) -> dict[str, Any]:
            return {
                'item_id': item.item_id,
                'status': item.status,
                'storage_keys': item.output.storage_keys if item.output else None,
                'error_type': item.error_type,
                'retry_count': item.retry_count,
                'first_attempt_at': item.first_attempt_at,
                'last_attempt_at': item.last_attempt_at,
                'created_at': item.created_at,
                'updated_at': item.updated_at
            }

        # Track counts and timing
        total_items = 0
        success_count = 0
        error_count = 0
        first_attempt = None
        last_attempt = None

        full_part = 0
        success_part = 0
        error_part = 0

        # Process items in batches
        for batch in self.db.get_all_job_items(job.job_id, batch_size=1000):
            total_items += len(batch)

            # Separate by status
            success_batch: list[JobItem] = []
            error_batch: list[JobItem] = []

            for item in batch:
                # Track timing
                if item.first_attempt_at:
                    if not first_attempt or item.first_attempt_at < first_attempt:
                        first_attempt = item.first_attempt_at
                if item.last_attempt_at:
                    if not last_attempt or item.last_attempt_at > last_attempt:
                        last_attempt = item.last_attempt_at

                # Categorize
                if item.status == 'success':
                    success_count += 1
                    success_batch.append(item)
                elif item.status == 'failed':
                    error_count += 1
                    error_batch.append(item)

            # Upload full manifest chunk (all items in this batch)
            if batch:
                full_manifest_chunk = {
                    'job_id': job.job_id,
                    'part': full_part,
                    'items': [item_to_manifest_entry(item) for item in batch]
                }
                await self._upload_manifest(job.job_id, f'manifests/full/part-{full_part}.json', full_manifest_chunk)
                full_part += 1

            # Upload success manifest chunk
            if success_batch:
                success_manifest_chunk = {
                    'job_id': job.job_id,
                    'part': success_part,
                    'items': [item_to_manifest_entry(item) for item in success_batch]
                }
                await self._upload_manifest(job.job_id, f'manifests/success/part-{success_part}.json', success_manifest_chunk)
                success_part += 1

            # Upload error manifest chunk
            if error_batch:
                error_manifest_chunk = {
                    'job_id': job.job_id,
                    'part': error_part,
                    'items': [item_to_manifest_entry(item) for item in error_batch]
                }
                await self._upload_manifest(job.job_id, f'manifests/errors/part-{error_part}.json', error_manifest_chunk)
                error_part += 1

        # Calculate job duration
        job_duration_seconds = None
        if first_attempt and last_attempt:
            from datetime import datetime
            start = datetime.fromisoformat(first_attempt.replace('Z', '+00:00'))
            end = datetime.fromisoformat(last_attempt.replace('Z', '+00:00'))
            job_duration_seconds = (end - start).total_seconds()

        # Create metadata summary
        job_metadata = {
            'job_id': job.job_id,
            'job_name': job.job_name,
            'scraper_id': job.scraper_id,
            'status': 'completed',
            'total_items': total_items,
            'success_count': success_count,
            'error_count': error_count,
            'duration_seconds': job_duration_seconds,
            'created_at': job.created_at,
            'updated_at': job.updated_at,
            'manifest_info': {
                'full_parts': full_part,
                'success_parts': success_part,
                'error_parts': error_part
            }
        }

        # Upload metadata
        await self._upload_manifest(job.job_id, 'job_metadata.json', job_metadata)

        logger.info(f"Generated manifests for job {job.job_id}: {total_items} items in {full_part} parts")

    async def _upload_manifest(self, job_id: str, key_suffix: str, data: dict[str, Any]):
        """Upload a manifest file to S3."""
        key = f"{job_id}/{key_suffix}"
        await self.storage.upload_json(key, data)
        logger.debug(f"Uploaded manifest: {key}")
