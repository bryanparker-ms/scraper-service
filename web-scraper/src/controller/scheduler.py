import asyncio
import time
import logging
from typing import Any, Dict
from src.controller.circuit_breaker import CircuitBreaker
from src.shared.db import DynamoDBDatabaseService
from src.shared.queue import SqsQueue
from src.shared.settings import Settings
from src.shared.models import DEFAULT_MAX_CONCURRENT_WORKERS, Job, JobItem, ThrottlingPolicy
from src.shared.storage import S3ResultStorage

logger = logging.getLogger(__name__)


class JobScheduler:
    """
    Background scheduler that reads pending items from the DB and pushes them to the queue while enforcing concurrency limits. This runs as a background task in the controller service,
    providing centralized concurrency control without requiring a separate deployment.

    Concurrency control limits how many workers can process items from the same job simultaneously, preventing overwhelming target systems.
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
        self.last_queue_time: Dict[str, float] = {}  # Track last queue time per job for rate limiting
        self.running = False
        self.poll_interval = poll_interval_seconds

        logger.info(f'JobScheduler initialized (poll interval: {poll_interval_seconds}s)')

    async def start(self):
        """Start the scheduler background task"""
        self.running = True
        logger.info('Starting job scheduler loop')

        # Run scheduler loop
        while self.running:
            try:
                await self.schedule_cycle()
                await asyncio.sleep(self.poll_interval)
            except Exception as e:
                logger.error(f"Scheduler error: {e}", exc_info=True)
                await asyncio.sleep(self.poll_interval)  # Back off on error

        logger.info('Job scheduler loop stopped')

    async def stop(self):
        """Stop the scheduler gracefully"""
        logger.info('Stopping job scheduler')
        self.running = False

    async def schedule_cycle(self):
        """Single scheduling cycle... process all active jobs"""
        # get jobs that have queued items
        active_jobs = self.db.get_active_jobs()

        if not active_jobs:
            # no active jobs, nothing to do
            return

        logger.info(f'Found {len(active_jobs)} active job(s)')

        # process each job
        for job in active_jobs:
            try:
                await self.schedule_job(job)
            except Exception as e:
                logger.error(f'Error scheduling job {job.job_id}: {e}', exc_info=True)

        # periodically cleanup finished jobs (every ~10 seconds)
        if int(time.time()) % 10 == 0:
            self.cleanup_finished_jobs()

    async def schedule_job(self, job: Job):
        """
        Schedule items for a single job.

        Simple scheduling logic executed every cycle:
        1. Get status summary (pending, queued, in_progress, success, failed counts)
        2. Check circuit breaker - if error threshold exceeded, mark job as failed
        3. Check rate limiting - if min_delay not elapsed, skip this cycle
        4. Calculate available slots: max_concurrent - (queued + in_progress)
        5. Queue items to fill slots (1 item if rate limited, otherwise batch up to 10)

        Args:
            job: Job to schedule items for
        """
        logger.info(f'Scheduling job {job.job_id} (status: {job.status})')

        # step 1: Get status summary in a single query
        status_summary = self.db.get_job_status(job.job_id)
        in_flight = status_summary.queued + status_summary.in_progress

        logger.info(
            f'Job {job.job_id}: '
            f'pending={status_summary.pending}, '
            f'queued={status_summary.queued}, '
            f'in_progress={status_summary.in_progress}, '
            f'success={status_summary.success}, '
            f'failed={status_summary.failed}'
        )

        # step 2: Check circuit breaker (if configured)
        if job.execution_policy and job.execution_policy.circuit_breaker:
            circuit = self.get_circuit_breaker(job)

            # skip if circuit is already open
            if not circuit.should_allow_request():
                logger.info(f'Circuit breaker OPEN for job {job.job_id}, skipping')
                return

            # check if circuit should trip based on current metrics
            circuit_ok = circuit.check_and_update(
                success_count=status_summary.success,
                error_count=status_summary.failed,
                consecutive_errors=0  # Not tracked in summary, would need separate query if needed
            )

            if not circuit_ok:
                logger.warning(f'Circuit breaker tripped for job {job.job_id}, marking as failed')
                self.db.update_job_status(job.job_id, 'failed')
                return

        # Step 3: Check rate limiting (per-item delay)
        min_delay = 0.0
        if job.execution_policy and job.execution_policy.throttling:
            min_delay = job.execution_policy.throttling.min_delay_between_items_seconds

        if min_delay > 0:
            last_queue = self.last_queue_time.get(job.job_id, 0)
            elapsed = time.time() - last_queue

            if elapsed < min_delay:
                logger.debug(f'Job {job.job_id} rate limited: {elapsed:.1f}s elapsed < {min_delay}s required')
                return

        # Step 4: Calculate available slots
        max_concurrent = DEFAULT_MAX_CONCURRENT_WORKERS
        if job.execution_policy and job.execution_policy.throttling:
            max_concurrent = job.execution_policy.throttling.max_concurrent_workers

        # At capacity? Skip
        if in_flight >= max_concurrent:
            logger.info(f'Job {job.job_id} at capacity ({in_flight}/{max_concurrent}), skipping')
            return

        # Calculate how many items to queue
        # Per-item rate limiting: queue 1 item at a time
        items_to_queue_count = 1 if min_delay > 0 else min(max_concurrent - in_flight, 10)
        items_to_queue_count = min(items_to_queue_count, status_summary.pending)  # Don't queue more than exists

        # no items to queue?
        if items_to_queue_count <= 0:
            # check if job is complete (no pending or in-flight items)
            if in_flight == 0 and status_summary.pending == 0:
                logger.info(f'Job {job.job_id} has no work remaining, checking if complete')
                await self.check_and_finalize_job(job)
            return

        logger.info(f'Job {job.job_id}: queueing {items_to_queue_count} items ({in_flight}/{max_concurrent} in-flight)')

        # step 4: get and queue items
        items_to_queue: list[JobItem] = []

        for i in range(items_to_queue_count):
            item = self.db.get_next_pending_item(job.job_id)

            if not item:
                logger.warning(f'Expected pending item but got None (iteration {i}/{items_to_queue_count})')
                break

            # Update status immediately to prevent duplicate queueing
            self.db.update_item_status(job.job_id, item.item_id, 'queued')
            items_to_queue.append(item)

        # Queue to SQS
        if items_to_queue:
            self.queue.enqueue(items_to_queue)
            self.last_queue_time[job.job_id] = time.time()  # Update last queue time for rate limiting
            logger.info(f'Queued {len(items_to_queue)} item(s) for job {job.job_id}')

    def get_circuit_breaker(self, job: Job) -> CircuitBreaker:
        """
        Get or create circuit breaker for job.

        Args:
            job: Job to get circuit breaker for

        Returns:
            CircuitBreaker for this job
        """
        if job.job_id not in self.circuit_breakers:
            # read circuit breaker policy from ExecutionPolicy
            if job.execution_policy and job.execution_policy.circuit_breaker:
                policy = job.execution_policy.circuit_breaker
                self.circuit_breakers[job.job_id] = CircuitBreaker(
                    job_id=job.job_id,
                    min_requests=policy.min_requests,
                    failure_threshold_percentage=policy.failure_threshold_percentage,
                    consecutive_failure_threshold=20  # hardcoded for now
                )
            else:
                # default circuit breaker settings
                self.circuit_breakers[job.job_id] = CircuitBreaker(
                    job_id=job.job_id,
                    min_requests=50,
                    failure_threshold_percentage=0.5,
                    consecutive_failure_threshold=20
                )

            logger.info(f'Created circuit breaker for job {job.job_id}')

        return self.circuit_breakers[job.job_id]

    def cleanup_finished_jobs(self):
        """Remove circuit breakers for completed jobs to free memory"""
        # get list of active job IDs
        active_jobs = self.db.get_active_jobs()
        active_job_ids = {job.job_id for job in active_jobs}

        # remove circuit breakers for jobs that are no longer active
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
        # check if job has any incomplete items
        status_summary = self.db.get_job_status(job.job_id)

        # count incomplete items
        incomplete = status_summary.pending + status_summary.queued + status_summary.in_progress

        if incomplete > 0:
            # job still has items to process
            return False

        # job is complete! generate manifests
        logger.info(f'Job {job.job_id} is complete, generating manifests')
        await self.generate_manifests(job)

        # update job status to 'completed'
        self.db.update_job_status(job.job_id, 'completed')
        logger.info(f'Job {job.job_id} marked as completed')

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
        # helper to convert item to manifest entry
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

        # track counts and timing
        total_items = 0
        success_count = 0
        error_count = 0
        first_attempt = None
        last_attempt = None

        full_part = 0
        success_part = 0
        error_part = 0

        # process items in batches
        for batch in self.db.get_all_job_items(job.job_id, batch_size=1000):
            total_items += len(batch)

            # separate by status
            success_batch: list[JobItem] = []
            error_batch: list[JobItem] = []

            for item in batch:
                # track timing
                if item.first_attempt_at:
                    if not first_attempt or item.first_attempt_at < first_attempt:
                        first_attempt = item.first_attempt_at
                if item.last_attempt_at:
                    if not last_attempt or item.last_attempt_at > last_attempt:
                        last_attempt = item.last_attempt_at

                # categorize
                if item.status == 'success':
                    success_count += 1
                    success_batch.append(item)
                elif item.status == 'failed':
                    error_count += 1
                    error_batch.append(item)

            # upload full manifest chunk (all items in this batch)
            if batch:
                full_manifest_chunk = {
                    'job_id': job.job_id,
                    'part': full_part,
                    'items': [item_to_manifest_entry(item) for item in batch]
                }
                await self._upload_manifest(job.job_id, f'manifests/full/part-{full_part}.json', full_manifest_chunk)
                full_part += 1

            # upload success manifest chunk
            if success_batch:
                success_manifest_chunk = {
                    'job_id': job.job_id,
                    'part': success_part,
                    'items': [item_to_manifest_entry(item) for item in success_batch]
                }
                await self._upload_manifest(job.job_id, f'manifests/success/part-{success_part}.json', success_manifest_chunk)
                success_part += 1

            # upload error manifest chunk
            if error_batch:
                error_manifest_chunk = {
                    'job_id': job.job_id,
                    'part': error_part,
                    'items': [item_to_manifest_entry(item) for item in error_batch]
                }
                await self._upload_manifest(job.job_id, f'manifests/errors/part-{error_part}.json', error_manifest_chunk)
                error_part += 1

        # calculate job duration
        job_duration_seconds = None
        if first_attempt and last_attempt:
            from datetime import datetime
            start = datetime.fromisoformat(first_attempt.replace('Z', '+00:00'))
            end = datetime.fromisoformat(last_attempt.replace('Z', '+00:00'))
            job_duration_seconds = (end - start).total_seconds()

        # create metadata summary
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

        # upload metadata
        await self._upload_manifest(job.job_id, 'job_metadata.json', job_metadata)

        logger.info(f"Generated manifests for job {job.job_id}: {total_items} items in {full_part} parts")

    async def _upload_manifest(self, job_id: str, key_suffix: str, data: dict[str, Any]):
        """Upload a manifest file to S3."""
        key = f'{job_id}/{key_suffix}'
        await self.storage.upload_json(key, data)
        logger.debug(f'Uploaded manifest: {key}')
