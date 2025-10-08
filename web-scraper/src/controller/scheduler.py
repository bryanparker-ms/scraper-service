import asyncio
import time
import logging
from typing import Dict
from src.shared.db import DynamoDBDatabaseService
from src.shared.queue import SqsQueue
from src.shared.settings import Settings
from src.shared.models import Job, JobItem

logger = logging.getLogger(__name__)


class TokenBucket:
    """In-memory token bucket for rate limiting"""

    def __init__(self, rate: float, burst: float):
        """
        Initialize token bucket.

        Args:
            rate: Tokens per second (refill rate)
            burst: Maximum capacity (burst size)
        """
        self.rate = rate
        self.capacity = burst
        self.tokens = burst
        self.last_refill = time.time()

    def try_acquire(self, cost: float = 1.0) -> bool:
        """
        Try to acquire a token, return True if successful.

        Args:
            cost: Number of tokens to consume (default: 1.0)

        Returns:
            True if token acquired, False otherwise
        """
        now = time.time()
        elapsed = now - self.last_refill

        # Refill tokens based on elapsed time
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
        self.last_refill = now

        # Try to consume
        if self.tokens >= cost:
            self.tokens -= cost
            return True
        return False

    def tokens_available(self) -> float:
        """Get current token count (for observability)"""
        now = time.time()
        elapsed = now - self.last_refill
        return min(self.capacity, self.tokens + elapsed * self.rate)


class JobScheduler:
    """
    Background scheduler that reads queued items from DynamoDB
    and pushes them to SQS at the configured rate.

    This runs as a background task in the controller service, providing
    centralized rate limiting without requiring a separate deployment.
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
        self.rate_limiters: Dict[str, TokenBucket] = {}
        self.running = False
        self.poll_interval = poll_interval_seconds

        logger.info(f"JobScheduler initialized (poll interval: {poll_interval_seconds}s)")

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
            # No active jobs, nothing to do
            return

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

        Batches up to 10 items at a time if tokens are available.

        Args:
            job: Job to schedule items for
        """
        # Get or create rate limiter for this job
        limiter = self.get_rate_limiter(job)

        # Collect items to batch (up to 10 for SQS batch limit)
        items_to_queue: list[JobItem] = []
        max_batch_size = 10

        for _ in range(max_batch_size):
            # Try to acquire token
            if not limiter.try_acquire():
                # No more tokens available
                break

            # Get next pending item
            item = self.db.get_next_pending_item(job.job_id)

            if not item:
                # No more pending items
                break

            items_to_queue.append(item)

        # If we have items to queue, push them in batch
        if items_to_queue:
            # Push to SQS using enqueue method (takes sequence of JobItem)
            self.queue.enqueue(items_to_queue)

            # Update all item statuses: pending -> queued
            for item in items_to_queue:
                self.db.update_item_status(job.job_id, item.item_id, 'queued')

            logger.debug(f"Scheduled {len(items_to_queue)} item(s) for job {job.job_id}")

    def get_rate_limiter(self, job: Job) -> TokenBucket:
        """
        Get or create rate limiter for job.

        Args:
            job: Job to get rate limiter for

        Returns:
            TokenBucket for this job
        """
        if job.job_id not in self.rate_limiters:
            # Read rate from ExecutionPolicy.throttling
            if job.execution_policy and job.execution_policy.throttling:
                policy = job.execution_policy.throttling
                rate = policy.rate_limit_per_second
                # Use concurrent_requests as burst capacity
                burst = float(policy.concurrent_requests)
            else:
                # Default: 1 req/s with burst of 5
                rate = 1.0
                burst = 5.0

            self.rate_limiters[job.job_id] = TokenBucket(rate=rate, burst=burst)
            logger.info(f"Created rate limiter for job {job.job_id}: {rate} req/s, burst {burst}")

        return self.rate_limiters[job.job_id]

    def cleanup_finished_jobs(self):
        """Remove rate limiters for completed jobs to free memory"""
        # Get list of active job IDs
        active_jobs = self.db.get_active_jobs()
        active_job_ids = {job.job_id for job in active_jobs}

        # Remove limiters for jobs that are no longer active
        removed = 0
        for job_id in list(self.rate_limiters.keys()):
            if job_id not in active_job_ids:
                del self.rate_limiters[job_id]
                removed += 1

        if removed > 0:
            logger.info(f'Cleaned up {removed} rate limiter(s) for finished jobs')
