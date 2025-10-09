import asyncio
import time
import logging
from typing import Dict, Literal
from src.shared.db import DynamoDBDatabaseService
from src.shared.queue import SqsQueue
from src.shared.settings import Settings
from src.shared.models import Job, JobItem

logger = logging.getLogger(__name__)

CircuitState = Literal['closed', 'open', 'half_open']


class CircuitBreaker:
    """
    Circuit breaker to detect failure patterns and stop processing jobs.

    Monitors job item success/failure rates and trips the circuit when
    failure thresholds are exceeded.
    """

    def __init__(
        self,
        job_id: str,
        min_requests: int = 50,
        failure_threshold_percentage: float = 0.5,
        consecutive_failure_threshold: int = 20,
        open_duration_seconds: int = 300
    ):
        """
        Initialize circuit breaker.

        Args:
            job_id: Job ID this circuit breaker monitors
            min_requests: Minimum requests before circuit can trip
            failure_threshold_percentage: Trip if failure rate exceeds this (0.0-1.0)
            consecutive_failure_threshold: Trip after this many consecutive failures
            open_duration_seconds: How long to keep circuit open (5 minutes default)
        """
        self.job_id = job_id
        self.min_requests = min_requests
        self.failure_threshold = failure_threshold_percentage
        self.consecutive_threshold = consecutive_failure_threshold
        self.open_duration = open_duration_seconds

        self.state: CircuitState = 'closed'
        self.tripped_at: float | None = None
        self.consecutive_failures = 0

    def should_allow_request(self) -> bool:
        """Check if requests should be allowed through the circuit."""
        if self.state == 'closed':
            return True

        if self.state == 'open':
            # Check if we should transition to half_open
            if self.tripped_at and (time.time() - self.tripped_at) > self.open_duration:
                logger.info(f'Circuit for job {self.job_id} transitioning to half_open (cooldown expired)')
                self.state = 'half_open'
                return True
            return False

        if self.state == 'half_open':
            # In half_open, allow limited requests to test recovery
            return True

        return False

    def check_and_update(self, success_count: int, error_count: int, consecutive_errors: int) -> bool:
        """
        Check if circuit should trip based on failure metrics.

        Args:
            success_count: Number of successful items
            error_count: Number of failed items
            consecutive_errors: Number of consecutive errors

        Returns:
            True if circuit is allowing requests, False if circuit is open
        """
        total = success_count + error_count

        # Update consecutive failure counter
        self.consecutive_failures = consecutive_errors

        # Check consecutive failure threshold
        if consecutive_errors >= self.consecutive_threshold:
            if self.state != 'open':
                self._trip_circuit(f"{consecutive_errors} consecutive failures")
            return False

        # Check failure rate (only if we have enough data)
        if total >= self.min_requests:
            failure_rate = error_count / total
            if failure_rate >= self.failure_threshold:
                if self.state != 'open':
                    self._trip_circuit(f"{failure_rate:.1%} failure rate ({error_count}/{total})")
                return False

        # If we're in half_open and seeing success, close the circuit
        if self.state == 'half_open' and consecutive_errors == 0:
            logger.info(f"Circuit for job {self.job_id} closing (recovery detected)")
            self.state = 'closed'
            self.tripped_at = None

        return self.state != 'open'

    def _trip_circuit(self, reason: str):
        """Trip the circuit breaker."""
        logger.warning(f"🔴 Circuit breaker TRIPPED for job {self.job_id}: {reason}")
        self.state = 'open'
        self.tripped_at = time.time()

    def get_state(self) -> CircuitState:
        """Get current circuit state."""
        return self.state


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
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
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
        Checks circuit breaker to prevent queueing items for failing jobs.

        Args:
            job: Job to schedule items for
        """
        # Get or create circuit breaker for this job
        circuit = self.get_circuit_breaker(job)

        # Check circuit breaker status
        if not circuit.should_allow_request():
            logger.debug(f'Circuit breaker OPEN for job {job.job_id}, skipping scheduling')
            return

        # Update circuit breaker with latest failure metrics
        failure_metrics = self.db.get_job_failure_metrics(job.job_id)
        circuit_ok = circuit.check_and_update(
            success_count=failure_metrics['success_count'],
            error_count=failure_metrics['error_count'],
            consecutive_errors=failure_metrics['consecutive_errors']
        )

        if not circuit_ok:
            # Circuit just tripped, stop scheduling and pause job
            logger.warning(f'Stopping scheduling for job {job.job_id} (circuit breaker tripped)')
            self.db.update_job_status(job.job_id, 'paused')
            return

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

            # Update status IMMEDIATELY to prevent duplicate queueing
            # This must happen before we add to items_to_queue
            self.db.update_item_status(job.job_id, item.item_id, 'queued')

            items_to_queue.append(item)

        # If we have items to queue, push them in batch to SQS
        if items_to_queue:
            # Push to SQS using enqueue method (takes sequence of JobItem)
            self.queue.enqueue(items_to_queue)

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
                    consecutive_failure_threshold=20,  # hardcoded for now
                    open_duration_seconds=policy.open_circuit_seconds
                )
            else:
                # Default circuit breaker settings
                self.circuit_breakers[job.job_id] = CircuitBreaker(
                    job_id=job.job_id,
                    min_requests=50,
                    failure_threshold_percentage=0.5,
                    consecutive_failure_threshold=20,
                    open_duration_seconds=300
                )

            logger.info(f"Created circuit breaker for job {job.job_id}")

        return self.circuit_breakers[job.job_id]

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
