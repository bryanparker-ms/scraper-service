import time
import logging
from typing import Literal

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
