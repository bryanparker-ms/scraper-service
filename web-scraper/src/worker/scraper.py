import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Protocol, Union
import httpx
from httpx import TimeoutException, HTTPStatusError, RequestError

from src.shared.models import JobItem, RetryableError, NonRetryableError
from src.worker.models import ScrapeResult

# Import registry at the end to avoid circular imports
# Scrapers will register themselves when this module is imported

logger = logging.getLogger(__name__)


class ScraperError(Exception):
    """Base exception for scraper-related errors."""
    def __init__(self, message: str, error_type: Union[RetryableError, NonRetryableError], original_error: Optional[Exception] = None):
        super().__init__(message)
        self.error_type: Union[RetryableError, NonRetryableError] = error_type
        self.original_error: Optional[Exception] = original_error


class BaseScraper(Protocol):
    async def scrape(self, job_item: JobItem) -> ScrapeResult:
        """
        Perform the scraping operation for the given job item.

        Args:
            job_item: The job item containing inputs and metadata

        Returns:
            ScrapeResult: The result of the scraping operation
        """
        ...


class BaseHttpScraper(ABC):
    """
    Base class for HTTP-based scrapers using httpx.

    Provides common functionality:
    - HTTP client management with proxy support
    - Error handling and classification
    - Retry logic with exponential backoff
    - Request/response logging
    """

    def __init__(self, config: Dict[str, Any] | None = None):
        self.config = config or {}
        self._client: Optional[httpx.AsyncClient] = None

        # Default configuration
        self.max_retries = self.config.get('max_retries', 3)
        self.base_delay = self.config.get('base_delay', 1.0)
        self.max_delay = self.config.get('max_delay', 60.0)
        self.timeout = self.config.get('timeout', 30.0)

    async def scrape(self, job_item: JobItem) -> ScrapeResult:
        """
        Main scraping method with retry logic and error handling.
        """
        client = await self._get_client()

        last_error: Optional[Exception] = None

        for attempt in range(self.max_retries + 1):
            try:
                logger.debug(f"Scraping attempt {attempt + 1}/{self.max_retries + 1} for item {job_item.item_id}")

                # Validate inputs before processing
                self.validate_inputs(job_item)

                # Call the concrete scraper implementation
                # Wrap to catch httpx exceptions and convert them automatically
                try:
                    result = await self.do_scrape(client, job_item)
                except (HTTPStatusError, TimeoutException, RequestError) as httpx_error:
                    # Automatically classify and convert httpx exceptions to ScraperError
                    classified_error = self._classify_error(httpx_error)
                    raise ScraperError(
                        f"HTTP error: {httpx_error}",
                        classified_error,
                        httpx_error
                    )

                logger.info(f"Successfully scraped item {job_item.item_id} on attempt {attempt + 1}")
                return result

            except ScraperError as e:
                last_error = e
                logger.warning(f"Scraper error on attempt {attempt + 1} for item {job_item.item_id}: {e}")

                # Don't retry non-retryable errors
                if not self._is_retryable_error(e.error_type):
                    logger.info(f"Non-retryable error for item {job_item.item_id}, not retrying: {e.error_type}")
                    raise

                # Don't retry if this was the last attempt
                if attempt >= self.max_retries:
                    break

                # Wait before retry
                delay = min(self.base_delay * (2 ** attempt), self.max_delay)
                logger.debug(f"Waiting {delay}s before retry {attempt + 2}")
                await asyncio.sleep(delay)

            except Exception as e:
                # Catch any other unexpected errors (not httpx, not ScraperError)
                last_error = e
                logger.warning(f"Unexpected error on attempt {attempt + 1} for item {job_item.item_id}: {e}")

                # Classify as unexpected and convert to ScraperError
                error_type: RetryableError = "unexpected"
                scraper_error = ScraperError(str(e), error_type, e)

                if attempt >= self.max_retries:
                    raise scraper_error

                # Wait before retry for unexpected errors
                delay = min(self.base_delay * (2 ** attempt), self.max_delay)
                await asyncio.sleep(delay)

        # If we get here, all retries failed
        if last_error:
            if isinstance(last_error, ScraperError):
                raise last_error
            else:
                classified_error = self._classify_error(last_error)
                raise ScraperError(f"Max retries exceeded: {last_error}", classified_error, last_error)
        else:
            error_type: RetryableError = "unexpected"
            raise ScraperError("Max retries exceeded with unknown error", error_type)

    @abstractmethod
    async def do_scrape(self, client: httpx.AsyncClient, job_item: JobItem) -> ScrapeResult:
        """
        Implement the scraping logic for this specific scraper.

        This is the method you override to implement your scraping logic.
        The base class handles all the infrastructure:
        - HTTP client setup with proxy/timeout config
        - Automatic retry with exponential backoff
        - Automatic httpx error classification (timeouts, HTTP errors, network errors)
        - Logging and error tracking

        Call scrape() from outside, implement do_scrape() inside.

        Args:
            client: Configured httpx client ready to use
            job_item: Job item with inputs to process

        Returns:
            ScrapeResult: The scraping result with HTML, extracted data, and optional screenshot

        Raises:
            ScraperError: For custom business logic errors (e.g., no results found)
            Any other exception will be automatically classified and converted to ScraperError
        """
        pass

    def validate_inputs(self, job_item: JobItem) -> None:
        """
        Validate that job item has required inputs.
        Override in concrete scrapers for specific validation.
        """
        pass

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client with appropriate configuration."""
        if self._client is None:
            # Configure client with timeouts, headers, etc.
            timeout = httpx.Timeout(self.timeout)
            headers = self.config.get('headers', {})

            # Add proxy if configured
            proxy = None
            if 'proxy_url' in self.config:
                proxy = self.config['proxy_url']

            self._client = httpx.AsyncClient(
                timeout=timeout,
                headers=headers,
                proxy=proxy,
                follow_redirects=True
            )

        return self._client

    def _classify_error(self, error: Exception) -> Union[RetryableError, NonRetryableError]:
        """
        Classify an exception into retryable or non-retryable error types.
        """
        if isinstance(error, TimeoutException):
            return "timeout"
        elif isinstance(error, HTTPStatusError):
            status_code = error.response.status_code
            if status_code >= 500:
                return "server_error"
            elif status_code == 429:
                return "blocked"
            elif status_code == 404:
                return "not_found"
            elif status_code in (400, 401, 403):
                return "invalid_input"
            else:
                return "unexpected"
        elif isinstance(error, RequestError):
            return "network_error"
        else:
            return "unexpected"

    def _is_retryable_error(self, error_type: Union[RetryableError, NonRetryableError]) -> bool:
        """Check if an error type should be retried."""
        retryable_errors: tuple[RetryableError, ...] = (
            "timeout", "server_error", "network_error", "proxy_error", "blocked", "unexpected"
        )
        return error_type in retryable_errors

    async def close(self) -> None:
        """Clean up HTTP client."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()
