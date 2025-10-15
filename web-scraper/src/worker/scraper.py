import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Any, Protocol, Union
import httpx
from httpx import TimeoutException, HTTPStatusError, RequestError
from bs4 import BeautifulSoup, Tag

from src.shared.models import ExecutionPolicy, JobItem, ProxyPolicy, RetryableError, NonRetryableError
from src.worker.models import ScrapeResult

# Import registry at the end to avoid circular imports
# Scrapers will register themselves when this module is imported

logger = logging.getLogger(__name__)


class ScraperError(Exception):
    """Base exception for scraper-related errors."""
    def __init__(self, message: str, error_type: RetryableError | NonRetryableError, original_error: Exception | None = None):
        super().__init__(message)
        self.error_type: RetryableError | NonRetryableError = error_type
        self.original_error: Exception | None = original_error


class BaseScraper(Protocol):
    def __init__(self, execution_policy: ExecutionPolicy):
        ...

    async def scrape(self, job_item: JobItem) -> ScrapeResult:
        """
        Perform the scraping operation for the given job item.

        Args:
            job_item: The job item containing inputs and metadata

        Returns:
            ScrapeResult: The result of the scraping operation
        """
        ...


class BaseHttpScraper(ABC, BaseScraper):
    """
    Base class for HTTP-based scrapers using httpx.

    Provides common functionality:
    - HTTP client management with proxy support
    - Error handling and classification
    - Retry logic with exponential backoff
    - Request/response logging
    """

    def __init__(self, config: dict[str, Any] | None = None, execution_policy: ExecutionPolicy | None = None):
        self.config = config or {}
        self._client: httpx.AsyncClient | None = None
        self.execution_policy = execution_policy

        # Load retry configuration from ExecutionPolicy or defaults
        if execution_policy:
            self.max_retries = execution_policy.retries.max_retries
            self.backoff_strategy = execution_policy.retries.backoff_strategy
            self.backoff_factor = execution_policy.retries.backoff_factor
            self.timeout = execution_policy.timeouts.request_timeout_seconds
            self.connect_timeout = execution_policy.timeouts.connect_timeout_seconds
            self.proxy_config = execution_policy.proxy
        else:
            # Default configuration (fallback if no ExecutionPolicy provided)
            self.max_retries = self.config.get('max_retries', 3)
            self.backoff_strategy = self.config.get('backoff_strategy', 'exponential')
            self.backoff_factor = self.config.get('backoff_factor', 1.0)
            self.timeout = self.config.get('timeout', 30.0)
            self.connect_timeout = self.config.get('connect_timeout', 10.0)
            self.proxy_config = None

    async def scrape(self, job_item: JobItem) -> ScrapeResult:
        """
        Main scraping method with retry logic and error handling.
        """
        client = await self._get_client()

        last_error: Exception | None = None

        for attempt in range(self.max_retries + 1):
            try:
                logger.debug(f'Scraping attempt {attempt + 1}/{self.max_retries + 1} for item {job_item.item_id}')

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
                        f'HTTP error: {httpx_error}',
                        classified_error,
                        httpx_error
                    )

                # Post-scrape validation hook
                self.validate_result(result, job_item)

                logger.info(f'Successfully scraped item {job_item.item_id} on attempt {attempt + 1}')
                return result
            except ScraperError as e:
                last_error = e
                logger.warning(f'Scraper error on attempt {attempt + 1} for item {job_item.item_id}: {e}')

                # Don't retry non-retryable errors
                if not self._is_retryable_error(e.error_type):
                    logger.info(f'Non-retryable error for item {job_item.item_id}, not retrying: {e.error_type}')
                    raise

                # Don't retry if this was the last attempt
                if attempt >= self.max_retries:
                    break

                # Wait before retry with configurable backoff strategy
                delay = self._calculate_backoff_delay(attempt)
                logger.debug(f'Waiting {delay}s before retry {attempt + 2}')
                await asyncio.sleep(delay)

            except Exception as e:
                # Catch any other unexpected errors (not httpx, not ScraperError)
                last_error = e
                logger.warning(f'Unexpected error on attempt {attempt + 1} for item {job_item.item_id}: {e}')

                # Classify as unexpected and convert to ScraperError
                scraper_error = ScraperError(str(e), 'unexpected', e)

                if attempt >= self.max_retries:
                    raise scraper_error

                # Wait before retry for unexpected errors
                delay = self._calculate_backoff_delay(attempt)
                await asyncio.sleep(delay)

        # If we get here, all retries failed
        if last_error:
            if isinstance(last_error, ScraperError):
                raise last_error
            else:
                classified_error = self._classify_error(last_error)
                raise ScraperError(f'Max retries exceeded: {last_error}', classified_error, last_error)
        else:
            raise ScraperError('Max retries exceeded with unknown error', 'unexpected')

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

        Raises:
            ScraperError: If validation fails
        """
        pass

    def validate_result(self, result: ScrapeResult, job_item: JobItem) -> None:
        """
        Validate the scrape result after scraping completes.

        This is called automatically after do_scrape() returns successfully.
        Override in concrete scrapers to add custom validation logic.

        Common validations:
        - Check that HTML is not empty
        - Check that required data fields are present
        - Check for minimum HTML size
        - Check for error markers in HTML

        Args:
            result: The scrape result to validate
            job_item: The job item that was scraped

        Raises:
            ScraperError: If validation fails
        """
        pass

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client with appropriate configuration."""
        if self._client is None:
            # Configure client with timeouts (both connect and request)
            timeout = httpx.Timeout(
                connect=self.connect_timeout,
                read=self.timeout,
                write=self.timeout,
                pool=self.timeout
            )
            headers = self.config.get('headers', {})

            # Add proxy if configured via ExecutionPolicy or manual config
            proxy = None
            if self.proxy_config:
                proxy = self._build_proxy_url(self.proxy_config)
                logger.debug(f'Using {self.proxy_config.type} proxy for scraper')
            elif 'proxy_url' in self.config:
                # Fallback to manual proxy configuration
                proxy = self.config['proxy_url']

            self._client = httpx.AsyncClient(
                timeout=timeout,
                headers=headers,
                proxy=proxy,
                follow_redirects=True
            )

        return self._client

    def _build_proxy_url(self, proxy_policy: ProxyPolicy) -> str:
        """
        Build proxy URL from ProxyPolicy.

        Supports geo-targeting for BrightData proxies by appending geo parameters
        to the username string (e.g., username-country-us-state-ca).

        The proxy URL uses http:// scheme which works for both HTTP and HTTPS target sites.

        Args:
            proxy_policy: Proxy configuration from ExecutionPolicy

        Returns:
            Formatted proxy URL: http://username:password@host:port

        Raises:
            ValueError: If proxy credentials are not configured in environment
        """
        from src.shared.settings import Settings
        settings = Settings()

        # Get proxy credentials from settings (raises ValueError if not configured)
        proxy_config = settings.proxy_config(proxy_policy.type)
        proxy_host = proxy_config['proxy_url']  # e.g., brd.superproxy.io:33335
        proxy_username = proxy_config['proxy_username']  # username from env
        proxy_password = proxy_config['proxy_password']

        # Apply geo-targeting based on proxy type
        # - Residential: supports state/city targeting
        # - Datacenter/ISP: only country-level (configured in zone)
        # - Web unlocker: no geo-targeting support
        final_username = proxy_username

        if proxy_policy.geo_target and (proxy_policy.geo_target.state or proxy_policy.geo_target.city):
            if proxy_policy.type == 'residential':
                # Residential proxies support granular geo-targeting
                username_parts = [proxy_username + '-country-us']

                if proxy_policy.geo_target.state:
                    state_code = proxy_policy.geo_target.state.lower()
                    username_parts.append(f'state-{state_code}')

                if proxy_policy.geo_target.city:
                    city_slug = proxy_policy.geo_target.city.lower().replace(' ', '_')
                    username_parts.append(f'city-{city_slug}')

                final_username = '-'.join(username_parts)
                logger.debug(f'Applied residential proxy geo-targeting: state={proxy_policy.geo_target.state}, city={proxy_policy.geo_target.city}')
            else:
                # Datacenter/web-unlocker don't support state/city targeting
                logger.warning(
                    f"Geo-targeting (state/city) specified for '{proxy_policy.type}' proxy, "
                    f"but this proxy type only supports country-level geo-targeting. "
                    f"Ignoring state/city parameters and using zone configuration."
                )

        # Build proxy URL - use http:// scheme (works for both HTTP and HTTPS targets)
        proxy_url = f'http://{final_username}:{proxy_password}@{proxy_host}'

        logger.debug(f'Using {proxy_policy.type} proxy: {final_username}@{proxy_host}')

        return proxy_url

    def _classify_error(self, error: Exception) -> Union[RetryableError, NonRetryableError]:
        """
        Classify an exception into retryable or non-retryable error types.
        """
        if isinstance(error, TimeoutException):
            return 'timeout'
        elif isinstance(error, HTTPStatusError):
            status_code = error.response.status_code
            if status_code >= 500:
                return 'server_error'
            elif status_code == 429:
                return 'blocked'
            elif status_code == 404:
                return 'not_found'
            elif status_code in (400, 401, 403):
                return 'invalid_input'
            else:
                return 'unexpected'
        elif isinstance(error, RequestError):
            return 'network_error'
        else:
            return 'unexpected'

    def _is_retryable_error(self, error_type: Union[RetryableError, NonRetryableError]) -> bool:
        """Check if an error type should be retried."""
        retryable_errors: tuple[RetryableError, ...] = (
            'timeout', 'server_error', 'network_error', 'proxy_error', 'blocked', 'unexpected'
        )

        return error_type in retryable_errors

    def _calculate_backoff_delay(self, attempt: int) -> float:
        """
        Calculate backoff delay based on configured strategy.

        Args:
            attempt: Current attempt number (0-indexed)

        Returns:
            Delay in seconds
        """
        if self.backoff_strategy == 'exponential':
            # Exponential backoff: base * factor * (2^attempt)
            delay = self.backoff_factor * (2 ** attempt)
        else:  # linear
            # Linear backoff: base * factor * attempt
            delay = self.backoff_factor * (attempt + 1)

        # Cap at maximum delay (60 seconds default)
        return min(delay, 60.0)

    # ========== HTML Parsing Utilities ==========

    def parse_html(self, html: str, parser: str = 'html.parser') -> BeautifulSoup:
        """
        Parse HTML string into BeautifulSoup object.

        Args:
            html: HTML string to parse
            parser: Parser to use (default: 'html.parser')

        Returns:
            BeautifulSoup object
        """
        return BeautifulSoup(html, parser)

    def select_one(self, soup: BeautifulSoup, selector: str, required: bool = False) -> Tag | None:
        """
        Select a single element by CSS selector.

        Args:
            soup: BeautifulSoup object to search in
            selector: CSS selector
            required: If True, raises ScraperError if element not found

        Returns:
            Tag if found, None otherwise

        Raises:
            ScraperError: If required=True and element not found
        """
        element = soup.select_one(selector)
        if required and not element:
            raise ScraperError(f'Required element not found: {selector}', 'no_results')
        return element

    def extract_text(
        self,
        element: Tag | None,
        strip: bool = True,
        replace_br: bool = True,
        default: str | None = None
    ) -> str | None:
        """
        Extract text from a BeautifulSoup element.

        Args:
            element: BeautifulSoup Tag or None
            strip: Strip whitespace from result (default: True)
            replace_br: Replace <br> tags with newlines (default: True)
            default: Value to return if element is None (default: None)

        Returns:
            Extracted text or default value
        """
        if element is None:
            return default

        # Replace <br> tags with newlines
        if replace_br:
            for br in element.find_all('br'):
                br.replace_with('\n')

        text = element.get_text()
        return text.strip() if strip else text

    def extract_text_by_selector(
        self,
        soup: BeautifulSoup,
        selector: str,
        strip: bool = True,
        replace_br: bool = True,
        default: str | None = None,
        required: bool = False
    ) -> str | None:
        """
        Select element by CSS selector and extract its text.

        Combines select_one() and extract_text() for convenience.

        Args:
            soup: BeautifulSoup object to search in
            selector: CSS selector
            strip: Strip whitespace from result (default: True)
            replace_br: Replace <br> tags with newlines (default: True)
            default: Value to return if element not found (default: None)
            required: If True, raises ScraperError if element not found

        Returns:
            Extracted text or default value

        Raises:
            ScraperError: If required=True and element not found
        """
        element = self.select_one(soup, selector, required=required)
        return self.extract_text(element, strip=strip, replace_br=replace_br, default=default)

    # ========== Validation Utilities ==========

    def assert_html_contains(self, html: str, expected: str, error_msg: str | None = None) -> None:
        """
        Assert HTML contains expected string, raise ScraperError if not.

        Args:
            html: HTML content to check
            expected: String that should be present
            error_msg: Custom error message (optional)

        Raises:
            ScraperError: If expected string not found
        """
        if expected not in html:
            raise ScraperError(error_msg or f'Expected "{expected}" not found in HTML', 'no_results')

    def assert_html_not_contains(self, html: str, unexpected: str, error_msg: str | None = None) -> None:
        """
        Assert HTML does NOT contain unexpected string.

        Args:
            html: HTML content to check
            unexpected: String that should NOT be present
            error_msg: Custom error message (optional)

        Raises:
            ScraperError: If unexpected string found
        """
        if unexpected in html:
            raise ScraperError(error_msg or f'Unexpected "{unexpected}" found in HTML', 'no_results')

    def assert_fields_present(self, data: dict[str, Any], required_fields: list[str]) -> None:
        """
        Assert all required fields are present in extracted data.

        Args:
            data: Dictionary of extracted data
            required_fields: List of field names that must be present and non-empty

        Raises:
            ScraperError: If any required fields are missing or empty
        """
        missing = [f for f in required_fields if f not in data or not data[f]]
        if missing:
            raise ScraperError(f'Missing required fields: {missing}', 'no_results')

    async def close(self) -> None:
        """Clean up HTTP client."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()
