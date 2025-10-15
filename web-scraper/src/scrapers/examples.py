"""
Example scrapers for testing and demonstration purposes.
"""

import httpx

from src.shared.models import JobItem
from src.worker.models import ScrapeResult
from src.worker.registry import registry
from src.worker.scraper import BaseHttpScraper, ScraperError


@registry.register(
    scraper_id='example-http',
    name='Example HTTP Scraper',
    version='1.0.0',
    description='Example HTTP scraper that makes real requests to httpbin.org'
)
class ExampleHttpScraper(BaseHttpScraper):
    """
    Example HTTP scraper that demonstrates real web scraping.
    Makes actual HTTP requests to httpbin.org for testing.
    """

    def validate_inputs(self, job_item: JobItem) -> None:
        """Validate required inputs for this scraper."""
        if 'test_param' not in job_item.input:
            from src.shared.models import NonRetryableError
            error_type: NonRetryableError = 'invalid_input'
            raise ScraperError('Missing required input: test_param', error_type)

    async def do_scrape(self, client: httpx.AsyncClient, job_item: JobItem) -> ScrapeResult:
        """
        Example implementation that makes real HTTP requests.
        Uses httpbin.org for reliable testing.

        Note: httpx exceptions (HTTPStatusError, TimeoutException, RequestError) are
        automatically caught and handled by the base class. Just write the happy path!
        """
        test_param = job_item.input['test_param']

        # Make a test HTTP request to httpbin.org
        # No need to catch httpx exceptions - the base class handles them!
        response = await client.get(f'https://httpbin.org/json', params={'test': test_param})
        response.raise_for_status()

        # Get the HTML content
        html_content = response.text

        # Parse JSON response for extracted data
        json_data = response.json()

        extracted_data = {
            'item_id': job_item.item_id,
            'test_param': test_param,
            'response_url': str(response.url),
            'status_code': response.status_code,
            'content_length': len(html_content),
            'httpbin_data': json_data
        }

        return ScrapeResult(
            html=html_content,
            data=extracted_data,
            screenshot=None
        )


@registry.register(
    scraper_id='example-mock',
    name='Example Mock Scraper',
    version='1.0.0',
    description='Simple mock scraper for testing that returns static data'
)
class ExampleScraper(BaseHttpScraper):
    """
    Simple mock scraper for basic testing (backwards compatibility).
    """

    async def do_scrape(self, client: httpx.AsyncClient, job_item: JobItem) -> ScrapeResult:
        """Mock implementation that returns test data."""
        mock_html = f"""
        <html>
        <head><title>Mock Page for {job_item.item_id}</title></head>
        <body>
            <h1>Property Information</h1>
            <div class="property-data">
                <p>Item ID: {job_item.item_id}</p>
                <p>Job ID: {job_item.job_id}</p>
                <p>Inputs: {job_item.input}</p>
            </div>
        </body>
        </html>
        """

        extracted_data = {
            'item_id': job_item.item_id,
            'processed_at': '2024-01-01T00:00:00Z',
            'mock_property_value': '$250,000',
            'mock_tax_amount': '$2,500'
        }

        return ScrapeResult(
            html=mock_html.strip(),
            data=extracted_data,
            screenshot=None
        )


@registry.register(
    scraper_id='example-fail',
    name='Example Scraper that fails',
    version='1.0.0',
    description='Simple scraper for testing that fails'
)
class ExampleFailScraper(BaseHttpScraper):
    """
    Simple scraper for testing that fails.
    """

    async def do_scrape(self, client: httpx.AsyncClient, job_item: JobItem) -> ScrapeResult:
        raise ValueError('This is a test failure')
