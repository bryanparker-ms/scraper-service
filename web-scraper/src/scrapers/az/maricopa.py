import asyncio
from typing import Any
import httpx

from src.shared.models import ExecutionPolicy, JobItem
from src.worker.models import ScrapeResult
from src.worker.registry import registry
from src.worker.scraper import BaseHttpScraper, ScraperError


@registry.register(
    scraper_id='maricopa-az',
    name='Maricopa County, AZ',
    version='1.0.0',
    description='Maricopa County, AZ scraper'
)
class MaricopaAZHttpScraper(BaseHttpScraper):
    def __init__(self, config: dict[str, Any] | None = None, execution_policy: ExecutionPolicy | None = None):
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari'}
        super().__init__({'headers': headers, **(config or {})}, execution_policy=execution_policy)

    def validate_inputs(self, job_item: JobItem) -> None:
        if 'parcel_number' not in job_item.input:
            raise ScraperError('Missing required input: parcel_number', 'invalid_input')

    async def do_scrape(self, client: httpx.AsyncClient, job_item: JobItem) -> ScrapeResult:
        parcel_number = job_item.input['parcel_number'].replace('-', '')

        resp = await client.get(f'https://treasurer.maricopa.gov/Parcel?Parcel={parcel_number}')
        resp.raise_for_status()

        await asyncio.sleep(0.5)

        resp = await client.get(f'https://treasurer.maricopa.gov/Parcel/DetailedTaxStatement.aspx')
        resp.raise_for_status()

        html_content = resp.text

        # Validate we got the right page (catch error pages early)
        self.assert_html_not_contains(html_content, 'No records found', 'Invalid parcel number format')
        self.assert_html_contains(html_content, 'Detailed Tax Statement', 'Did not reach tax statement page')

        # Parse HTML
        soup = self.parse_html(html_content)

        # Extract data using utilities
        extracted_data = {
            'owner_and_address': self.extract_text_by_selector(
                soup,
                '#cphMainContent_cphRightColumn_dtlTaxBill_ParcelNASitusLegal_lblNameAddress div',
                replace_br=True
            ),
            'address': self.extract_text_by_selector(
                soup,
                '#cphMainContent_cphRightColumn_dtlTaxBill_ParcelNASitusLegal_lblSitusAddress'
            ),
            'total_tax': self.extract_text_by_selector(
                soup,
                '#cphMainContent_cphRightColumn_dtlTaxBill_lblGrandTotalCurrentYear'
            ),
            'header': self.extract_text_by_selector(
                soup,
                '#siteInnerContentContainer .rightColumn .panel-header h3'
            ),
            'parcel_number': self.extract_text_by_selector(
                soup,
                '.parcel-num',
                required=True  # This should always be present
            ),
        }

        # Validate required fields are present
        self.assert_fields_present(extracted_data, ['parcel_number', 'total_tax'])

        return ScrapeResult(
            html=html_content,
            data=extracted_data,
        )

"""
Example requests:

curl -X POST http://localhost:8000/jobs \
    -H 'Content-Type: application/json' \
    -d '{
        "job_name": "Test Maricopa",
        "scraper_id": "maricopa-az",
        "items": [
            {"item_id":"200-14-030","input":{"parcel_number":"200-14-030"}}
        ]
    }'


curl -X POST http://localhost:8000/jobs \
    -H 'Content-Type: application/json' \
    -d '{
        "job_name": "Test Maricopa with Proxy",
        "scraper_id": "maricopa-az",
        "execution_policy": {
            "proxy": {
                "type": "residential",
                "geo_target": {
                    "state": "AZ"
                }
            },
            "retries": {
                "max_retries": 3,
                "backoff_strategy": "exponential",
                "backoff_factor": 1.0
            },
            "timeouts": {
                "connect_timeout_seconds": 10,
                "request_timeout_seconds": 30
            }
        },
        "items": [
            {"item_id":"200-14-030","input":{"parcel_number":"200-14-030"}}
        ]
    }'
"""