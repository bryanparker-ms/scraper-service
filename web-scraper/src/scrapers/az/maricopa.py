import asyncio
from typing import Any
from bs4 import BeautifulSoup, Tag
import httpx

from src.shared.models import JobItem
from src.worker.models import ScrapeResult
from src.worker.registry import registry
from src.worker.scraper import BaseHttpScraper, ScraperError


@registry.register(
    scraper_id='maricopa-az',
    name='Maricopa County, AZ',
    version='0.0.1',
    description='Maricopa County, AZ scraper'
)
class MaricopaAZHttpScraper(BaseHttpScraper):
    def __init__(self, config: dict[str, Any] | None = None):
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari'}
        super().__init__({'headers': headers, **(config or {})})

    def validate_inputs(self, job_item: JobItem) -> None:
        if 'parcel_number' not in job_item.input:
            raise ScraperError('Missing required input: parcel_number', 'invalid_input')

    async def do_scrape(self, client: httpx.AsyncClient, job_item: JobItem) -> ScrapeResult:
        parcel_number = job_item.input['parcel_number'].replace('-', '')

        resp = await client.get(f'https://treasurer.maricopa.gov/Parcel?Parcel={parcel_number}')
        resp.raise_for_status()

        await asyncio.sleep(0.5)

        resp = await client.get(f'https://treasurer.maricopa.gov/Parcel/DetailedTaxStatement.aspx')
        soup = BeautifulSoup(resp.text, 'html.parser')
        resp.raise_for_status()

        html_content = resp.text

        owner_el = soup.select_one('#cphMainContent_cphRightColumn_dtlTaxBill_ParcelNASitusLegal_lblNameAddress div')
        address_el = soup.select_one('#cphMainContent_cphRightColumn_dtlTaxBill_ParcelNASitusLegal_lblSitusAddress')
        total_tax_el = soup.select_one('#cphMainContent_cphRightColumn_dtlTaxBill_lblGrandTotalCurrentYear')
        header_el = soup.select_one('#siteInnerContentContainer .rightColumn .panel-header h3')
        parcel_number_el = soup.select_one('.parcel-num')

        extracted_data = {
            'owner_and_address': self._extract_text(owner_el),
            'address': self._extract_text(address_el),
            'total_tax': total_tax_el.text.strip() if total_tax_el else None,
            'header': header_el.text.strip() if header_el else None,
            'parcel_number': parcel_number_el.text.strip() if parcel_number_el else None,
        }

        return ScrapeResult(
            html=html_content,
            data=extracted_data,
        )

    def _extract_text(self, el: Tag | None) -> str | None:
        if not el:
            return None

        for br in el.find_all('br'):
            br.replace_with('\n')

        return el.text.strip()
