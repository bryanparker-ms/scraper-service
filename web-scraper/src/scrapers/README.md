# Scrapers Directory

This directory contains all custom web scrapers for the scraping service.

## Structure

```
src/scrapers/
├── __init__.py              # Import all scrapers to register them
├── README.md                # This file
├── travis_county_tx.py      # Example: Travis County, TX scraper
├── williamson_county_tx.py  # Example: Williamson County, TX scraper
└── ...                      # More scrapers as you add them
```

## Creating a New Scraper

See [SCRAPER_TEMPLATE.md](../../SCRAPER_TEMPLATE.md) in the root directory for a complete template.

### Quick Start

1. **Create a new file** in this directory (e.g., `your_scraper.py`)

2. **Use the decorator to register your scraper**:

```python
from src.worker.registry import registry
from src.worker.scraper import BaseHttpScraper
from src.worker.models import ScrapeResult
from src.shared.models import JobItem
import httpx


@registry.register(
    scraper_id="your-scraper-id",
    name="Your Scraper Name",
    version="1.0.0",
    description="What this scraper does"
)
class YourScraper(BaseHttpScraper):
    async def _scrape_implementation(self, client: httpx.AsyncClient, job_item: JobItem) -> ScrapeResult:
        # Your scraping logic here
        response = await client.get("https://example.com")
        return ScrapeResult(
            html=response.text,
            data={"extracted": "data"},
            screenshot=None
        )
```

3. **Import your scraper** in `__init__.py`:

```python
# In src/scrapers/__init__.py
from src.scrapers.your_scraper import YourScraper
```

4. **Use it in a job** by specifying the `scraper_id`:

```json
{
  "job_id": "job-123",
  "scraper_id": "your-scraper-id",
  "items": [...]
}
```

## Available Scrapers

To see all registered scrapers:

```python
from src.worker.registry import registry

# List all scrapers
for metadata in registry.list_all():
    print(f"{metadata.scraper_id}: {metadata.name} v{metadata.version}")
```

## Example Scrapers

- **example-mock** - Simple mock scraper for testing (returns static data)
- **example-http** - HTTP scraper that makes real requests to httpbin.org

These are defined in `src/worker/scraper.py` for now, but will eventually be moved here.