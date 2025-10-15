"""
Scraper registry for managing and selecting scrapers.

Simple class-based registry that maps scraper IDs to scraper classes. Uses decorator syntax for easy registration.
"""

from dataclasses import dataclass
from typing import Type

from src.worker.scraper import BaseScraper


@dataclass
class ScraperMetadata:
    """Metadata about a scraper."""
    scraper_id: str
    name: str
    version: str = '1.0.0'
    description: str | None = None


class ScraperRegistry:
    """
    Registry for managing scrapers.

    Simple implementation that stores scrapers in a dict.
    Can be extended later for file-based discovery or database-backed configs.
    """

    def __init__(self):
        self._scrapers: dict[str, tuple[Type[BaseScraper], ScraperMetadata]] = {}

    def register(
        self,
        scraper_id: str,
        name: str,
        version: str = '1.0.0',
        description: str | None = None
    ):
        """
        Decorator to register a scraper class.

        Usage:
            @registry.register(
                scraper_id="travis-county-tx",
                name="Travis County Property Tax Scraper",
                version="1.0.0"
            )
            class TravisCountyScraper(BaseHttpScraper):
                ...
        """
        def decorator(scraper_class: Type[BaseScraper]) -> Type[BaseScraper]:
            metadata = ScraperMetadata(
                scraper_id=scraper_id,
                name=name,
                version=version,
                description=description
            )

            self._scrapers[scraper_id] = (scraper_class, metadata)
            return scraper_class

        return decorator

    def get(self, scraper_id: str) -> Type[BaseScraper] | None:
        """Get scraper class by ID."""
        if scraper_id in self._scrapers:
            return self._scrapers[scraper_id][0]

        return None

    def get_metadata(self, scraper_id: str) -> ScraperMetadata | None:
        """Get metadata for a scraper."""
        if scraper_id in self._scrapers:
            return self._scrapers[scraper_id][1]

        return None

    def list_all(self) -> list[ScraperMetadata]:
        """List all registered scrapers."""
        return [metadata for _, metadata in self._scrapers.values()]

    def exists(self, scraper_id: str) -> bool:
        """Check if a scraper is registered."""
        return scraper_id in self._scrapers


# Global registry instance
registry = ScraperRegistry()
