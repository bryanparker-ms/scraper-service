"""
Scraper registry module.

Import all scrapers here to register them with the registry.
Each scraper should use the @registry.register decorator.
"""

# Import base classes and registry (for reference)
from src.worker.registry import registry
from src.worker.scraper import BaseHttpScraper, BaseScraper

# Import example scrapers
from src.scrapers.examples import ExampleHttpScraper, ExampleScraper

from src.scrapers.az.maricopa import MaricopaAZHttpScraper

# Import all custom scrapers here as you create them
# Example:
# from src.scrapers.travis_county_tx import TravisCountyScraper
# from src.scrapers.williamson_county_tx import WilliamsonCountyScraper

__all__ = [
    'registry',
    'BaseHttpScraper',
    'BaseScraper',
    'ExampleHttpScraper',
    'ExampleScraper',
    'MaricopaAZHttpScraper',
]