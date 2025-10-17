#!/usr/bin/env python3
"""
Test harness for running scrapers locally without infrastructure.

Usage:
    python scripts/test_scraper.py <scraper_id> [--input key=value] [--json]
    python scripts/test_scraper.py <scraper_id> --batch-file inputs.jsonl

Examples:
    # Single input
    python scripts/test_scraper.py maricopa-az --input parcel_number=200-14-030
    python scripts/test_scraper.py maricopa-az --input parcel_number=200-14-030 --json

    # Batch inputs from file
    python scripts/test_scraper.py maricopa-az --batch-file parcels.jsonl

Batch file format (JSONL - one JSON object per line):
    {"item_id": "1", "parcel_number": "200-14-030"}
    {"item_id": "2", "parcel_number": "200-14-031"}
    {"item_id": "3", "parcel_number": "200-14-032"}
"""

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any, cast

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.worker.registry import registry
from src.shared.models import JobItem, ExecutionPolicy
from src.worker.models import ScrapeResult
from src.worker.scraper import BaseHttpScraper

import src.scrapers  # type: ignore


def parse_input_args(input_args: list[str]) -> dict[str, Any]:
    """Parse --input key=value arguments into a dict."""
    inputs: dict[str, Any] = {}

    for arg in input_args:
        if '=' not in arg:
            raise ValueError(f'Invalid input format: {arg}. Expected key=value')

        key, value = arg.split('=', 1)
        inputs[key] = value

    return inputs


async def run_scraper(
    scraper_id: str,
    item_id: str,
    inputs: dict[str, Any],
    execution_policy: ExecutionPolicy | None = None,
    verbose: bool = True
) -> tuple[str, ScrapeResult | None, Exception | None]:
    """Run a scraper with the given inputs. Returns (item_id, result, error)."""

    # Get scraper from registry
    scraper_class = registry.get(scraper_id)
    if not scraper_class:
        available = ', '.join([meta.scraper_id for meta in registry.list_all()])
        print(f'Error: Scraper "{scraper_id}" not found')
        print(f'Available scrapers: {available}')
        sys.exit(1)

    # Create job item
    job_item = JobItem(
        item_id=item_id,
        job_id='test-job',
        input=inputs,
        status='pending'
    )

    # Instantiate scraper
    scraper = scraper_class(execution_policy=execution_policy or ExecutionPolicy())

    if verbose:
        print(f'⚡ Running scraper: {scraper_id}')
        print(f'🆔 Item ID: {item_id}')
        print(f'➡️  Input: {json.dumps(inputs, indent=2)}')
        print(f'\n{"="*60}\n')

    try:
        # Run the scraper
        result = await scraper.scrape(job_item)

        if verbose:
            print('✅ Scrape successful!\n')

            # Print extracted data
            print('📊 Extracted Data:')
            print(json.dumps(result.data, indent=2))

            # Print HTML preview
            if result.html:
                html_preview = result.html[:500].replace('\n', ' ')
                print(f'\n📄 HTML Preview ({len(result.html)} chars):')
                print(f'{html_preview}...')

            # Print screenshot info if present
            if result.screenshot:
                print(f'\n📸 Screenshot: {len(result.screenshot)} bytes')

        return (item_id, result, None)

    except Exception as e:
        if verbose:
            print(f'❌ Scrape failed: {e}')
            import traceback
            traceback.print_exc()
        return (item_id, None, e)
    finally:
        if isinstance(scraper, BaseHttpScraper):
            # Clean up
            await scraper.close()


def load_batch_file(file_path: str) -> list[dict[str, Any]]:
    """Load batch inputs from a JSONL file."""
    items: list[dict[str, Any]] = []
    path = Path(file_path)

    if not path.exists():
        print(f'Error: Batch file not found: {file_path}')
        sys.exit(1)

    try:
        with path.open('r') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue

                try:
                    item = json.loads(line)
                    if not isinstance(item, dict):
                        print(f'Error: Line {line_num} is not a JSON object')
                        sys.exit(1)

                    item = cast(dict[str, Any], item)

                    # Require item_id
                    if 'item_id' not in item:
                        print(f'Error: Line {line_num} missing required field "item_id"')
                        sys.exit(1)

                    items.append(item)
                except json.JSONDecodeError as e:
                    print(f'Error: Invalid JSON on line {line_num}: {e}')
                    sys.exit(1)

    except Exception as e:
        print(f'Error reading batch file: {e}')
        sys.exit(1)

    return items


async def run_batch(
    scraper_id: str,
    items: list[dict[str, Any]],
    execution_policy: ExecutionPolicy | None = None,
    concurrency: int = 1
) -> None:
    """Run scraper for multiple items with optional concurrency."""
    print(f'🔄 Running batch of {len(items)} items (concurrency: {concurrency})\n')

    results: list[tuple[str, ScrapeResult | None, Exception | None]] = []

    if concurrency == 1:
        # Sequential mode
        for idx, item in enumerate(items, 1):
            item_id = item['item_id']
            inputs = {k: v for k, v in item.items() if k != 'item_id'}

            print(f'\n{"="*60}')
            print(f'Item {idx}/{len(items)}: {item_id}')
            print(f'{"="*60}')

            result = await run_scraper(scraper_id, item_id, inputs, execution_policy, verbose=False)
            results.append(result)

            # Print result summary
            _, scrape_result, error = result
            if error:
                print(f'❌ Failed: {error}')
            else:
                print(f'✅ Success')
                if scrape_result and scrape_result.data:
                    print(f'📊 Data: {json.dumps(scrape_result.data, indent=2)}')
    else:
        # Concurrent mode using semaphore
        semaphore = asyncio.Semaphore(concurrency)

        async def run_with_semaphore(idx: int, item: dict[str, Any]) -> tuple[str, ScrapeResult | None, Exception | None]:
            async with semaphore:
                item_id = item['item_id']
                inputs = {k: v for k, v in item.items() if k != 'item_id'}

                print(f'▶️  Starting {item_id} ({idx}/{len(items)})')
                result = await run_scraper(scraper_id, item_id, inputs, execution_policy, verbose=False)

                _, scrape_result, error = result
                if error:
                    print(f'❌ {item_id}: Failed - {error}')
                else:
                    print(f'✅ {item_id}: Success')

                return result

        # Run all items concurrently
        tasks = [run_with_semaphore(idx, item) for idx, item in enumerate(items, 1)]
        results = await asyncio.gather(*tasks)

    # Print summary
    print(f'\n\n{"="*60}')
    print('📊 Batch Summary')
    print(f'{"="*60}')

    success_count = sum(1 for _, _, error in results if error is None)
    fail_count = len(results) - success_count

    print(f'Total: {len(results)}')
    print(f'✅ Success: {success_count}')
    print(f'❌ Failed: {fail_count}')

    if fail_count > 0:
        print('\nFailed items:')
        for item_id, _, error in results:
            if error:
                print(f'  • {item_id}: {error}')


def main():
    parser = argparse.ArgumentParser(
        description='Test harness for running scrapers locally',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument('scraper_id', nargs='?', help='Scraper ID to run')
    parser.add_argument(
        '--input',
        action='append',
        default=[],
        help='Input parameter (key=value). Can be specified multiple times.'
    )
    parser.add_argument(
        '--batch-file',
        type=str,
        help='Path to JSONL file with batch inputs'
    )
    parser.add_argument(
        '--concurrency',
        type=int,
        default=1,
        help='Number of concurrent scrapers to run (default: 1, sequential)'
    )
    parser.add_argument(
        '--json',
        action='store_true',
        help='Output full result as JSON'
    )
    parser.add_argument(
        '--list',
        action='store_true',
        help='List available scrapers'
    )

    args = parser.parse_args()

    # List scrapers if requested
    if args.list:
        print('Available scrapers:')
        for metadata in registry.list_all():
            print(f'  • {metadata.scraper_id}: {metadata.name}')

        sys.exit(0)

    # Require scraper_id if not listing
    if not args.scraper_id:
        parser.error('scraper_id is required (unless using --list)')

    # Batch mode
    if args.batch_file:
        items = load_batch_file(args.batch_file)
        asyncio.run(run_batch(args.scraper_id, items, concurrency=args.concurrency))
        sys.exit(0)

    # Single mode
    # Parse inputs
    try:
        inputs = parse_input_args(args.input)
    except ValueError as e:
        print(f'Error: {e}')
        sys.exit(1)

    if not inputs:
        print('Error: No inputs provided. Use --input key=value or --batch-file')
        sys.exit(1)

    # Run scraper
    _, result, error = asyncio.run(run_scraper(args.scraper_id, 'test-item', inputs))

    if error:
        sys.exit(1)

    # Output JSON if requested
    if args.json and result:
        print('\n' + '='*60)
        print('Full JSON Result:')
        print(json.dumps({
            'data': result.data,
            'html_length': len(result.html) if result.html else 0,
            'screenshot_length': len(result.screenshot) if result.screenshot else 0
        }, indent=2))


if __name__ == '__main__':
    main()
