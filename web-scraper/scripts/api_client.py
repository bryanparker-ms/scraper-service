#!/usr/bin/env python3
"""
CLI client for interacting with the Web Scraper API.

Usage:
    uv run scripts/api_client.py --help
    uv run scripts/api_client.py jobs list
    uv run scripts/api_client.py jobs create --scraper-id maricopa-az --items-file items.jsonl
    uv run scripts/api_client.py jobs status <job_id>
    uv run scripts/api_client.py jobs results <job_id>
    uv run scripts/api_client.py jobs download <job_id> <item_id> --artifact html

Environment Variables:
    API_URL: Base URL for the API (default: http://localhost:8000)
    API_KEY: API key for authentication (optional for local development)
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, cast
import httpx

# Default configuration
DEFAULT_API_URL = os.getenv('API_URL', 'http://localhost:8000')
API_KEY = os.getenv('API_KEY')


class APIClient:
    """Client for interacting with the Web Scraper API."""

    def __init__(self, base_url: str, api_key: str | None = None):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.client = httpx.Client(timeout=60.0)

    def _headers(self) -> dict[str, str]:
        """Get request headers with optional API key."""
        headers = {'Content-Type': 'application/json'}
        if self.api_key:
            headers['X-API-Key'] = self.api_key
        return headers

    def _handle_response(self, response: httpx.Response) -> Any:
        """Handle API response and print errors."""
        try:
            response.raise_for_status()
            if response.headers.get('content-type', '').startswith('application/json'):
                return response.json()
            return response.text
        except httpx.HTTPStatusError as e:
            error_text = e.response.text
            try:
                # Try to pretty print error response if it's JSON
                error_json = json.loads(error_text)
                error_text = json.dumps(error_json, indent=2)
            except (json.JSONDecodeError, ValueError):
                # If not JSON, use as-is
                pass
            print(f'❌ HTTP {e.response.status_code}:\n{error_text}')
            sys.exit(1)
        except Exception as e:
            print(f'❌ Error: {e}')
            sys.exit(1)

    def list_jobs(self) -> dict[str, Any]:
        """List all jobs."""
        response = self.client.get(f'{self.base_url}/jobs', headers=self._headers())
        return self._handle_response(response)

    def create_job(
        self,
        job_id: str,
        scraper_id: str,
        items: list[dict[str, Any]],
        job_name: str | None = None,
        execution_policy: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Create a new job."""
        payload: dict[str, Any] = {
            'job_id': job_id,
            'scraper_id': scraper_id,
            'items': items
        }

        if job_name:
            payload['job_name'] = job_name

        if execution_policy:
            payload['execution_policy'] = execution_policy

        response = self.client.post(
            f'{self.base_url}/jobs',
            headers=self._headers(),
            json=payload
        )
        return self._handle_response(response)

    def get_job_status(self, job_id: str) -> dict[str, Any]:
        """Get job status."""
        response = self.client.get(
            f'{self.base_url}/jobs/{job_id}/status',
            headers=self._headers()
        )
        return self._handle_response(response)

    def get_job_results(
        self,
        job_id: str,
        filter: str | None = None,
        part: int | None = None
    ) -> dict[str, Any]:
        """Get job results."""
        params: dict[str, Any] = {}
        if filter:
            params['filter'] = filter
        if part is not None:
            params['part'] = part

        response = self.client.get(
            f'{self.base_url}/jobs/{job_id}/results',
            headers=self._headers(),
            params=params
        )
        return self._handle_response(response)

    def download_artifact(
        self,
        job_id: str,
        item_id: str,
        artifact: str,
        output_path: str
    ) -> None:
        """Download a job item artifact."""
        response = self.client.get(
            f'{self.base_url}/jobs/{job_id}/items/{item_id}/download',
            headers=self._headers(),
            params={'artifact': artifact}
        )

        try:
            response.raise_for_status()
            with open(output_path, 'wb') as f:
                f.write(response.content)
            print(f'✅ Downloaded to {output_path}')
        except httpx.HTTPStatusError as e:
            print(f'❌ HTTP {e.response.status_code}: {e.response.text}')
            sys.exit(1)

    def get_queue_length(self) -> int:
        """Get queue length."""
        response = self.client.get(
            f'{self.base_url}/jobs/queue/length',
            headers=self._headers()
        )
        return self._handle_response(response)

    def purge_queue(self) -> None:
        """Purge the queue."""
        response = self.client.post(
            f'{self.base_url}/jobs/queue/purge',
            headers=self._headers()
        )
        self._handle_response(response)
        print('✅ Queue purged')


def load_items_file(file_path: str) -> list[dict[str, Any]]:
    """Load items from a JSONL file."""
    items: list[dict[str, Any]] = []
    path = Path(file_path)

    if not path.exists():
        print(f'❌ File not found: {file_path}')
        sys.exit(1)

    try:
        with path.open('r') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue

                try:
                    data = json.loads(line)
                    if not isinstance(data, dict):
                        print(f'❌ Line {line_num} is not a JSON object')
                        sys.exit(1)

                    item = cast(dict[str, Any], data)

                    # Ensure item has required structure for API
                    if 'item_id' not in item:
                        print(f'❌ Line {line_num}: missing required "item_id" field')
                        sys.exit(1)

                    # If item doesn't have 'input' field, transform it
                    # by moving all fields except 'item_id' into 'input'
                    if 'input' not in item:
                        item_id = item.pop('item_id')
                        item = {
                            'item_id': item_id,
                            'input': item
                        }

                    items.append(item)
                except json.JSONDecodeError as e:
                    print(f'❌ Invalid JSON on line {line_num}: {e}')
                    sys.exit(1)

    except Exception as e:
        print(f'❌ Error reading file: {e}')
        sys.exit(1)

    return items


def load_execution_policy(file_path: str) -> dict[str, Any]:
    """Load execution policy from JSON file."""
    path = Path(file_path)

    if not path.exists():
        print(f'❌ File not found: {file_path}')
        sys.exit(1)

    try:
        with path.open('r') as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        print(f'❌ Invalid JSON in execution policy file: {e}')
        sys.exit(1)
    except Exception as e:
        print(f'❌ Error reading execution policy file: {e}')
        sys.exit(1)


def print_json(data: Any, indent: int = 2) -> None:
    """Pretty print JSON data."""
    print(json.dumps(data, indent=indent))


def main():
    parser = argparse.ArgumentParser(
        description='CLI client for Web Scraper API',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument(
        '--url',
        default=DEFAULT_API_URL,
        help=f'API base URL (default: {DEFAULT_API_URL})'
    )
    parser.add_argument(
        '--api-key',
        default=API_KEY,
        help='API key for authentication (default: from API_KEY env var)'
    )

    subparsers = parser.add_subparsers(dest='command', required=True, help='Command to execute')

    # Jobs commands
    jobs_parser = subparsers.add_parser('jobs', help='Job management commands')
    jobs_subparsers = jobs_parser.add_subparsers(dest='jobs_command', required=True)

    # List jobs
    jobs_subparsers.add_parser('list', help='List all jobs')

    # Create job
    create_parser = jobs_subparsers.add_parser('create', help='Create a new job')
    create_parser.add_argument('--job-id', required=True, help='Unique job ID')
    create_parser.add_argument('--job-name', help='Human-readable job name')
    create_parser.add_argument('--scraper-id', required=True, help='Scraper ID to use')
    create_parser.add_argument(
        '--items-file',
        required=True,
        help='Path to JSONL file with items'
    )
    create_parser.add_argument(
        '--execution-policy',
        help='Path to JSON file with execution policy'
    )

    # Job status
    status_parser = jobs_subparsers.add_parser('status', help='Get job status')
    status_parser.add_argument('job_id', help='Job ID')

    # Job results
    results_parser = jobs_subparsers.add_parser('results', help='Get job results')
    results_parser.add_argument('job_id', help='Job ID')
    results_parser.add_argument('--filter', choices=['all', 'success', 'errors'], help='Filter results')
    results_parser.add_argument('--part', type=int, help='Specific manifest part')

    # Download artifact
    download_parser = jobs_subparsers.add_parser('download', help='Download job item artifact')
    download_parser.add_argument('job_id', help='Job ID')
    download_parser.add_argument('item_id', help='Item ID')
    download_parser.add_argument(
        '--artifact',
        choices=['html', 'data', 'metadata', 'screenshot'],
        default='html',
        help='Artifact type to download'
    )
    download_parser.add_argument('-o', '--output', required=True, help='Output file path')

    # Queue commands
    queue_parser = subparsers.add_parser('queue', help='Queue management commands')
    queue_subparsers = queue_parser.add_subparsers(dest='queue_command', required=True)

    # Queue length
    queue_subparsers.add_parser('length', help='Get queue length')

    # Purge queue
    queue_subparsers.add_parser('purge', help='Purge the queue')

    args = parser.parse_args()

    # Initialize client
    client = APIClient(args.url, args.api_key)

    # Execute command
    try:
        if args.command == 'jobs':
            if args.jobs_command == 'list':
                result = client.list_jobs()
                print_json(result)

            elif args.jobs_command == 'create':
                items = load_items_file(args.items_file)

                execution_policy = None
                if args.execution_policy:
                    execution_policy = load_execution_policy(args.execution_policy)

                result = client.create_job(
                    job_id=args.job_id,
                    scraper_id=args.scraper_id,
                    items=items,
                    job_name=args.job_name,
                    execution_policy=execution_policy
                )
                print('✅ Job created successfully')
                print_json(result)

            elif args.jobs_command == 'status':
                result = client.get_job_status(args.job_id)
                print_json(result)

            elif args.jobs_command == 'results':
                result = client.get_job_results(
                    args.job_id,
                    filter=args.filter,
                    part=args.part
                )
                print_json(result)

            elif args.jobs_command == 'download':
                client.download_artifact(
                    args.job_id,
                    args.item_id,
                    args.artifact,
                    args.output
                )

        elif args.command == 'queue':
            if args.queue_command == 'length':
                length = client.get_queue_length()
                print(f'Queue length: {length}')

            elif args.queue_command == 'purge':
                client.purge_queue()

    except KeyboardInterrupt:
        print('\n⚠️  Interrupted')
        sys.exit(1)


if __name__ == '__main__':
    main()
