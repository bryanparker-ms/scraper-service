import asyncio
import gzip
import json
import logging
from pathlib import Path
from typing import Any, Literal, Optional

import boto3
from mypy_boto3_s3.client import S3Client

from src.shared.interfaces import ResultStorage
from src.shared.models import ItemMetadata, JobItem, ManifestItem, StorageKeys
from src.shared.settings import Settings
from src.shared.utils import now_iso
from src.worker.models import ScrapeResult

logger = logging.getLogger(__name__)


class StorageError(Exception):
    """Raised when storage operations fail."""
    def __init__(self, message: str, errors: list[tuple[str, Exception]] | None = None):
        super().__init__(message)
        self.errors = errors or []


class S3ResultStorage(ResultStorage):
    """S3-based implementation of ResultStorage."""

    # Compression threshold in bytes (compress HTML if larger than this)
    COMPRESSION_THRESHOLD = 1024  # 1KB

    def __init__(self, settings: Settings, s3_client: Optional[S3Client] = None):
        self.settings = settings
        self.s3_client = s3_client or self._create_s3_client(settings)
        self.bucket = settings.bucket_name

    def _create_s3_client(self, settings: Settings) -> S3Client:
        client_kwargs: dict[str, Any] = {
            'config': settings.boto_config,
            'region_name': settings.aws_region,
        }

        # Only add credentials if they're provided (for local development)
        if settings.aws_access_key_id and settings.aws_secret_access_key:
            client_kwargs['aws_access_key_id'] = settings.aws_access_key_id
            client_kwargs['aws_secret_access_key'] = settings.aws_secret_access_key

        return boto3.client('s3', **client_kwargs)

    async def store_result(
        self,
        job_item: JobItem,
        result: ScrapeResult,
        metadata: ItemMetadata
    ) -> StorageKeys:
        """
        Store all artifacts for a scrape result.
        Uploads happen concurrently. Handles partial failures gracefully.
        """
        keys = StorageKeys()
        errors: list[tuple[str, Exception]] = []

        # Build S3 key prefix for this item
        prefix = f"{job_item.job_id}/items/{job_item.item_id}"

        # Prepare upload tasks
        upload_tasks: list[tuple[str, Any]] = []

        if result.html:
            upload_tasks.append(('html', self._upload_html(prefix, result.html)))

        if result.data:
            upload_tasks.append(('data', self._upload_data(prefix, result.data)))

        if result.screenshot:
            upload_tasks.append(('screenshot', self._upload_screenshot(prefix, result.screenshot)))

        # Always upload metadata (with storage keys populated after other uploads)
        # We'll do this separately after other uploads complete

        # Execute uploads in parallel
        if upload_tasks:
            results = await asyncio.gather(
                *[task for _, task in upload_tasks],
                return_exceptions=True
            )

            # Process results
            for (artifact_name, _), result_or_error in zip(upload_tasks, results):
                if isinstance(result_or_error, Exception):
                    errors.append((artifact_name, result_or_error))
                    logger.error(
                        f"Failed to upload {artifact_name} for item {job_item.item_id}: {result_or_error}"
                    )
                else:
                    # result_or_error is the S3 key
                    setattr(keys, artifact_name, result_or_error)
                    logger.debug(f"Uploaded {artifact_name} to {result_or_error}")

        # Update metadata with actual storage keys
        metadata.storage_keys = keys

        # Upload metadata
        try:
            metadata_key = await self._upload_metadata(prefix, metadata)
            keys.metadata = metadata_key
        except Exception as e:
            errors.append(('metadata', e))
            logger.error(f"Failed to upload metadata for item {job_item.item_id}: {e}")

        # If ALL uploads failed, raise exception
        total_artifacts = len(upload_tasks) + 1  # +1 for metadata
        if len(errors) == total_artifacts:
            raise StorageError(
                f"All artifact uploads failed for item {job_item.item_id}",
                errors
            )

        # Log if we had partial failures
        if errors:
            logger.warning(
                f"Partial storage failure for item {job_item.item_id}: "
                f"{len(errors)}/{total_artifacts} artifacts failed"
            )

        return keys

    async def _upload_html(self, prefix: str, html: str) -> str:
        """Upload HTML content, with optional compression."""
        key = f"{prefix}/html.html"
        content = html.encode('utf-8')
        extra_args: dict[str, Any] = {'ContentType': 'text/html; charset=utf-8'}

        # Compress if larger than threshold
        if len(content) > self.COMPRESSION_THRESHOLD:
            content = gzip.compress(content)
            extra_args['ContentEncoding'] = 'gzip'
            logger.debug(f"Compressed HTML for {key} (original: {len(html)} bytes)")

        await self._upload_bytes(key, content, extra_args)
        return key

    async def _upload_data(self, prefix: str, data: dict[str, Any]) -> str:
        """Upload extracted data as JSON."""
        key = f"{prefix}/data.json"
        content = json.dumps(data, indent=2).encode('utf-8')

        await self._upload_bytes(
            key,
            content,
            {'ContentType': 'application/json'}
        )
        return key

    async def _upload_metadata(self, prefix: str, metadata: ItemMetadata) -> str:
        """Upload item metadata as JSON."""
        key = f"{prefix}/metadata.json"
        content = metadata.model_dump_json(indent=2).encode('utf-8')

        await self._upload_bytes(
            key,
            content,
            {'ContentType': 'application/json'}
        )
        return key

    async def _upload_screenshot(self, prefix: str, screenshot: str) -> str:
        """Upload screenshot (base64 encoded or file path)."""
        key = f"{prefix}/screenshot.png"

        # For now, assume screenshot is base64 encoded string
        # TODO: Handle different formats (file path, bytes, etc.)
        import base64
        content = base64.b64decode(screenshot)

        await self._upload_bytes(
            key,
            content,
            {'ContentType': 'image/png'}
        )
        return key

    async def _upload_bytes(self, key: str, content: bytes, extra_args: dict[str, Any]) -> None:
        """Upload bytes to S3 using asyncio executor."""
        loop = asyncio.get_event_loop()

        def _sync_upload():
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=content,
                **extra_args
            )

        await loop.run_in_executor(None, _sync_upload)

    async def get_html(self, job_id: str, item_id: str) -> str:
        """Retrieve HTML for an item."""
        key = f"{job_id}/items/{item_id}/html.html"
        content = await self._download_bytes(key)

        # Check if content is gzipped
        try:
            # Try to decompress
            return gzip.decompress(content).decode('utf-8')
        except gzip.BadGzipFile:
            # Not gzipped, return as-is
            return content.decode('utf-8')

    async def get_data(self, job_id: str, item_id: str) -> dict[str, Any]:
        """Retrieve extracted data for an item."""
        key = f"{job_id}/items/{item_id}/data.json"
        content = await self._download_bytes(key)
        return json.loads(content.decode('utf-8'))

    async def get_metadata(self, job_id: str, item_id: str) -> ItemMetadata:
        """Retrieve metadata for an item."""
        key = f"{job_id}/items/{item_id}/metadata.json"
        content = await self._download_bytes(key)
        return ItemMetadata.model_validate_json(content.decode('utf-8'))

    async def _download_bytes(self, key: str) -> bytes:
        """Download bytes from S3 using asyncio executor."""
        loop = asyncio.get_event_loop()

        def _sync_download() -> bytes:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
            return response['Body'].read()

        return await loop.run_in_executor(None, _sync_download)

    async def upload_json(self, key: str, data: dict[str, Any] | list[Any]) -> None:
        """
        Upload arbitrary JSON data to S3.

        Args:
            key: S3 key (path) to upload to
            data: Dictionary or list to serialize as JSON
        """
        content = json.dumps(data, indent=2).encode('utf-8')
        await self._upload_bytes(
            key,
            content,
            {'ContentType': 'application/json'}
        )

    async def download_json(self, key: str) -> dict[str, Any] | list[Any]:
        """
        Download and parse JSON data from S3.

        Args:
            key: S3 key (path) to download from

        Returns:
            Parsed JSON data (dict or list)

        Raises:
            Exception: If file doesn't exist or JSON parsing fails
        """
        content = await self._download_bytes(key)
        return json.loads(content.decode('utf-8'))

    async def download_bytes(self, key: str) -> bytes:
        """
        Download raw bytes from S3.

        Args:
            key: S3 key (path) to download from

        Returns:
            Raw file content as bytes

        Raises:
            Exception: If file doesn't exist
        """
        return await self._download_bytes(key)

    async def update_manifest(
        self,
        job_id: str,
        item_id: str,
        storage_keys: StorageKeys,
        status: Literal['success', 'error']
    ) -> None:
        """
        Update job manifest with completed item.
        Stores individual manifest entry per item for easy aggregation later.
        """
        manifest_item = ManifestItem(
            item_id=item_id,
            status=status,
            storage_keys=storage_keys,
            completed_at=now_iso()
        )

        # Store as individual manifest entry
        key = f"{job_id}/manifests/items/{item_id}.json"
        content = manifest_item.model_dump_json(indent=2).encode('utf-8')

        await self._upload_bytes(
            key,
            content,
            {'ContentType': 'application/json'}
        )

        logger.debug(f"Updated manifest for item {item_id} in job {job_id}")


class LocalFilesystemStorage(ResultStorage):
    """Local filesystem implementation of ResultStorage for dev/testing."""

    COMPRESSION_THRESHOLD = 1024  # 1KB

    def __init__(self, base_path: str | Path = "/tmp/web-scraper-storage"):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

    async def store_result(
        self,
        job_item: JobItem,
        result: ScrapeResult,
        metadata: ItemMetadata
    ) -> StorageKeys:
        """Store all artifacts to local filesystem."""
        keys = StorageKeys()
        errors: list[tuple[str, Exception]] = []

        # Build directory path for this item
        item_dir = self.base_path / job_item.job_id / "items" / job_item.item_id
        item_dir.mkdir(parents=True, exist_ok=True)

        # Store HTML
        if result.html:
            try:
                html_path = item_dir / "html.html"
                content = result.html.encode('utf-8')

                # Compress if large
                if len(content) > self.COMPRESSION_THRESHOLD:
                    content = gzip.compress(content)
                    html_path = item_dir / "html.html.gz"

                html_path.write_bytes(content)
                keys.html = str(html_path.relative_to(self.base_path))
            except Exception as e:
                errors.append(('html', e))
                logger.error(f"Failed to store HTML: {e}")

        # Store data
        if result.data:
            try:
                data_path = item_dir / "data.json"
                data_path.write_text(json.dumps(result.data, indent=2))
                keys.data = str(data_path.relative_to(self.base_path))
            except Exception as e:
                errors.append(('data', e))
                logger.error(f"Failed to store data: {e}")

        # Store screenshot
        if result.screenshot:
            try:
                screenshot_path = item_dir / "screenshot.png"
                import base64
                screenshot_path.write_bytes(base64.b64decode(result.screenshot))
                keys.screenshot = str(screenshot_path.relative_to(self.base_path))
            except Exception as e:
                errors.append(('screenshot', e))
                logger.error(f"Failed to store screenshot: {e}")

        # Update metadata with storage keys
        metadata.storage_keys = keys

        # Store metadata
        try:
            metadata_path = item_dir / "metadata.json"
            metadata_path.write_text(metadata.model_dump_json(indent=2))
            keys.metadata = str(metadata_path.relative_to(self.base_path))
        except Exception as e:
            errors.append(('metadata', e))
            logger.error(f"Failed to store metadata: {e}")

        # Check if all uploads failed
        total_artifacts = sum([
            bool(result.html),
            bool(result.data),
            bool(result.screenshot),
            1  # metadata
        ])

        if len(errors) == total_artifacts:
            raise StorageError(
                f"All artifact stores failed for item {job_item.item_id}",
                errors
            )

        return keys

    async def get_html(self, job_id: str, item_id: str) -> str:
        """Retrieve HTML for an item."""
        item_dir = self.base_path / job_id / "items" / item_id

        # Try gzipped version first
        html_gz_path = item_dir / "html.html.gz"
        if html_gz_path.exists():
            return gzip.decompress(html_gz_path.read_bytes()).decode('utf-8')

        # Try uncompressed
        html_path = item_dir / "html.html"
        return html_path.read_text()

    async def get_data(self, job_id: str, item_id: str) -> dict[str, Any]:
        """Retrieve extracted data for an item."""
        data_path = self.base_path / job_id / "items" / item_id / "data.json"
        return json.loads(data_path.read_text())

    async def get_metadata(self, job_id: str, item_id: str) -> ItemMetadata:
        """Retrieve metadata for an item."""
        metadata_path = self.base_path / job_id / "items" / item_id / "metadata.json"
        return ItemMetadata.model_validate_json(metadata_path.read_text())

    async def update_manifest(
        self,
        job_id: str,
        item_id: str,
        storage_keys: StorageKeys,
        status: Literal['success', 'error']
    ) -> None:
        """Update job manifest with completed item."""
        manifest_item = ManifestItem(
            item_id=item_id,
            status=status,
            storage_keys=storage_keys,
            completed_at=now_iso()
        )

        # Store as individual manifest entry
        manifest_dir = self.base_path / job_id / "manifests" / "items"
        manifest_dir.mkdir(parents=True, exist_ok=True)

        manifest_path = manifest_dir / f"{item_id}.json"
        manifest_path.write_text(manifest_item.model_dump_json(indent=2))
