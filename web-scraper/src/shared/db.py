from decimal import Decimal
from typing import Any, Generator, Mapping, Optional, cast
import typing
import boto3
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from mypy_boto3_dynamodb.client import DynamoDBClient
from mypy_boto3_dynamodb.type_defs import AttributeValueTypeDef
from src.controller.errors import JobNotFound
from src.shared.interfaces import DatabaseService
from src.shared.models import Job, JobItem, JobItemOutput, JobItemStatus, JobItemSummary, JobStatus, RetryableError, NonRetryableError
from src.shared.settings import Settings
from src.shared.utils import item_sk, job_pk, now_iso

serializer = TypeSerializer()
deserializer = TypeDeserializer()

class DynamoDBDatabaseService(DatabaseService):
    def __init__(self, settings: Settings, ddb_client: Optional[DynamoDBClient] = None):
        self.settings = settings
        self.ddb_client = ddb_client or self.__create_ddb_client(settings)

    def create_job_if_not_exists(self, job: Job):
        self.ddb_client.put_item(
            TableName=self.settings.table_name,
            Item=_to_ddb_item(_job_to_ddb_item(job)),
            ConditionExpression='attribute_not_exists(pk)'
        )

    def job_exists(self, job_id: str) -> bool:
        resp = self.ddb_client.get_item(
            TableName=self.settings.table_name,
            Key={'pk': {'S': job_pk(job_id)}, 'sk': {'S': 'META'}},
            ConsistentRead=True,
        )

        return 'Item' in resp

    def get_job(self, job_id: str) -> Job:
        if not self.job_exists(job_id):
            raise JobNotFound(f'Job {job_id} not found')

        resp = self.ddb_client.get_item(
            TableName=self.settings.table_name,
            Key={'pk': {'S': job_pk(job_id)}, 'sk': {'S': 'META'}},
            ConsistentRead=True,
        )

        if 'Item' not in resp:
            raise JobNotFound(f'Job {job_id} not found')

        ddb_item = _from_ddb_item(resp['Item'])
        return Job.model_validate(ddb_item)

    def get_job_status(self, job_id: str) -> JobItemSummary:
        summary = JobItemSummary()
        statuses = cast(list[JobItemStatus], list(typing.get_args(JobItemStatus)))

        for status in statuses:
            count = self._count_items_by_status(job_id, status)

            if hasattr(summary, status):
                setattr(summary, status, count)

        return summary

    def _count_items_by_status(self, job_id: str, status: JobItemStatus) -> int:
        """Count items for a job with a specific status."""
        total_count = 0
        start_key: Mapping[str, AttributeValueTypeDef] = {}

        while True:
            query_params: dict[str, Any] = {
                'TableName': self.settings.table_name,
                'KeyConditionExpression': 'pk = :pk AND begins_with(sk, :sk_prefix)',
                'FilterExpression': '#status = :status',
                'ExpressionAttributeNames': {
                    '#status': 'status'
                },
                'ExpressionAttributeValues': {
                    ':pk': {'S': job_pk(job_id)},
                    ':sk_prefix': {'S': 'item#'},
                    ':status': {'S': status}
                },
                'Select': 'COUNT',  # This is the key - only return count, not data
                'ConsistentRead': True,
            }

            if start_key:
                query_params['ExclusiveStartKey'] = start_key

            resp = self.ddb_client.query(**query_params)
            total_count += resp['Count']

            if 'LastEvaluatedKey' not in resp:
                break

            start_key = resp['LastEvaluatedKey']

        return total_count

    def get_jobs(self, limit: int = 100) -> list[Job]:
        jobs: list[Job] = []
        start_key: Mapping[str, AttributeValueTypeDef] = {}

        while True:
            scan_params: dict[str, Any] = {
                'TableName': self.settings.table_name,
                'FilterExpression': '#type = :type AND #sk = :sk',
                'ExpressionAttributeNames': {
                    '#type': 'type',
                    '#sk': 'sk'
                },
                'ExpressionAttributeValues': {
                    ':type': {'S': 'job'},
                    ':sk': {'S': 'META'}
                },
                'Limit': self.settings.ddb_page_limit,
                'ConsistentRead': False,
            }

            if start_key:
                scan_params['ExclusiveStartKey'] = start_key

            resp = self.ddb_client.scan(**scan_params)

            for item in resp['Items']:
                # Convert DynamoDB item to regular dict
                jobs.append(Job.model_validate(_from_ddb_item(item)))

            if 'LastEvaluatedKey' not in resp:
                break

            start_key = resp['LastEvaluatedKey']

        # Sort by created_at in descending order and limit results
        jobs.sort(key=lambda x: x.created_at, reverse=True)
        return jobs[:limit]

    def create_job_item(self, job_id: str, job_item: JobItem):
        ddb_item = _job_item_to_ddb_item(job_item)

        self.ddb_client.put_item(
            TableName=self.settings.table_name,
            Item=_to_ddb_item(ddb_item),
            ConditionExpression='attribute_not_exists(pk) AND attribute_not_exists(sk)'
        )

    def get_job_item(self, job_id: str, item_id: str) -> JobItem:
        """Retrieve a specific job item."""
        resp = self.ddb_client.get_item(
            TableName=self.settings.table_name,
            Key={
                'pk': {'S': job_pk(job_id)},
                'sk': {'S': item_sk(item_id)}
            },
            ConsistentRead=True,
        )

        if 'Item' not in resp:
            raise ValueError(f"Job item {item_id} not found for job {job_id}")

        ddb_item = _from_ddb_item(resp['Item'])
        return JobItem.model_validate(ddb_item)

    def update_job_item_success(
        self,
        job_id: str,
        item_id: str,
        output: JobItemOutput,
        completed_at: str
    ) -> None:
        """Update job item with success status and output."""
        self.ddb_client.update_item(
            TableName=self.settings.table_name,
            Key={
                'pk': {'S': job_pk(job_id)},
                'sk': {'S': item_sk(item_id)}
            },
            UpdateExpression=(
                'SET #status = :status, '
                '#output = :output, '
                '#updated_at = :updated_at, '
                '#last_attempt_at = :last_attempt_at'
            ),
            ExpressionAttributeNames={
                '#status': 'status',
                '#output': 'output',
                '#updated_at': 'updated_at',
                '#last_attempt_at': 'last_attempt_at',
            },
            ExpressionAttributeValues=_to_ddb_item({
                ':status': 'success',
                ':output': output.model_dump(),
                ':updated_at': now_iso(),
                ':last_attempt_at': completed_at,
            })
        )

    def update_job_item_error(
        self,
        job_id: str,
        item_id: str,
        error_type: RetryableError | NonRetryableError,
        error_message: str,
        retry_count: int,
        last_attempt_at: str,
        max_retries: int = 3
    ) -> None:
        """
        Update job item with failed status and error details.

        Note: This should only be called when retries are exhausted (handled by scraper).
        The scraper retries internally, so by the time this is called, the item has failed
        permanently and should be marked as 'failed'.
        """
        status: JobItemStatus = 'failed'

        self.ddb_client.update_item(
            TableName=self.settings.table_name,
            Key={
                'pk': {'S': job_pk(job_id)},
                'sk': {'S': item_sk(item_id)}
            },
            UpdateExpression=(
                'SET #status = :status, '
                '#error_type = :error_type, '
                '#retry_count = :retry_count, '
                '#updated_at = :updated_at, '
                '#last_attempt_at = :last_attempt_at, '
                '#first_attempt_at = if_not_exists(#first_attempt_at, :first_attempt_at)'
            ),
            ExpressionAttributeNames={
                '#status': 'status',
                '#error_type': 'error_type',
                '#retry_count': 'retry_count',
                '#updated_at': 'updated_at',
                '#last_attempt_at': 'last_attempt_at',
                '#first_attempt_at': 'first_attempt_at',
            },
            ExpressionAttributeValues=_to_ddb_item({
                ':status': status,
                ':error_type': error_type,
                ':retry_count': retry_count,
                ':updated_at': now_iso(),
                ':last_attempt_at': last_attempt_at,
                ':first_attempt_at': last_attempt_at,
            })
        )

    def get_active_jobs(self) -> list[Job]:
        """
        Get all jobs that need scheduling (for scheduler).

        Returns jobs that are not yet completed, failed, or paused.
        """
        jobs: list[Job] = []
        start_key: Mapping[str, AttributeValueTypeDef] = {}

        while True:
            scan_params: dict[str, Any] = {
                'TableName': self.settings.table_name,
                'FilterExpression': '#type = :type AND #sk = :sk AND attribute_exists(#status) AND #status <> :completed AND #status <> :failed AND #status <> :paused',
                'ExpressionAttributeNames': {
                    '#type': 'type',
                    '#sk': 'sk',
                    '#status': 'status'
                },
                'ExpressionAttributeValues': {
                    ':type': {'S': 'job'},
                    ':sk': {'S': 'META'},
                    ':completed': {'S': 'completed'},
                    ':failed': {'S': 'failed'},
                    ':paused': {'S': 'paused'}
                },
                'Limit': self.settings.ddb_page_limit,
                'ConsistentRead': False,
            }

            if start_key:
                scan_params['ExclusiveStartKey'] = start_key

            resp = self.ddb_client.scan(**scan_params)

            for item in resp['Items']:
                jobs.append(Job.model_validate(_from_ddb_item(item)))

            if 'LastEvaluatedKey' not in resp:
                break

            start_key = resp['LastEvaluatedKey']

        return jobs

    def get_next_pending_item(self, job_id: str) -> JobItem | None:
        """
        Get next pending item for a job (for scheduler).

        Returns the first item with status='pending'.

        Due to how DynamoDB Limit works with FilterExpression, we need to scan
        through items to find pending ones. This function will scan through all
        items until it finds one with status='pending' or reaches the end.

        The query uses pagination to scan through items efficiently in batches of 100.

        Args:
            job_id: Job ID to search

        Returns:
            Next pending JobItem, or None if no pending items found
        """
        import logging
        logger = logging.getLogger(__name__)

        start_key: Mapping[str, AttributeValueTypeDef] = {}
        scanned_total = 0

        while True:
            query_params: dict[str, Any] = {
                'TableName': self.settings.table_name,
                'KeyConditionExpression': 'pk = :pk AND begins_with(sk, :sk_prefix)',
                'FilterExpression': '#status = :status',
                'ExpressionAttributeNames': {
                    '#status': 'status'
                },
                'ExpressionAttributeValues': {
                    ':pk': {'S': job_pk(job_id)},
                    ':sk_prefix': {'S': 'item#'},
                    ':status': {'S': 'pending'}
                },
                'Limit': 100,  # Scan in chunks of 100
                'ConsistentRead': True
            }

            if start_key:
                query_params['ExclusiveStartKey'] = start_key

            resp = self.ddb_client.query(**query_params)

            scanned_total += resp.get('ScannedCount', 0)
            items = resp.get('Items', [])

            if items:
                # Found a pending item!
                item = JobItem.model_validate(_from_ddb_item(items[0]))
                logger.info(f"Found pending item {item.item_id} after scanning {scanned_total} items")
                return item

            # No matches in this batch, continue if there are more items
            if 'LastEvaluatedKey' not in resp:
                # Reached end of all items, no pending items found
                logger.info(f"No pending items found after scanning all {scanned_total} items")
                return None

            start_key = resp['LastEvaluatedKey']

    def update_item_status(self, job_id: str, item_id: str, status: JobItemStatus) -> None:
        """
        Update item status (for scheduler: pending -> queued).

        Args:
            job_id: Job ID
            item_id: Item ID
            status: New status
        """
        self.ddb_client.update_item(
            TableName=self.settings.table_name,
            Key={
                'pk': {'S': job_pk(job_id)},
                'sk': {'S': item_sk(item_id)}
            },
            UpdateExpression='SET #status = :status, #updated_at = :updated_at',
            ExpressionAttributeNames={
                '#status': 'status',
                '#updated_at': 'updated_at'
            },
            ExpressionAttributeValues=_to_ddb_item({
                ':status': status,
                ':updated_at': now_iso()
            })
        )

    def get_job_failure_metrics(self, job_id: str) -> dict[str, int]:
        """
        Get failure metrics for circuit breaker.

        Query items with status in ('success', 'failed') to calculate:
        - success_count: Number of successful items
        - error_count: Number of failed items
        - consecutive_errors: Consecutive errors from most recent items

        Returns:
            Dict with success_count, error_count, consecutive_errors
        """
        # Query all completed items for this job
        items: list[dict[str, Any]] = []
        start_key: Mapping[str, AttributeValueTypeDef] = {}

        while True:
            query_params: dict[str, Any] = {
                'TableName': self.settings.table_name,
                'KeyConditionExpression': 'pk = :pk AND begins_with(sk, :sk_prefix)',
                'FilterExpression': '#status IN (:success, :failed)',
                'ExpressionAttributeNames': {
                    '#status': 'status'
                },
                'ExpressionAttributeValues': {
                    ':pk': {'S': job_pk(job_id)},
                    ':sk_prefix': {'S': 'item#'},
                    ':success': {'S': 'success'},
                    ':failed': {'S': 'failed'}
                },
                'ConsistentRead': True,
            }

            if start_key:
                query_params['ExclusiveStartKey'] = start_key

            resp = self.ddb_client.query(**query_params)

            for item in resp.get('Items', []):
                deserialized = _from_ddb_item(item)
                items.append(deserialized)

            if 'LastEvaluatedKey' not in resp:
                break

            start_key = resp['LastEvaluatedKey']

        # count successes and errors
        success_count = sum(1 for item in items if item.get('status') == 'success')
        error_count = sum(1 for item in items if item.get('status') == 'failed')

        # sort by last_attempt_at to get chronological order (most recent last). items without last_attempt_at go to beginning
        sorted_items = sorted(
            items,
            key=lambda x: x.get('last_attempt_at', '1970-01-01T00:00:00Z')
        )

        # count consecutive errors from the end (most recent)
        consecutive_errors = 0
        for item in reversed(sorted_items):
            if item.get('status') == 'failed':
                consecutive_errors += 1
            elif item.get('status') == 'success':
                # Hit a success, stop counting
                break

        return {
            'success_count': success_count,
            'error_count': error_count,
            'consecutive_errors': consecutive_errors
        }

    def update_job_status(self, job_id: str, status: JobStatus) -> None:
        """
        Update job status.

        Args:
            job_id: Job ID
            status: New status
        """
        self.ddb_client.update_item(
            TableName=self.settings.table_name,
            Key={
                'pk': {'S': job_pk(job_id)},
                'sk': {'S': 'META'}
            },
            UpdateExpression='SET #status = :status, #updated_at = :updated_at',
            ExpressionAttributeNames={
                '#status': 'status',
                '#updated_at': 'updated_at'
            },
            ExpressionAttributeValues=_to_ddb_item({
                ':status': status,
                ':updated_at': now_iso()
            })
        )

    def get_all_job_items(self, job_id: str, batch_size: int = 100) -> Generator[list[JobItem], Any, None]:
        """
        Get all job items for a job as an iterator (for manifest generation).

        Yields items in batches to avoid loading everything into memory at once.

        Args:
            job_id: Job ID
            batch_size: Number of items to yield at once (default: 100)

        Yields:
            Batches of job items
        """
        start_key: Mapping[str, AttributeValueTypeDef] = {}
        batch: list[JobItem] = []

        while True:
            query_params: dict[str, Any] = {
                'TableName': self.settings.table_name,
                'KeyConditionExpression': 'pk = :pk AND begins_with(sk, :sk_prefix)',
                'ExpressionAttributeValues': {
                    ':pk': {'S': job_pk(job_id)},
                    ':sk_prefix': {'S': 'item#'}
                },
                'ConsistentRead': True,
            }

            if start_key:
                query_params['ExclusiveStartKey'] = start_key

            resp = self.ddb_client.query(**query_params)

            for item in resp.get('Items', []):
                deserialized = _from_ddb_item(item)
                import logging
                logger = logging.getLogger(__name__)
                logger.info(f"Deserializing item: pk={deserialized.get('pk')}, sk={deserialized.get('sk')}, keys={list(deserialized.keys())}")
                batch.append(JobItem.model_validate(deserialized))

                # Yield batch when it reaches batch_size
                if len(batch) >= batch_size:
                    yield batch
                    batch = []

            if 'LastEvaluatedKey' not in resp:
                break

            start_key = resp['LastEvaluatedKey']

        # Yield any remaining items
        if batch:
            yield batch

    def __create_ddb_client(self, settings: Settings) -> DynamoDBClient:
        return boto3.client(
            'dynamodb',
            config=settings.boto_config,
            region_name=settings.aws_region,
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
        )


def _to_ddb_item(item: dict[str, Any]) -> dict[str, AttributeValueTypeDef]:
    return {k: serializer.serialize(_sanitize_for_ddb(v)) for k, v in item.items()}


def _from_ddb_item(item: dict[str, AttributeValueTypeDef]) -> dict[str, Any]:
    # resp: dict[str, Any] = {}
    # for key, value in item.items():
    #     resp[key] = value.get('S') or value.get('N') or value.get('BOOL') or value.get('NULL') or value.get('M')

    # return resp
    return {k: deserializer.deserialize(v) for k, v in item.items()}


def _sanitize_for_ddb(v: Any) -> Any:
    # Convert floats to Decimal (DynamoDB requirement)
    if isinstance(v, float):
        return Decimal(str(v))
    if isinstance(v, dict):
        return {k: _sanitize_for_ddb(val) for k, val in v.items()}  # type: ignore
    if isinstance(v, list):
        return [_sanitize_for_ddb(x) for x in v]  # type: ignore
    if isinstance(v, set):
        # Let TypeSerializer map to SS/NS/BS depending on members
        return {_sanitize_for_ddb(x) for x in v}  # type: ignore
    # bytes/memoryview are fine; serializer handles Binary
    return v


def _job_to_ddb_item(job: Job) -> dict[str, Any]:
    job_data = job.model_dump()

    # job_data.pop('job_id')

    return {
        'pk': job_pk(job.job_id),
        'sk': 'META',
        'type': 'job',
        **job_data,
    }


def _job_item_to_ddb_item(job_item: JobItem) -> dict[str, Any]:
    job_item_data = job_item.model_dump()

    # job_item_data.pop('job_id')
    # job_item_data.pop('item_id')

    return {
        'pk': job_pk(job_item.job_id),
        'sk': item_sk(job_item.item_id),
        'type': 'item',
        **job_item_data,
    }
