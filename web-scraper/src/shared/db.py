from decimal import Decimal
from typing import Any, Mapping, Optional, cast
import typing
import boto3
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from mypy_boto3_dynamodb.client import DynamoDBClient
from mypy_boto3_dynamodb.type_defs import AttributeValueTypeDef
from src.controller.errors import JobNotFound
from src.shared.interfaces import DatabaseService
from src.shared.models import Job, JobItem, JobItemOutput, JobItemStatus, JobItemSummary, RetryableError, NonRetryableError
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
        last_attempt_at: str
    ) -> None:
        """Update job item with error status and details."""
        # Determine if this should be marked as 'error' or 'retrying'
        # Based on whether it's a retryable error
        from src.shared.models import JobItem as JobItemModel
        is_retryable = JobItemModel.is_retryable_error(error_type)

        # TODO: Also check retry_count against max_retries from execution policy
        # For now, we'll use status 'error' for non-retryable, 'retrying' for retryable
        status: JobItemStatus = 'retrying' if is_retryable else 'error'

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
