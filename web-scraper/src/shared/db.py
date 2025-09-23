from typing import Any, Mapping, Optional
import boto3
from mypy_boto3_dynamodb.client import DynamoDBClient
from mypy_boto3_dynamodb.type_defs import AttributeValueTypeDef
from shared.interfaces import DatabaseService
from shared.models import Job, JobItem
from shared.settings import Settings


class DynamoDBDatabaseService(DatabaseService):
    def __init__(self, settings: Settings, ddb_client: Optional[DynamoDBClient] = None):
        self.settings = settings
        self.ddb_client = ddb_client or self.__create_ddb_client(settings)

    def create_job_if_not_exists(self):
        pass

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
                jobs.append(Job.model_validate(self._from_ddb_item(item)))

            if 'LastEvaluatedKey' not in resp:
                break

            start_key = resp['LastEvaluatedKey']

        # Sort by created_at in descending order and limit results
        jobs.sort(key=lambda x: x.created_at, reverse=True)
        return jobs[:limit]

    def _from_ddb_item(self, item: dict[str, AttributeValueTypeDef]) -> dict[str, Any]:
        resp: dict[str, Any] = {}
        for key, value in item.items():
            resp[key] = value.get('S') or value.get('N') or value.get('BOOL') or value.get('NULL') or value.get('M')

        return resp

    def __create_ddb_client(self, settings: Settings) -> DynamoDBClient:
        return boto3.client(
            'dynamodb',
            config=settings.boto_config,
            region_name=settings.aws_region,
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
        )
