import json
from typing import Optional, Sequence

from mypy_boto3_sqs.type_defs import SendMessageBatchRequestEntryTypeDef
from src.shared.interfaces import QueueResponse, QueueService
from src.shared.models import JobItem
from src.shared.settings import Settings
from mypy_boto3_sqs.client import SQSClient
import boto3


class SqsQueue(QueueService):
    def __init__(self, settings: Settings, sqs_client: Optional[SQSClient] = None):
        self.settings = settings
        self.sqs_client = self.__create_sqs_client(settings)

    def enqueue(self, job_items: Sequence[JobItem]) -> None:
        queue_entries = _to_sqs_messages(job_items)
        self.sqs_client.send_message_batch(QueueUrl=self.settings.queue_url, Entries=queue_entries)

    def dequeue(self) -> Optional[QueueResponse]:
        resp = self.sqs_client.receive_message(
            QueueUrl=self.settings.queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10,
            VisibilityTimeout=self.settings.visibility_timeout,
        )

        msgs = resp.get('Messages', [])
        if not msgs:
            return None

        msg = msgs[0]
        body = msg.get('Body')
        receipt_handle = msg.get('ReceiptHandle')
        message_id = msg.get('MessageId')

        if not body or not receipt_handle or not message_id:
            return None

        try:
            # return json.loads(body), receipt_handle, message_id
            body = json.loads(body)
            job_item = JobItem.model_validate(body)

            return job_item, receipt_handle, message_id
        except Exception as e:
            print(f'Error parsing message: {e}')
            # Make message immediately visible and delete it (poison message handling)
            self.sqs_client.change_message_visibility(
                QueueUrl=self.settings.queue_url,
                ReceiptHandle=receipt_handle,
                VisibilityTimeout=0
            )
            self.sqs_client.delete_message(
                QueueUrl=self.settings.queue_url,
                ReceiptHandle=receipt_handle
            )

            return None

    def __create_sqs_client(self, settings: Settings) -> SQSClient:
        return boto3.client('sqs', region_name=settings.aws_region, config=settings.boto_config)


def _to_sqs_messages(job_items: Sequence[JobItem]) -> Sequence[SendMessageBatchRequestEntryTypeDef]:
    return [{
        'Id': f'{job_item.job_id}-{i}',
        'MessageBody': json.dumps({
            'job_id': job_item.job_id,
            'item_id': job_item.item_id,
            'inputs': job_item.input,
        })
    } for i, job_item in enumerate(job_items)]

