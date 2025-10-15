"""Clear all items from DynamoDB table for testing."""
import boto3
from src.shared.settings import Settings

def clear_table():
    settings = Settings()
    ddb_client = boto3.client(
        'dynamodb',
        config=settings.boto_config,
        region_name=settings.aws_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )

    table_name = settings.table_name
    print(f"Clearing table: {table_name}")

    # Scan all items
    start_key = None
    deleted_count = 0

    while True:
        scan_params = {
            'TableName': table_name,
            'ProjectionExpression': 'pk, sk',  # Only fetch keys
        }

        if start_key:
            scan_params['ExclusiveStartKey'] = start_key

        resp = ddb_client.scan(**scan_params)

        # Delete items in batch
        items = resp.get('Items', [])
        if items:
            # Batch delete (max 25 at a time)
            for i in range(0, len(items), 25):
                batch = items[i:i+25]
                delete_requests = [
                    {'DeleteRequest': {'Key': {'pk': item['pk'], 'sk': item['sk']}}}
                    for item in batch
                ]

                ddb_client.batch_write_item(
                    RequestItems={
                        table_name: delete_requests
                    }
                )

                deleted_count += len(batch)
                print(f"Deleted {deleted_count} items...")

        if 'LastEvaluatedKey' not in resp:
            break

        start_key = resp['LastEvaluatedKey']

    print(f"Done! Deleted {deleted_count} total items.")


if __name__ == '__main__':
    clear_table()