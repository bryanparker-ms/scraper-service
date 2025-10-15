import logging
import uuid
import json
from typing import Any, Literal
from fastapi.responses import Response
from src.controller.models import CreateJobRequest, CreateJobResponse, ErrorResponse, GetJobsResponse, JobStatusResponse, JobSummary
from src.shared.db import DynamoDBDatabaseService
from src.shared.models import Job, JobItem
from src.shared.queue import SqsQueue
from src.shared.settings import Settings
from src.shared.storage import S3ResultStorage

logger = logging.getLogger(__name__)


settings = Settings()
db = DynamoDBDatabaseService(settings)
queue = SqsQueue(settings)
storage = S3ResultStorage(settings)

def get_latest_jobs(n: int = 100) -> GetJobsResponse:
    """
    Get the last `n` jobs
    """
    jobs = db.get_jobs(n)

    summaries = [JobSummary(
        job_id=job.job_id,
        status=job.status,
        created_at=job.created_at,
        updated_at=job.updated_at,
        total_items=job.total_items
    ) for job in jobs]

    return GetJobsResponse(jobs=summaries)


def create_job(request: CreateJobRequest) -> CreateJobResponse | ErrorResponse:
    """
    Create a new job.
    """
    job_id = request.job_id or str(uuid.uuid4())

    if db.job_exists(job_id):
        return ErrorResponse(status=409, message='Job already exists')

    job = Job(
        job_id=job_id,
        total_items=len(request.items),
    )

    if request.execution_policy:
        job.execution_policy = request.execution_policy

    if request.job_name:
        job.job_name = request.job_name

    if request.scraper_id:
        job.scraper_id = request.scraper_id

    db.create_job_if_not_exists(job)

    # create job items with status='pending'. the scheduler will pick these up and push to SQS at the configured rate.
    for item in request.items:
        job_item = JobItem(
            job_id=job_id,
            item_id=item.item_id,
            input=item.input,
            status='pending',  # Pending = in DynamoDB, waiting for scheduler
            retry_count=0
        )

        db.create_job_item(job_id, job_item)

    logger.info(f'Created {len(request.items)} job items for job {job_id}')
    return CreateJobResponse(job_id=job_id, seeded_count=len(request.items))


def get_job_status(job_id: str) -> JobStatusResponse | ErrorResponse:
    """
    Get the status of a job.
    """
    if not db.job_exists(job_id):
        return ErrorResponse(status=404, message='Job not found')

    job = db.get_job(job_id)

    return JobStatusResponse(status=job.status, summary=db.get_job_status(job_id))


def get_queue_length() -> int:
    return queue.get_length()


def purge_queue() -> None:
    """
    Purge the queue of all messages.
    """
    queue.purge()

    return None


async def get_job_results(
    job_id: str,
    filter: Literal['all', 'success', 'errors'] = 'all',
    part: int | None = None
) -> dict[str, Any] | ErrorResponse:
    """
    Get job results and manifest data.

    Args:
        job_id: Job ID
        filter: Filter by status (all/success/errors)
        part: Specific manifest part number (0-indexed)

    Returns:
        Job metadata and manifest data
    """
    if not db.job_exists(job_id):
        return ErrorResponse(status=404, message='Job not found')

    job = db.get_job(job_id)

    # job must be completed to have results
    if job.status != 'completed':
        return ErrorResponse(
            status=400,
            message=f'Job is not completed yet (status: {job.status}). Results are only available for completed jobs.'
        )

    try:
        metadata_key = f'{job_id}/job_metadata.json'
        metadata = await storage.download_json(metadata_key)

        # validate that metadata is a dict
        if not isinstance(metadata, dict):
            return ErrorResponse(
                status=500,
                message='Invalid metadata format: expected object'
            )
    except Exception as e:
        return ErrorResponse(
            status=500,
            message=f'Failed to retrieve job metadata: {str(e)}'
        )

    # determine which manifest to fetch based on filter
    manifest_prefix = {
        'all': 'manifests/full',
        'success': 'manifests/success',
        'errors': 'manifests/errors'
    }[filter]

    # get manifest info from metadata
    manifest_info = metadata.get('manifest_info', {})
    part_counts = {
        'all': manifest_info.get('full_parts', 0),
        'success': manifest_info.get('success_parts', 0),
        'errors': manifest_info.get('error_parts', 0)
    }
    total_parts = part_counts[filter]

    # if no specific part requested, return metadata + links to all parts
    if part is None:
        response = {
            'job_id': job_id,
            'metadata': metadata,
            'manifest': {
                'filter': filter,
                'total_parts': total_parts,
                'parts': [
                    {
                        'part': i,
                        'url': f'/jobs/{job_id}/results?filter={filter}&part={i}'
                    }
                    for i in range(total_parts)
                ]
            }
        }
        return response

    # validate part number
    if part < 0 or part >= total_parts:
        return ErrorResponse(
            status=400,
            message=f'Invalid part number {part}. Valid range: 0-{total_parts - 1}'
        )

    # download specific manifest part
    try:
        manifest_key = f'{job_id}/{manifest_prefix}/part-{part}.json'
        manifest_data = await storage.download_json(manifest_key)

        return {
            'job_id': job_id,
            'filter': filter,
            'part': part,
            'total_parts': total_parts,
            'data': manifest_data
        }
    except Exception as e:
        return ErrorResponse(
            status=500,
            message=f'Failed to retrieve manifest part {part}: {str(e)}'
        )


async def download_job_item(
    job_id: str,
    item_id: str,
    artifact: Literal['html', 'data', 'metadata', 'screenshot']
) -> Response | ErrorResponse:
    """
    Download a specific artifact for a job item.

    Args:
        job_id: Job ID
        item_id: Item ID
        artifact: Which artifact to download (html/data/metadata/screenshot)

    Returns:
        File content as HTTP response
    """
    # check if job and item exist
    try:
        job_item = db.get_job_item(job_id, item_id)
    except Exception:
        return ErrorResponse(status=404, message='Job item not found')

    # item must be completed successfully
    if job_item.status != 'success':
        return ErrorResponse(
            status=400,
            message=f'Item status is {job_item.status}. Only successful items can be downloaded.'
        )

    # download the requested artifact from S3
    try:
        if artifact == 'html':
            content = await storage.get_html(job_id, item_id)
            return Response(content=content, media_type='text/html')

        elif artifact == 'data':
            data = await storage.get_data(job_id, item_id)
            return Response(content=json.dumps(data, indent=2), media_type='application/json')

        elif artifact == 'metadata':
            metadata = await storage.get_metadata(job_id, item_id)
            return Response(content=metadata.model_dump_json(indent=2), media_type='application/json')

        elif artifact == 'screenshot':
            # Screenshot is stored as PNG
            key = f"{job_id}/items/{item_id}/screenshot.png"
            content_bytes = await storage.download_bytes(key)
            return Response(content=content_bytes, media_type='image/png')

    except Exception as e:
        return ErrorResponse(
            status=404,
            message=f'Artifact not found: {str(e)}'
        )
