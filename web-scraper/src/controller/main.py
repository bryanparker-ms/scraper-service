from contextlib import asynccontextmanager
from fastapi import FastAPI, Response, Query, Security, HTTPException, status
from fastapi.security import APIKeyHeader
from typing import Any, Literal
import asyncio
import logging

from src.controller.models import CreateJobRequest, CreateJobResponse, ErrorResponse, GetJobsResponse, JobStatusResponse
from src.controller.service import get_job_status, get_latest_jobs, create_job, get_queue_length, purge_queue, get_job_results, download_job_item
from src.controller.scheduler import JobScheduler
from src.shared.settings import Settings

logger = logging.getLogger(__name__)

# Initialize scheduler
settings = Settings()
scheduler = JobScheduler(settings)

# API Key authentication
api_key_header = APIKeyHeader(name='X-API-Key', auto_error=False)

def verify_api_key(api_key: str = Security(api_key_header)) -> str:
    """Verify API key from request header"""
    expected_key = settings.api_key

    # If no API key is configured, allow all requests (for local development)
    if not expected_key:
        logger.warning('No API key configured - authentication is disabled')
        return 'unauthenticated'

    if not api_key or api_key != expected_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail='Invalid or missing API key'
        )

    return api_key


@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan - start/stop background tasks"""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Set our application loggers to INFO
    logging.getLogger('src.controller').setLevel(logging.INFO)
    logging.getLogger('src.worker').setLevel(logging.INFO)
    logging.getLogger('src.shared').setLevel(logging.INFO)

    # Suppress noisy third-party loggers
    logging.getLogger('botocore').setLevel(logging.WARNING)
    logging.getLogger('boto3').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)

    # Startup: Start scheduler as background task
    logger.info('Starting scheduler background task')
    scheduler_task = asyncio.create_task(scheduler.start())

    yield

    # Shutdown: Stop scheduler gracefully
    logger.info('Stopping scheduler background task')
    await scheduler.stop()
    scheduler_task.cancel()
    try:
        await scheduler_task
    except asyncio.CancelledError:
        pass
    logger.info('Scheduler stopped')


# Create FastAPI app with lifespan
app = FastAPI(title='Web Scraper Controller', lifespan=lifespan)


@app.get('/jobs')
def get_jobs_route(api_key: str = Security(verify_api_key)) -> GetJobsResponse | ErrorResponse:
    return get_latest_jobs()


@app.post('/jobs')
def create_job_route(request: CreateJobRequest, api_key: str = Security(verify_api_key)) -> CreateJobResponse | ErrorResponse:
    return create_job(request)


@app.get('/jobs/{job_id}/status')
def get_job_status_route(job_id: str, api_key: str = Security(verify_api_key)) -> JobStatusResponse | ErrorResponse:
    return get_job_status(job_id)


@app.get('/jobs/queue/length')
def get_queue_length_route(api_key: str = Security(verify_api_key)) -> int:
    return get_queue_length()


@app.post('/jobs/queue/purge')
def purge_queue_route(api_key: str = Security(verify_api_key)) -> Response:
    purge_queue()

    return Response(status_code=200)


@app.get('/jobs/{job_id}/results')
async def get_job_results_route(
    job_id: str,
    filter: Literal['all', 'success', 'errors'] | None = Query(None, description="Filter results by status"),
    part: int | None = Query(None, description='Specific manifest part to retrieve (0-indexed)'),
    api_key: str = Security(verify_api_key)
) -> dict[str, Any] | ErrorResponse:
    """
    Get job results/manifest.

    Returns metadata and manifest data. For large jobs with multiple parts,
    use the 'part' parameter to retrieve specific chunks.
    """
    return await get_job_results(job_id, filter=filter, part=part)


@app.get('/jobs/{job_id}/items/{item_id}/download')
async def download_job_item_route(
    job_id: str,
    item_id: str,
    artifact: Literal['html', 'data', 'metadata', 'screenshot'] = Query('html', description='Which artifact to download'),
    api_key: str = Security(verify_api_key)
):
    """
    Download a specific artifact for a job item.

    Artifacts:
    - html: The scraped HTML content
    - data: The extracted JSON data
    - metadata: Item metadata (timing, storage keys, etc.)
    - screenshot: Screenshot (if captured)
    """
    return await download_job_item(job_id, item_id, artifact)
