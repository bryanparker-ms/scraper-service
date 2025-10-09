from contextlib import asynccontextmanager
from fastapi import FastAPI, Response, Query
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


@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan - start/stop background tasks"""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Startup: Start scheduler as background task
    logger.info("Starting scheduler background task")
    scheduler_task = asyncio.create_task(scheduler.start())

    yield

    # Shutdown: Stop scheduler gracefully
    logger.info("Stopping scheduler background task")
    await scheduler.stop()
    scheduler_task.cancel()
    try:
        await scheduler_task
    except asyncio.CancelledError:
        pass
    logger.info("Scheduler stopped")


# Create FastAPI app with lifespan
app = FastAPI(title='Web Scraper Controller', lifespan=lifespan)


@app.get('/jobs')
def get_jobs_route() -> GetJobsResponse | ErrorResponse:
    return get_latest_jobs()


@app.post('/jobs')
def create_job_route(request: CreateJobRequest) -> CreateJobResponse | ErrorResponse:
    return create_job(request)


@app.get('/jobs/{job_id}/status')
def get_job_status_route(job_id: str) -> JobStatusResponse | ErrorResponse:
    return get_job_status(job_id)


@app.get('/jobs/queue/length')
def get_queue_length_route() -> int:
    return get_queue_length()


@app.post('/jobs/queue/purge')
def purge_queue_route() -> Response:
    purge_queue()

    return Response(status_code=200)


@app.get('/jobs/{job_id}/results')
async def get_job_results_route(
    job_id: str,
    filter: Literal['all', 'success', 'errors'] | None = Query(None, description="Filter results by status"),
    part: int | None = Query(None, description="Specific manifest part to retrieve (0-indexed)")
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
    artifact: Literal['html', 'data', 'metadata', 'screenshot'] = Query('html', description="Which artifact to download")
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
