import uuid
from src.controller.models import CreateJobRequest, CreateJobResponse, ErrorResponse, GetJobsResponse, JobStatusResponse, JobSummary
from src.shared.db import DynamoDBDatabaseService
from src.shared.models import Job, JobItem
from src.shared.queue import SqsQueue
from src.shared.settings import Settings


settings = Settings()
db = DynamoDBDatabaseService(settings)
queue = SqsQueue(settings)

def get_latest_jobs() -> GetJobsResponse:
    jobs = db.get_jobs()

    summaries = [JobSummary(
        job_id=job.job_id,
        status=job.status,
        created_at=job.created_at,
        updated_at=job.updated_at,
        total_items=job.total_items
    ) for job in jobs]

    return GetJobsResponse(jobs=summaries)


def create_job(request: CreateJobRequest) -> CreateJobResponse | ErrorResponse:
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

    # Create job items with status='pending'
    # The scheduler will pick these up and push to SQS at the configured rate
    for item in request.items:
        job_item = JobItem(
            job_id=job_id,
            item_id=item.item_id,
            input=item.input,
            status='pending',  # Pending = in DynamoDB, waiting for scheduler
            retry_count=0
        )

        db.create_job_item(job_id, job_item)

    # No SQS push here! Scheduler handles rate-limited queueing
    return CreateJobResponse(job_id=job_id, seeded_count=len(request.items))


def get_job_status(job_id: str) -> JobStatusResponse | ErrorResponse:
    if not db.job_exists(job_id):
        return ErrorResponse(status=404, message='Job not found')

    job = db.get_job(job_id)

    return JobStatusResponse(status=job.status, summary=db.get_job_status(job_id))


def get_queue_length() -> int:
    return queue.get_length()


def purge_queue() -> None:
    queue.purge()

    return None
