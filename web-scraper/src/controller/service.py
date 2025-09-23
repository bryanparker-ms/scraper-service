from controller.models import GetJobsResponse, JobSummary
from shared.db import DynamoDBDatabaseService
from shared.settings import Settings


settings = Settings()
db = DynamoDBDatabaseService(settings)

def get_latest_jobs() -> GetJobsResponse:
    jobs = db.get_jobs()
    summaries = [JobSummary(
        job_id=job.job_id,
        status=job.status,
        created_at=job.created_at,
        updated_at=job.updated_at,
        total_items=len(job.items)
    ) for job in jobs]

    return GetJobsResponse(jobs=summaries)
