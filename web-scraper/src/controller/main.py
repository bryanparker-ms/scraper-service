from fastapi import FastAPI

from controller.models import ErrorResponse, GetJobsResponse
from controller.service import get_latest_jobs
# from shared.settings import Settings

# from src.errors import JobNotFound
# from src.models.general import ErrorResponse
# from src.models.job import CreateJobRequest, CreateJobResponse, FinalizeResponse, JobStatus, PauseResumeResponse
# from src.service.job import create_job, finalize_job, get_job_status, get_latest_jobs, pause_resume_job


app = FastAPI()


@app.get('/jobs')
def get_jobs_route() -> GetJobsResponse | ErrorResponse:
    return get_latest_jobs()
