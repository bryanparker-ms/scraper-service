from fastapi import FastAPI

from src.controller.models import CreateJobRequest, CreateJobResponse, ErrorResponse, GetJobsResponse, JobStatusResponse
from src.controller.service import get_job_status, get_latest_jobs, create_job


app = FastAPI()


@app.get('/jobs')
def get_jobs_route() -> GetJobsResponse | ErrorResponse:
    return get_latest_jobs()


@app.post('/jobs')
def create_job_route(request: CreateJobRequest) -> CreateJobResponse | ErrorResponse:
    return create_job(request)


@app.get('/jobs/{job_id}/status')
def get_job_status_route(job_id: str) -> JobStatusResponse | ErrorResponse:
    return get_job_status(job_id)
