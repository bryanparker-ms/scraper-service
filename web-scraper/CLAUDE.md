## Summary

I am designing and building a robust web scraping service that can manage large "scrape jobs". Within a scrape job, there are N "job items", which are a series of steps that are executed in order to gather information or execute actions on a website. A process will execute each of these job items (on the order of tens of thousands to maybe low millions of job items per scrape job) on different inputs. The service should be generic, but the information I'm trying to scrape is property tax information (e.g., tax bills, property assessments). These bulk scrape jobs are going to usually be at the county level (i.e., US county), and will generally consist of a couple of steps like (as an example): 1) load a URL, 2) submit some form data, 3) click on an item in the search results, 4) maybe a navigation or two, 5) collect data on the page, 6) Done.

A few examples of scrape jobs are:

### Examples

-   collect market values for all properties in a given county (i.e., one job item per property in the county. The job item contains the inputs necessary to execute the scrape—for example, the parcel ID or address)
-   download a PDF of a tax bill for 1,000 properties
-   collect the HTML for a target page for all properties in a given county

## Service Architecture

In general, the way I've viewed this service is some version of the following:

-   a "controller" service that receives a scrape job (i.e., metadata about the scrape job and the list of job item inputs that need to be executed) and persists it and adds the items to the queue. This service will also report out on the status of the scrape job and allow for management of the scrape job. The main interface for the controller is a REST API, but I'd like to have a CLI interface as well if I want to execute a scrape job locally.
-   a "worker" service that reads items from the queue and executes the steps of a scrape job. Based the scrape job, the worker will "decide" which scraper to use and then pass of the inputs to the scraper to execute. It will then persist the results of the scrape job item and update the status of the scrape job (if needed).
-   a "shared" library. Given that the controller and worker services will share a lot of code between them, it probably makes sense to have a shared library that contains the code and interfaces that are shared between the two services.

The service will need to account for the fact that there can be many different scrapers, which should factor into our design. In general, there are three types of scrapers (that I see at this point):

1. Bespoke HTTP based scrapers (e.g., using httpx) for a given site that we can use to "navigate" the site and collect data. These scrapers are targeting specific pages. We may have to make a few hops in order to get there (i.e., we may land on a given page to establish a session, and then simulate a POST request to get to the next page).
2. Bespoke Playwright based scrapers where we can write playwright code to navigate the site and collect information. Playwright might be an implementation detail that's hidden from the scraper, idk
3. A "generic" scraper that executes a list of "actions" (e.g., clicks, submits, etc.) on a page. This is the most generic and flexible type of scraper. I have another service that is a Chrome extension that records user's workflows and then stores those actions in a database as JSON. I'd like to be able execute these actions on a page.

The scrapers that we create will take the shape of one of the above types. There could potentially be hundreds of different scrapers for different sites (e.g., one for each county in a state) that the worker will need to choose from.

## Considerations

I need to account for the fact that some sites have measures in place to throttle/block scrapers. I have access to BrightData that I can use. For some sites, we could get by with just data center proxies (or maybe no proxy at all), while others might require residential proxies, or targeted residential proxies (e.g., for a particular state).

In some cases, I _might_ be able to get by with just making HTTP requests using something like httpx, while other times we might need to use Playwright or another programmatic browser.

When running a job item, any of these individual requests may fail for any number of reasons, including: 1) timeout, 2) no property found, 3) page structure changed, 4) blocked, 5) couldn't get to target location, 6) page under maintenance, 7) maybe others? There are lots of different types of errors—some recoverable and/or retry-able... some maybe not.

When designing this service, we need to carefully account for the management piece of this service. Particularly around things like:

-   tracking which items are done and what's their status (e.g., success, error [and why?], etc.)
-   tracking which items are being retried and how many times (we shouldn't just retry indefinitely, we should cap the retries)
-   tracking which items have errored and why

I kind of think we might need a circuit breaker situation of sorts too where we don't try to naively scrape hundreds of thousands of times if we notice that it's likely not to work. For example, we might notice that we have a string of failures or something, and we can extrapolate that it's likely not going to work for the rest. As a result, a "ciruit" is tripped and we can pause the job to avoid wasting resources.

Next, we need to account for how we store and represent this information. We need some way to store the data/line items that needs to be worked on, the working set of line items, the results, etc. I think I would like to store the results in S3 in some way. For some scrape jobs, I'm thinking that I might like to just store the page HTML and then have a post-processing step that extracts the data we want from the page(s) so that if we decide we want to go back and extract more information, we have the source handy. For other scrape jobs, we can store the collected data.

In order to store/reference the data in S3, we need some kind of manifest or some other way to find/reference the data in S3.

## Technology

For the "controller" service, I'm thinking we can use FastAPI for the REST API portion of this. For the "worker" service, the idea is that these will be ECS tasks that can auto-scale based on the number of job items that need to be worked on. The job data (i.e., job metadata, job items, statuses, etc.) can all be stored in DynamoDB. The job items queue will use SQS. The results will be stored in S3. Projects will be managed by uv. If you generate Python, generate well-typed Python.

While these are the current choices, I'd like to have the services interact with generic interfaces and have the ability to swap out the implementation details as needed.

## Design

Using the above information, design a system to manage the scrape jobs. Begin by listing all assumptions and clarifying requirements. Be exhaustive and consider edge cases, failure modes, and robustness concerns. Do not stop at a minimal implementation — I want you to think through the tradeoffs and outline at least 3 alternatives before converging on a recommendation. Do not optimize for brevity. I want completeness and detailed reasoning.
# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a distributed web scraper system built with Python, FastAPI, and AWS services. It consists of two main components:

- **Controller** (`src/controller/`): FastAPI web service that manages scraping jobs and exposes REST API
- **Worker** (`src/worker/`): Background workers that execute scraping tasks from SQS queue

## Architecture

### Core Components

- **Shared Models** (`src/shared/models.py`): Core data structures including `Job`, `JobItem`, and execution policies
- **Database Service** (`src/shared/db.py`): DynamoDB interface for job persistence
- **Queue Service** (`src/shared/queue.py`): SQS interface for job distribution
- **Settings** (`src/shared/settings.py`): Environment-based configuration management

### Job Flow

1. Jobs are created via POST `/jobs` with items to scrape
2. Job items are stored in DynamoDB and queued in SQS
3. Workers dequeue items and execute scraping via `BaseScraper` protocol
4. Results are stored back to database and optionally S3

## Development Commands

### Running the Application

```bash
# Install dependencies
uv sync

# Start the controller API server
uvicorn src.controller.main:app --reload --port 8000

# Run a worker (in separate terminal)
python -m src.worker.main
```

### Environment Setup

Required environment variables:
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `WEB_SCRAPER_QUEUE_URL`
- `TABLE_NAME` (defaults to 'scrape-state')
- `WEB_SCRAPER_RESULTS_BUCKET` (defaults to 'scrape-results')
- Proxy configuration: `PROXY_URL`, `PROXY_DATACENTER_PASSWORD`, `PROXY_RESIDENTIAL_PASSWORD`, `PROXY_WEB_UNLOCKER_PASSWORD`

## API Endpoints

- `GET /jobs` - List recent jobs
- `POST /jobs` - Create new scraping job
- `GET /jobs/{job_id}/status` - Get job status and item summary

## Key Design Patterns

- **Protocol-based interfaces**: `DatabaseService`, `QueueService`, and `BaseScraper` use Protocol types for loose coupling
- **Execution policies**: Jobs include configurable throttling, proxy, retry, timeout, and circuit breaker policies
- **Error categorization**: Errors are classified as retryable vs non-retryable for intelligent retry logic
- **Batch processing**: Job items are queued in batches of 10 for efficiency

## File Structure

```
src/
├── shared/           # Common models, database, queue, and utilities
├── controller/       # FastAPI web service
└── worker/           # Background scraping workers
```