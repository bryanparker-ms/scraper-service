# Web Scraper Worker Development Plan

*Last Updated: 2025-01-07*

This document outlines the complete development plan for building out the web scraper worker system, organized in incremental chunks for verification and testing.

## 🎯 Overall Goals

Build a robust, scalable web scraping worker system that can:
- Process tens of thousands to millions of job items per scrape job
- Handle three types of scrapers: HTTP-based, Playwright-based, and generic action-based
- Provide comprehensive error handling, retries, and circuit breaking
- Support rate limiting, proxy rotation, and anti-detection measures
- Store results reliably in S3 with proper manifest generation
- Scale horizontally with ECS auto-scaling

## ✅ Phase 1: Foundation Infrastructure (COMPLETED)

### 1.1 Basic Worker Loop ✅
- [x] Create worker main loop structure in `src/worker/main.py`
- [x] Implement SQS message dequeue/process/delete cycle
- [x] Add proper error handling and graceful shutdown (SIGTERM/SIGINT)
- [x] Fix SQS message field name mismatch (`'inputs'` → `'input'`)
- [x] Test worker with existing SQS setup

### 1.2 Core Scraper Framework ✅
- [x] Define `BaseScraper` Protocol interface
- [x] Create simple `ExampleScraper` for testing
- [x] Integrate scraper selection logic into worker loop
- [x] Update worker to call scrapers instead of just logging
- [x] Test scraper integration end-to-end

## ✅ Phase 2: HTTP Scraper Infrastructure (COMPLETED)

### 2.1 BaseHttpScraper Implementation ✅
- [x] Create `BaseHttpScraper` ABC with httpx integration
- [x] Implement comprehensive error classification (retryable vs non-retryable)
- [x] Add retry logic with exponential backoff (1s → 2s → 4s → 8s, max 60s)
- [x] Add HTTP client management with proxy support
- [x] Implement proper resource cleanup with async context managers
- [x] Add input validation framework

### 2.2 Error Handling System ✅
- [x] Create `ScraperError` exception with error type classification
- [x] Map HTTP status codes to error types (5xx→retryable, 4xx→non-retryable, etc.)
- [x] Implement smart retry logic that stops immediately for non-retryable errors
- [x] Add structured logging throughout scraper lifecycle

### 2.3 Example HTTP Scraper ✅
- [x] Create `ExampleHttpScraper` making real HTTP requests to httpbin.org
- [x] Demonstrate input validation, error handling, and data extraction
- [x] Update worker scraper selection to use HTTP scraper based on inputs
- [x] Test HTTP scraper with real web requests

## ✅ Phase 3: Result Storage & Status Management (COMPLETED)

### 3.1 S3 Result Storage ✅
- [x] Create `ResultStorage` Protocol interface and S3 implementation
- [x] Store scraper results (HTML, data, screenshots) in S3 with compression
- [x] Generate incremental manifest files for easy result discovery
- [x] Implement proper S3 key naming conventions (`{job_id}/items/{item_id}/`)
- [x] Add result storage integration to worker loop
- [x] Create `LocalFilesystemStorage` for local development/testing
- [x] Add configuration to switch between S3 and local storage

**Storage Structure**:
```
{job_id}/items/{item_id}/html.html           # Raw HTML (gzipped if >1KB)
{job_id}/items/{item_id}/data.json           # Extracted data
{job_id}/items/{item_id}/metadata.json       # Scrape metadata
{job_id}/items/{item_id}/screenshot.png      # Optional screenshot
{job_id}/manifests/items/{item_id}.json      # Per-item manifest
```

### 3.2 Job Status Management ✅
- [x] Update job item status in DynamoDB after scraping (success/error)
- [x] Track retry counts, error details, and attempt timestamps
- [x] Store storage keys (S3 references) in DynamoDB
- [x] Implement proper error classification and retry logic in worker
- [x] Handle both retryable and non-retryable errors appropriately
- [x] Add comprehensive logging for debugging

## ✅ Phase 4: Scraper Registry & Selection (COMPLETED)

### 4.1 Scraper Registry System ✅
- [x] Create `ScraperRegistry` with decorator-based registration
- [x] Implement `@registry.register()` decorator for clean scraper registration
- [x] Add scraper metadata tracking (id, name, version, description)
- [x] Support explicit scraper selection via `scraper_id` in Job model
- [x] Update worker to use registry for scraper lookup
- [x] Add fallback to default scraper when none specified

### 4.2 Scraper Organization ✅
- [x] Create `src/scrapers/` directory for all custom scrapers
- [x] Move example scrapers to `src/scrapers/examples.py`
- [x] Set up scraper package imports for auto-registration
- [x] Create scraper template and documentation ([SCRAPER_TEMPLATE.md](SCRAPER_TEMPLATE.md))
- [x] Add README in scrapers directory with usage instructions
- [x] Fix type annotations for `ScraperError` error types

**Current Registered Scrapers**:
- `example-mock`: Simple mock scraper for testing
- `example-http`: HTTP scraper using httpbin.org for testing

## ✅ Phase 5: Real-World HTTP Scrapers (COMPLETED)

### 5.1 Property Tax Scraper - Maricopa County, AZ ✅
- [x] Created production scraper for Maricopa County Treasurer site
- [x] Implemented site-specific input validation and data extraction
- [x] Handled multi-step navigation (parcel lookup → tax statement)
- [x] Added BeautifulSoup parsing with helper utilities
- [x] Tested with real property data - working end-to-end!

### 5.2 Scraper Utilities & Validation ✅
- [x] Added HTML parsing utilities (`parse_html`, `select_one`, `extract_text`)
- [x] Created validation utilities (`assert_html_contains`, `assert_fields_present`)
- [x] Implemented hybrid validation (inline + post-scrape `validate_result` hook)
- [x] Simplified scraper code by pushing common patterns to base class
- [x] Added sanity checks to catch silent failures (e.g., error pages)

**Current Registered Scrapers**:
- `example-mock`: Simple mock scraper for testing
- `example-http`: HTTP scraper using httpbin.org for testing
- `maricopa-az`: Production scraper for Maricopa County, AZ property tax data ✅

## ✅ Phase 6: Execution Policy Integration (COMPLETED)

### 6.1 Proxy Configuration ✅
- [x] Read `ExecutionPolicy` from Job model in worker
- [x] Map `ProxyPolicy` to BrightData proxy URLs (datacenter/residential/web-unlocker)
- [x] Pass proxy configuration to scrapers via ExecutionPolicy
- [x] Implement geo-targeting for residential proxies (state/city)
- [x] Handle proxy type restrictions (datacenter=country only, residential=state/city)
- [x] Add validation and warnings for unsupported geo-targeting
- [x] Tested with datacenter and residential proxy types

### 6.2 Timeouts & Retries ✅
- [x] Apply timeout policies from `TimeoutPolicy` (connect + request timeouts)
- [x] Add retry policy integration (max retries, backoff strategy, backoff factor)
- [x] Implement configurable backoff strategies (linear/exponential)
- [x] Pass ExecutionPolicy through: Job → Worker → Scraper → httpx client

### 6.3 Rate Limiting ✅
- [x] Analyzed 7 different rate limiting approaches
- [x] Chose "Scheduler Service" pattern - controller handles rate-limited queueing
- [x] Created `JobScheduler` with background task in controller
- [x] Implemented `TokenBucket` for in-memory rate limiting
- [x] Added per-job rate limiters reading from `ThrottlingPolicy`
- [x] Implemented batching (up to 10 items per cycle for SQS efficiency)
- [x] Fixed race condition by updating status immediately after dequeuing
- [x] Optimized poll interval to 1 second (configurable)
- [x] Updated status flow: pending → queued → in_progress

### 6.4 Circuit Breaker ✅
- [x] Created `CircuitBreaker` class with three states (closed/open/half_open)
- [x] Implemented failure detection (consecutive failures >= 20 OR failure rate >= 50%)
- [x] Added `get_job_failure_metrics()` to query success/error counts from DynamoDB
- [x] Integrated circuit breaker into scheduler loop
- [x] Configured circuit breaker from `CircuitBreakerPolicy` in ExecutionPolicy
- [x] Pause job (status='paused') when circuit breaker trips
- [x] Implemented auto-recovery after cooldown period (5 min default)
- [x] Added half_open state to test recovery before fully closing circuit

## ✅ Phase 7: Manifest Aggregation & Job Finalization (COMPLETED)

### 7.1 Manifest Generation ✅
- [x] Detect when job is complete (all items processed)
- [x] Implement streaming architecture for large jobs (batch size: 1000 items)
- [x] Generate chunked manifests to handle millions of items without memory issues
- [x] Create `manifests/full/part-{N}.json` with all items (chunked)
- [x] Create `manifests/success/part-{N}.json` with successful items only (chunked)
- [x] Create `manifests/errors/part-{N}.json` with failed items only (chunked)
- [x] Create `job_metadata.json` with summary statistics and part counts
- [x] Integrate manifest generation into scheduler (auto-triggers on completion)
- [x] Update job status to 'completed' after manifest generation
- [x] Calculate job duration from first/last attempt timestamps

### 7.2 Result Access API ✅
- [x] Add endpoint: `GET /jobs/{job_id}/results` to download manifest
- [x] Add endpoint: `GET /jobs/{job_id}/items/{item_id}/download` for individual artifact download
- [x] Support filtering results by status (success/error/all)
- [x] Add pagination for large result sets (via part parameter)
- [x] Support downloading HTML, data, metadata, and screenshot artifacts
- [x] Add `download_json()` and `download_bytes()` public methods to storage interface
- [x] Add type validation for downloaded JSON (dict vs list)
- [x] Return metadata + links to all parts when no part specified
- [x] Validate job is completed before allowing result access

## 🎭 Phase 8: Playwright Browser Automation

### 8.1 BasePlaywrightScraper Framework
- [ ] Create `BasePlaywrightScraper` ABC similar to HTTP version
- [ ] Implement browser lifecycle management
- [ ] Add screenshot capture capabilities
- [ ] Handle browser crashes and timeouts
- [ ] Implement stealth measures and anti-detection

### 8.2 Browser-Specific Features
- [ ] JavaScript execution and waiting for dynamic content
- [ ] Complex user interaction simulation (clicks, forms, navigation)
- [ ] Cookie and session persistence across pages
- [ ] File download handling
- [ ] Mobile browser emulation for responsive sites

## 🎬 Phase 9: Generic Action Scrapers

### 9.1 Action-Based Framework
- [ ] Create `BaseActionScraper` for JSON action execution
- [ ] Implement action parser for Chrome extension recordings
- [ ] Add action execution engine (clicks, forms, navigation, etc.)
- [ ] Handle dynamic selectors and element waiting
- [ ] Add action sequence validation and error recovery

### 9.2 Action Types Implementation
- [ ] Navigation actions (goto, back, forward, reload)
- [ ] Interaction actions (click, type, select, upload)
- [ ] Wait actions (element, timeout, network idle)
- [ ] Extraction actions (text, attributes, screenshots)
- [ ] Conditional actions and control flow

## 🚀 Phase 10: Production Optimization

### 10.1 Performance Optimization
- [ ] Implement batch processing for higher throughput
- [ ] Add connection pooling and HTTP/2 support
- [ ] Optimize memory usage and garbage collection
- [ ] Add worker health metrics and auto-scaling triggers
- [ ] Implement result caching for duplicate requests

### 10.2 Monitoring & Observability
- [ ] Add comprehensive metrics collection
- [ ] Implement distributed tracing
- [ ] Add custom CloudWatch dashboards
- [ ] Set up alerting for failures and performance issues
- [ ] Add worker debugging and diagnostic tools

## 🧪 Phase 11: Testing & Validation

### 11.1 Automated Testing
- [ ] Unit tests for all scraper base classes
- [ ] Integration tests with mock websites
- [ ] End-to-end testing with real sites
- [ ] Load testing with high-volume job processing
- [ ] Chaos testing for failure scenarios

### 11.2 Quality Assurance
- [ ] Code review guidelines for new scrapers
- [ ] Scraper certification process
- [ ] Performance benchmarking suite
- [ ] Security review for credential handling
- [ ] Compliance validation for rate limiting

## 🛠️ Phase 12: Developer Experience

### 12.1 CLI Tools
- [ ] Create CLI for job management (`cli.py`)
- [ ] Commands: `create-job`, `list-jobs`, `job-status`, `cancel-job`
- [ ] Local job execution (bypass queue, run items directly)
- [ ] Import job items from CSV/JSON

### 12.2 Documentation
- [ ] API documentation with examples
- [ ] Scraper development guide
- [ ] Deployment guide for ECS
- [ ] Troubleshooting guide

---

## 📍 Current Status: Phase 7 Complete ✅

**Last Completed**: Manifest Aggregation & Result Access API (Phase 7) ✅
**Currently**: Production-ready system with complete end-to-end functionality from job creation to result retrieval
**Next Up**: Playwright Browser Automation (Phase 8) or CLI Tools (Phase 12)

**Working Features**:
- ✅ Complete worker loop with SQS integration
- ✅ HTTP scraper framework with retry/error handling
- ✅ S3 result storage with compression and manifests
- ✅ DynamoDB status tracking with three-state flow (pending → queued → in_progress)
- ✅ Scraper registry with decorator syntax
- ✅ Local storage option for development
- ✅ Production Maricopa County scraper working end-to-end
- ✅ ExecutionPolicy integration (proxy, timeout, retry, rate limiting, circuit breaker)
- ✅ BrightData proxy support (datacenter/residential/web-unlocker)
- ✅ Geo-targeting for residential proxies
- ✅ HTML parsing and validation utilities
- ✅ Hybrid validation framework (inline + post-scrape)
- ✅ Scheduler service with token bucket rate limiting
- ✅ Circuit breaker with auto-pause on failure patterns
- ✅ Job status management (paused when circuit trips)
- ✅ **NEW**: Chunked manifest generation for large jobs (handles millions of items)
- ✅ **NEW**: Result access API with filtering and pagination
- ✅ **NEW**: Individual artifact download (HTML, data, metadata, screenshots)

**Testing**:
```bash
# Create a job with explicit scraper
curl -X POST http://localhost:8000/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "job_id": "test-001",
    "scraper_id": "example-http",
    "items": [
      {"item_id": "item-1", "input": {"test_param": "hello"}},
      {"item_id": "item-2", "input": {"test_param": "world"}}
    ]
  }'

# Start worker
python -m src.worker.main

# Use local storage for testing
export USE_LOCAL_STORAGE=true
python -m src.worker.main
```

**System Architecture**:
```
Controller (FastAPI)
    ├─ REST API (create jobs, check status)
    └─ Background Scheduler (rate limiting + circuit breaker)
          ↓
       DynamoDB (pending items)
          ↓
       SQS Queue (rate-limited queueing)
          ↓
    Worker (ECS Tasks)
          ↓
       S3 Storage (results)
```

**Rate Limiting & Circuit Breaker Flow**:
1. Controller stores job items with status='pending'
2. Scheduler polls DynamoDB for pending items
3. Token bucket rate limiter controls queueing speed
4. Circuit breaker monitors failure metrics from DynamoDB
5. If circuit trips (20 consecutive errors OR 50%+ failure rate), job is paused
6. After cooldown (5 min), circuit enters half_open to test recovery
7. If recovery succeeds, circuit closes and job resumes

**Phase 7 Completion - Manifest Generation & Result Access**:
The system now has complete end-to-end functionality:
- Jobs are created → items queued with rate limiting → workers process → manifests generated → results accessible via API
- Handles jobs of any size (millions of items) with streaming architecture
- Complete observability from creation to completion