# Web Scraper Development Context

*Last Updated: 2025-01-14*

## Current Development Status

### Recent Work: Concurrency Control & Circuit Breaker Refinements ✅

We've completed significant refinements to the concurrency control and circuit breaker systems:

**Concurrency Control (Throttling)**:
- Replaced token bucket rate limiting with `max_concurrent_workers` concurrency control
- Scheduler now tracks in-flight items (queued + in_progress) per job
- Only queues new items when `in_flight < max_concurrent_workers`
- This prevents overwhelming target websites by limiting concurrent connections
- Fixed critical bug where scheduler was reading `status_summary.running` instead of `in_progress`

**Circuit Breaker Simplification**:
- Simplified from 3 states (closed/open/half_open) to 2 states (closed/open)
- Removed auto-recovery logic - circuit stays open permanently once tripped
- Jobs are marked as `failed` (not `paused`) when circuit breaker trips
- Batch size limiting prevents overshooting `min_requests` threshold
- Mid-batch circuit breaker checks catch failures during queueing

**Status Model Cleanup**:
- Removed `retrying` status entirely - scrapers handle retries internally
- Renamed `error` to `failed` to match `JobStatus` naming convention
- Simplified flow: `pending` → `queued` → `in_progress` → `success`/`failed`
- Items go directly to `failed` status after retry exhaustion

### What We've Accomplished

#### Phase 1-5: Foundation ✅
- Complete worker loop with SQS integration
- HTTP scraper framework with retry/error handling
- S3 result storage with compression and manifests
- Scraper registry with decorator syntax
- Production Maricopa County scraper working end-to-end

#### Phase 6: Execution Policy Integration ✅
- Proxy configuration (BrightData datacenter/residential/web-unlocker)
- Timeout and retry policies
- **Concurrency control** (max_concurrent_workers) - limits simultaneous workers per job
- **Circuit breaker** - detects failure patterns and fails jobs automatically

#### Phase 7: Manifest Aggregation & Job Finalization ✅
- Chunked manifest generation for large jobs (millions of items)
- Result access API with filtering and pagination
- Individual artifact download (HTML, data, metadata, screenshots)

### Key Design Decisions Made

1. **Concurrency Control over Rate Limiting**:
   - Chose to limit concurrent workers per job instead of requests per second
   - More intuitive: "max 3 workers on this job" vs "max 2 requests/second"
   - Prevents overwhelming target sites with concurrent connections
   - Scheduler-based approach gives centralized control

2. **Simplified Circuit Breaker**:
   - No auto-recovery - jobs stay failed once circuit trips
   - Clearer failure semantics - jobs either succeed or fail permanently
   - Removed complexity of half-open state and cooldown periods

3. **Status Model Simplification**:
   - Removed `retrying` status - scrapers retry internally, not via queue
   - Single terminal failure state (`failed`) instead of multiple (`error`, `retrying`, `max_retries`)
   - Matches conceptual model: items are either in-progress or done (success/failed)

### Current File Structure
```
src/
├── worker/
│   ├── main.py           # Worker loop with scraper integration
│   ├── scraper.py        # BaseHttpScraper + error handling
│   ├── registry.py       # Scraper registry with decorators
│   └── models.py         # ScrapeResult model
├── scrapers/
│   ├── examples.py       # Example scrapers (mock, http, fail)
│   └── az/
│       └── maricopa.py   # Production Maricopa County scraper
├── shared/
│   ├── models.py         # JobItem, Job, ExecutionPolicy models
│   ├── queue.py          # SQS integration
│   ├── db.py             # DynamoDB interface
│   ├── settings.py       # Environment configuration
│   ├── interfaces.py     # Protocol definitions
│   └── storage.py        # S3 + local filesystem storage
└── controller/
    ├── main.py           # FastAPI REST endpoints
    ├── service.py        # Job creation/status logic
    ├── scheduler.py      # Background scheduler with concurrency + circuit breaker
    └── circuit_breaker.py # Circuit breaker implementation
```

## What We're Working On Right Now

### Current State
- **Phase 7 Complete**: Full end-to-end system with manifest generation
- **System is production-ready** for HTTP-based scraping jobs
- Concurrency control and circuit breaker are working correctly
- Ready for real-world usage or next major features

### Immediate Next Steps (In Order of Priority)

1. **Testing & Validation**: Comprehensive testing of circuit breaker with real failure scenarios
2. **Playwright Framework**: Build BasePlaywrightScraper for JavaScript-heavy sites
3. **CLI Tools**: Build command-line interface for job management
4. **Monitoring**: Add CloudWatch metrics and alerting

## Three Types of Scrapers (Architecture)

1. **HTTP-Based Scrapers** ✅ (Complete)
   - Uses httpx for HTTP requests
   - BaseHttpScraper provides retry/error handling/validation
   - For sites that work with simple HTTP requests
   - Example: Maricopa County property tax scraper

2. **Playwright-Based Scrapers** (Next Major Feature)
   - BasePlaywrightScraper for browser automation
   - Handle JavaScript, complex interactions, screenshots
   - For sites requiring full browser rendering

3. **Generic Action Scrapers** (Future)
   - Execute JSON action lists from Chrome extension recordings
   - Most flexible but complex to implement
   - For recorded user workflows

## Testing the Current Implementation

### Test Circuit Breaker
```bash
# Create job with failing scraper to test circuit breaker
curl -X POST http://localhost:8000/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "job_id": "circuit-test",
    "scraper_id": "example-fail",
    "execution_policy": {
      "throttling": {"max_concurrent_workers": 3},
      "circuit_breaker": {"min_requests": 5, "failure_threshold_percentage": 0.8},
      "retries": {"max_retries": 1}
    },
    "items": [
      {"item_id": "item-01", "input": {"test": "value"}},
      {"item_id": "item-02", "input": {"test": "value"}},
      ...10 items total...
    ]
  }'

# Expected: ~5 items processed, then job marked as 'failed'
# Circuit trips when 80%+ of min_requests (5) have failed
```

### Test Concurrency Control
```bash
# Create job with max 3 concurrent workers
curl -X POST http://localhost:8000/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "job_id": "concurrency-test",
    "scraper_id": "example-http",
    "execution_policy": {
      "throttling": {"max_concurrent_workers": 3}
    },
    "items": [...50 items...]
  }'

# Expected: At most 3 items in-flight (queued + in_progress) at any time
# Check logs to see scheduler respecting concurrency limit
```

## Key Technical Notes

- **Concurrency Control**: Scheduler polls DynamoDB to count in-flight items before queueing
- **Circuit Breaker**: Checks failure metrics from DynamoDB before each batch
- **Batch Size Limiting**: Prevents overshooting `min_requests` threshold
- **Status Model**: Simple 5-state flow (pending/queued/in_progress/success/failed)
- **Worker Retries**: Handled internally by scraper, not by re-queueing to SQS

## Context for Future Sessions

When resuming development:
1. **System is feature-complete** for Phase 7 - manifest generation and results API
2. **Recent work**: Concurrency control and circuit breaker are fully functional and tested
3. **Status model is simplified** - removed `retrying`, renamed `error` to `failed`
4. **Next major chunk**: Playwright framework (Phase 8) or CLI tools (Phase 12)
5. **All patterns established** - ready for Playwright or additional scrapers

The codebase is at a **major milestone** - production-ready system with complete end-to-end functionality from job creation to result retrieval.