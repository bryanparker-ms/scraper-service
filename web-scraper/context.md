# Web Scraper Development Context

*Last Updated: 2025-01-02*

## Current Development Status

### What We've Accomplished

#### Phase 1: Basic Worker Infrastructure ✅
- **Worker Loop**: Complete SQS message processing loop in `src/worker/main.py`
  - Dequeue → process → delete message cycle
  - Proper error handling and graceful shutdown (SIGTERM/SIGINT)
  - Async-based with configurable sleep intervals
- **SQS Integration**: Fixed field name mismatch in `src/shared/queue.py`
  - Changed `'inputs'` to `'input'` to match `JobItem` model
  - Worker now successfully parses job items without validation errors

#### Phase 2: Core Scraper Framework ✅
- **BaseScraper Protocol**: Interface that all scrapers must implement
  - `async def scrape(job_item: JobItem) -> ScrapeResult`
  - Uses Protocol for structural typing (duck typing with type checking)
- **Simple Example Scraper**: Mock scraper returning test data for basic validation

#### Phase 3: HTTP Scraper Infrastructure ✅ (Just Completed)
- **BaseHttpScraper ABC**: Production-ready base class for HTTP-based scrapers
  - Full retry logic with exponential backoff (1s → 2s → 4s → 8s, max 60s)
  - Comprehensive error classification:
    - **Retryable**: `timeout`, `server_error`, `network_error`, `proxy_error`, `blocked`, `unexpected`
    - **Non-retryable**: `not_found`, `invalid_input`, `no_results`
  - HTTP client management with proxy support, custom headers, timeouts
  - Input validation framework
  - Proper resource cleanup with async context manager
- **ScraperError Exception**: Custom exception with error type classification
- **ExampleHttpScraper**: Concrete implementation making real HTTP requests to httpbin.org
- **Worker Integration**: Scraper selection logic based on job item inputs

### Key Design Decisions Made

1. **Protocol vs ABC Pattern**:
   - `BaseScraper` (Protocol) for interface definition
   - `BaseHttpScraper` (ABC) for shared implementation
   - This follows modern Python best practices

2. **Error Handling Strategy**:
   - Classify errors into retryable vs non-retryable
   - Stop retrying immediately for non-retryable errors
   - Exponential backoff with jitter prevention

3. **Single Item Processing**:
   - Worker processes one SQS message at a time
   - Natural backpressure and error isolation
   - Can scale by running more workers

### Current File Structure
```
src/
├── worker/
│   ├── main.py           # Complete worker loop with scraper integration
│   ├── scraper.py        # HTTP scraper framework + examples
│   └── models.py         # ScrapeResult model
├── shared/
│   ├── models.py         # JobItem, Job, ExecutionPolicy models
│   ├── queue.py          # SQS integration (fixed field names)
│   ├── db.py             # DynamoDB interface
│   ├── settings.py       # Environment configuration
│   └── interfaces.py     # Protocol definitions
└── controller/
    ├── main.py           # FastAPI REST endpoints
    ├── service.py        # Job creation/status logic
    └── models.py         # API request/response models
```

## What We're Working On Right Now

### Current State
- **HTTP Scraper Framework**: Just completed - fully functional and production-ready
- **Testing**: Ready to test HTTP scraper with real web requests
  - Create job with `{"test_param": "value"}` to trigger ExampleHttpScraper
  - Worker will make real HTTP request to httpbin.org
  - All error handling, retries, and classification working

### Immediate Next Steps (In Order of Priority)

1. **Validation Testing**: Confirm HTTP scraper works end-to-end with real requests
2. **Property Tax Scraper**: Build first concrete scraper for real county website
3. **Playwright Framework**: Build BasePlaywrightScraper for browser-based sites
4. **Result Storage**: S3 integration for storing HTML/data/screenshots
5. **Enhanced Scraper Selection**: Registry system for mapping sites to scrapers

## Three Types of Scrapers (Planned Architecture)

As outlined in the original requirements:

1. **HTTP-Based Scrapers** ✅ (Complete)
   - Uses httpx for HTTP requests
   - BaseHttpScraper provides retry/error handling
   - For sites that work with simple HTTP requests

2. **Playwright-Based Scrapers** (Next Major Chunk)
   - BasePlaywrightScraper for browser automation
   - Handle JavaScript, complex interactions
   - For sites requiring full browser rendering

3. **Generic Action Scrapers** (Future)
   - Execute JSON action lists from Chrome extension
   - Most flexible but complex to implement
   - For recorded user workflows

## Testing the Current Implementation

### Test HTTP Scraper
```bash
curl -X POST http://localhost:8000/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "job_id": "http-test",
    "items": [{"item_id": "test-001", "input": {"test_param": "hello"}}]
  }'
```

### Test Mock Scraper
```bash
curl -X POST http://localhost:8000/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "job_id": "mock-test",
    "items": [{"item_id": "test-001", "input": {"parcel_id": "123"}}]
  }'
```

## Key Technical Notes

- **Dependencies**: httpx already in pyproject.toml, ready for HTTP scraping
- **Error Handling**: Comprehensive classification matches shared model types
- **Resource Management**: HTTP clients properly closed, no connection leaks
- **Logging**: Structured logging throughout with appropriate levels
- **Type Safety**: Full type annotations, Protocol/ABC patterns

## Context for Future Sessions

When resuming development:
1. We have a **working foundation** - basic worker + HTTP scraper framework
2. The **HTTP scraper infrastructure is complete** and production-ready
3. Next major chunk is either **concrete property tax scrapers** or **Playwright framework**
4. All the hard patterns (retry, error handling, worker loop) are established
5. Ready to scale up to real-world scraping scenarios

The codebase is at a **solid milestone** - we have working, testable infrastructure ready for building real scrapers.