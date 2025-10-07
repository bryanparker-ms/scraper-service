# Web Scraper Worker Development Plan

*Last Updated: 2025-01-02*

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

## 🔄 Phase 3: Real-World HTTP Scrapers (NEXT)

### 3.1 Property Tax Scraper Template
- [ ] Research and select a county website for initial implementation
- [ ] Create concrete HTTP scraper for property tax site
- [ ] Implement site-specific input validation and data extraction
- [ ] Handle common property tax site patterns (search forms, result pages)
- [ ] Add proper error handling for site-specific failures

### 3.2 Advanced HTTP Patterns
- [ ] Session management and cookie handling
- [ ] Form submission and CSRF token handling
- [ ] Multi-step navigation patterns
- [ ] Response parsing with BeautifulSoup or similar
- [ ] PDF download handling for tax documents

## 📋 Phase 4: Result Storage & Status Management

### 4.1 S3 Result Storage
- [ ] Create `ResultStorage` interface and S3 implementation
- [ ] Store scraper results (HTML, data, screenshots) in S3
- [ ] Generate manifest files for easy result discovery
- [ ] Implement proper S3 key naming conventions
- [ ] Add result storage integration to worker loop

### 4.2 Job Status Management
- [ ] Update job item status in DynamoDB after scraping
- [ ] Track retry counts and error details
- [ ] Update job-level progress tracking
- [ ] Implement job completion detection
- [ ] Add status reporting for monitoring

## 🎭 Phase 5: Playwright Browser Automation

### 5.1 BasePlaywrightScraper Framework
- [ ] Create `BasePlaywrightScraper` ABC similar to HTTP version
- [ ] Implement browser lifecycle management
- [ ] Add screenshot capture capabilities
- [ ] Handle browser crashes and timeouts
- [ ] Implement stealth measures and anti-detection

### 5.2 Browser-Specific Features
- [ ] JavaScript execution and waiting for dynamic content
- [ ] Complex user interaction simulation (clicks, forms, navigation)
- [ ] Cookie and session persistence across pages
- [ ] File download handling
- [ ] Mobile browser emulation for responsive sites

## 🏗️ Phase 6: Scraper Registry & Selection

### 6.1 Dynamic Scraper Registry
- [ ] Create `ScraperRegistry` for mapping sites to scrapers
- [ ] Support configuration-driven scraper selection
- [ ] Implement auto-discovery of scraper modules
- [ ] Add scraper validation and health checks
- [ ] Support scraper-specific configuration

### 6.2 Enhanced Selection Logic
- [ ] Map job items to scrapers based on `site_key`, `rate_class`, etc.
- [ ] Support fallback scrapers for failed attempts
- [ ] Add A/B testing support for scraper variants
- [ ] Implement scraper performance tracking

## 🛡️ Phase 7: Robustness Features

### 7.1 Rate Limiting & Throttling
- [ ] Implement worker-level rate limiting with token buckets
- [ ] Add global rate coordination via Redis
- [ ] Support per-site rate limit configuration
- [ ] Implement smart backoff based on site responses
- [ ] Add proxy rotation and rate distribution

### 7.2 Circuit Breaker System
- [ ] Implement job-level circuit breakers
- [ ] Add site-level circuit breakers for systematic failures
- [ ] Create scraper-level circuit breakers for broken implementations
- [ ] Add manual circuit breaker controls via API
- [ ] Implement circuit breaker recovery logic

### 7.3 Enhanced Error Handling
- [ ] Poison message detection and quarantine
- [ ] Dead letter queue implementation
- [ ] Error pattern detection and alerting
- [ ] Automatic retry escalation (HTTP → Browser → Manual)
- [ ] Comprehensive error reporting and analytics

## 🎬 Phase 8: Generic Action Scrapers

### 8.1 Action-Based Framework
- [ ] Create `BaseActionScraper` for JSON action execution
- [ ] Implement action parser for Chrome extension recordings
- [ ] Add action execution engine (clicks, forms, navigation, etc.)
- [ ] Handle dynamic selectors and element waiting
- [ ] Add action sequence validation and error recovery

### 8.2 Action Types Implementation
- [ ] Navigation actions (goto, back, forward, reload)
- [ ] Interaction actions (click, type, select, upload)
- [ ] Wait actions (element, timeout, network idle)
- [ ] Extraction actions (text, attributes, screenshots)
- [ ] Conditional actions and control flow

## 🚀 Phase 9: Production Optimization

### 9.1 Performance Optimization
- [ ] Implement batch processing for higher throughput
- [ ] Add connection pooling and HTTP/2 support
- [ ] Optimize memory usage and garbage collection
- [ ] Add worker health metrics and auto-scaling triggers
- [ ] Implement result caching for duplicate requests

### 9.2 Monitoring & Observability
- [ ] Add comprehensive metrics collection
- [ ] Implement distributed tracing
- [ ] Add custom CloudWatch dashboards
- [ ] Set up alerting for failures and performance issues
- [ ] Add worker debugging and diagnostic tools

## 🧪 Phase 10: Testing & Validation

### 10.1 Automated Testing
- [ ] Unit tests for all scraper base classes
- [ ] Integration tests with mock websites
- [ ] End-to-end testing with real sites
- [ ] Load testing with high-volume job processing
- [ ] Chaos testing for failure scenarios

### 10.2 Quality Assurance
- [ ] Code review guidelines for new scrapers
- [ ] Scraper certification process
- [ ] Performance benchmarking suite
- [ ] Security review for credential handling
- [ ] Compliance validation for rate limiting

---

## 📍 Current Status: Phase 2 Complete

**Last Completed**: HTTP Scraper Infrastructure (Phase 2.3) ✅
**Currently Testing**: HTTP scraper with real web requests
**Next Up**: Real-World HTTP Scrapers (Phase 3.1) - Property Tax Scraper Template

**Ready for Testing**:
```bash
# Test HTTP scraper
curl -X POST http://localhost:8000/jobs \
  -H 'Content-Type: application/json' \
  -d '{"job_id": "test", "items": [{"item_id": "test-001", "input": {"test_param": "hello"}}]}'
```

The foundation is solid and production-ready. We can now build real scrapers on top of this robust infrastructure.