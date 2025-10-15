# Sample Requests

## Get jobs (last 100)

```bash
curl http://localhost:8000/jobs
```

## Get job status

```bash
curl http://localhost:8000/jobs/1234/status
```

## Create job

### Accept defaults

```bash
curl -X POST http://localhost:8000/jobs \
    -H 'Content-Type: application/json' \
    -d '{
        "job_id": "1234",
        "job_name": "Test Job",
        "scraper_id": "test",
        "items": [
            {"item_id":"PIN-001","input":{"parcel_id":"14-28-123-001-0000","tax_year":2024}},
            {"item_id":"PIN-002","input":{"parcel_id":"14-28-123-002-0000","tax_year":2024}}
        ]
    }'
```

### Overrides

```bash
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "job_id": "job-12345",
    "defaults": {
      "strategy": {
        "tier": "auto",
        "proxy_pref": "residential",
        "timeout_ms": 15000,
        "max_retries": 4
      }
    },
    "items": [
      {
        "item_id": "parcel-1",
        "inputs": { "parcel_id": "14-28-123-001-0000" },
        "site_key": "cook-il-treasurer",
        "rate_class": "county:cook-il"
      },
      {
        "item_id": "parcel-2",
        "inputs": { "parcel_id": "14-28-123-002-0000" },
        "site_key": "cook-il-treasurer",
        "rate_class": "county:cook-il",
        "strategy": {
          "proxy_pref": "datacenter"
        }
      },
      {
        "item_id": "parcel-3",
        "inputs": { "parcel_id": "14-28-123-003-0000" },
        "site_key": "la-city-treasurer",
        "rate_class": "city:la",
        "strategy": {
          "tier": "browser",
          "timeout_ms": 30000
        }
      }
    ]
  }'
```

### Examples

#### Throttling

```bash
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "job_id": "circuit-breaker-test",
    "job_name": "Circuit Breaker Test",
    "scraper_id": "example-fail",
    "execution_policy": {
      "throttling": {
        "max_concurrent_workers": 3
      },
      "circuit_breaker": {
        "min_requests": 5,
        "failure_threshold_percentage": 0.8
      },
      "retries": {
        "max_retries": 1
      }
    },
    "items": [
      {"item_id": "item-01", "input": {"test_param": "value1"}},
      {"item_id": "item-02", "input": {"test_param": "value2"}},
      {"item_id": "item-03", "input": {"test_param": "value3"}},
      {"item_id": "item-04", "input": {"test_param": "value4"}},
      {"item_id": "item-05", "input": {"test_param": "value5"}},
      {"item_id": "item-06", "input": {"test_param": "value6"}},
      {"item_id": "item-07", "input": {"test_param": "value7"}},
      {"item_id": "item-08", "input": {"test_param": "value8"}},
      {"item_id": "item-09", "input": {"test_param": "value9"}},
      {"item_id": "item-10", "input": {"test_param": "value10"}}
    ]
  }'
```

#### Rate limiting

```bash
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "job_id": "circuit-breaker-test",
    "job_name": "Circuit Breaker Test",
    "scraper_id": "example-fail",
    "execution_policy": {
      "throttling": {
        "min_delay_between_items_seconds": 5,
        "max_concurrent_workers": 3
      },
      "circuit_breaker": {
        "min_requests": 5,
        "failure_threshold_percentage": 0.8
      },
      "retries": {
        "max_retries": 1
      }
    },
    "items": [
      {"item_id": "item-01", "input": {"test_param": "value1"}},
      {"item_id": "item-02", "input": {"test_param": "value2"}},
      {"item_id": "item-03", "input": {"test_param": "value3"}},
      {"item_id": "item-04", "input": {"test_param": "value4"}},
      {"item_id": "item-05", "input": {"test_param": "value5"}},
      {"item_id": "item-06", "input": {"test_param": "value6"}},
      {"item_id": "item-07", "input": {"test_param": "value7"}},
      {"item_id": "item-08", "input": {"test_param": "value8"}},
      {"item_id": "item-09", "input": {"test_param": "value9"}},
      {"item_id": "item-10", "input": {"test_param": "value10"}}
    ]
  }'
```
