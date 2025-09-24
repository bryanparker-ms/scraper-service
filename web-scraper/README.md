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
