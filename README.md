# Web Scraper

A distributed web scraping service built with FastAPI, AWS SQS, and DynamoDB.

## Project Structure

This repository contains two main components:

### [web-scraper/](web-scraper/)
The core application code including controller, worker, and scrapers.

**See [web-scraper/README.md](web-scraper/README.md) for:**
- Service architecture and setup
- API documentation
- Local development guide
- Writing custom scrapers
- Testing scrapers with the test harness

### [infra/](infra/)
AWS infrastructure defined with AWS CDK (Python).

**See [infra/README.md](infra/README.md) for:**
- Infrastructure components
- Deployment instructions
- CDK stack details

## Quick Start

1. **Set up infrastructure:**
   ```bash
   cd infra
   # Follow instructions in infra/README.md
   ```

2. **Run the application:**
   ```bash
   cd web-scraper
   # Follow instructions in web-scraper/README.md
   ```

## Documentation

- [Application README](web-scraper/README.md) - Service setup, API docs, development
- [Infrastructure README](infra/README.md) - AWS infrastructure and deployment
- [Deployment Guide](DEPLOYMENT.md) - Detailed deployment instructions