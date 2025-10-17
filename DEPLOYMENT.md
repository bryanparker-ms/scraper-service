# Web Scraper Deployment Guide

This guide covers deploying the web scraper service to AWS using ECS Fargate.

## Architecture Overview

### Services

-   **Controller Service**: FastAPI web service with Application Load Balancer
-   **Worker Service**: Background processing service with auto-scaling
-   **Infrastructure**: DynamoDB, SQS, S3, ECS Cluster, VPC

### Deployment Strategy

-   **Controller**: ECS Fargate + ALB (always-on, stateless)
-   **Worker**: ECS Fargate with auto-scaling based on SQS queue depth
-   **Container Images**: Stored in ECR repositories

## Prerequisites

1. **AWS CLI configured** with appropriate permissions
2. **Podman installed** and running (or Docker as alternative)
3. **CDK installed**: `npm install -g aws-cdk`
4. **Python dependencies**: `uv install` in both `web-scraper/` and `infra/` directories
5. **Existing VPC**: The deployment uses VPC `vpc-142e866e` which must have:
    - Public subnets (for ALB and NAT gateway)
    - Private subnets with NAT gateway access (for workers)
    - Internet Gateway attached

## Deployment Steps

### 1. Deploy Infrastructure

```bash
cd infra/
cdk bootstrap  # First time only
cdk deploy
```

This creates:

-   ECS cluster (using existing VPC vpc-142e866e)
-   ECR repositories
-   Application Load Balancer
-   Auto-scaling configuration
-   CloudWatch log groups

### 2. Build and Deploy Services

```bash
cd ../web-scraper/
./scripts/deploy.sh
```

This script:

-   Builds Docker images for controller and worker
-   Pushes images to ECR repositories
-   Updates ECS services with new images

### 3. Verify Deployment

```bash
# Get controller URL
aws cloudformation describe-stacks \
    --stack-name InfraStack \
    --query 'Stacks[0].Outputs[?OutputKey==`ControllerLoadBalancerUrl`].OutputValue' \
    --output text

# Test the API
curl http://<controller-url>/jobs
```

## Service Configuration

### Controller Service

-   **CPU**: 256 units (0.25 vCPU)
-   **Memory**: 512 MB
-   **Port**: 8000
-   **Scaling**: Fixed at 1 instance
-   **Load Balancer**: Public ALB with health checks

### Worker Service

-   **CPU**: 256 units (0.25 vCPU)
-   **Memory**: 512 MB
-   **Scaling**: 0-10 instances based on:
    -   SQS queue depth (primary) - scales to 0 when no work
    -   CPU utilization (secondary)
-   **Network**: Private subnets only
-   **Cost Optimization**: Scales down to 0 workers during idle periods

## Auto-Scaling Configuration

### Worker Scaling Rules

1. **SQS Queue Depth**:

    - 0 messages: 0 workers (cost savings!)
    - 1-5 messages: 1 worker
    - 6-20 messages: 2 workers
    - 20+ messages: 5 workers
    - Max: 10 workers

2. **CPU Utilization**:

    - Target: 70% CPU
    - Scale up/down as needed

3. **Cooldown Periods**:
    - Scale up: 30 seconds (fast response to new work)
    - Scale down: 60 seconds (prevent rapid scaling)

## Monitoring

### CloudWatch Logs

-   Controller: `/ecs/web-scraper-controller`
-   Worker: `/ecs/web-scraper-worker`
-   Retention: 1 month

### CloudWatch Metrics

-   ECS service metrics (CPU, memory, task count)
-   SQS queue metrics (message count, age)
-   Application metrics (custom)

### Container Insights

-   Enabled on ECS cluster
-   Provides detailed container performance metrics

## Environment Variables

Both services receive these environment variables:

-   `AWS_DEFAULT_REGION`: AWS region
-   `AWS_ACCOUNT_ID`: AWS account ID
-   `DYNAMODB_TABLE_NAME`: DynamoDB table name
-   `SQS_QUEUE_URL`: SQS queue URL
-   `S3_BUCKET_NAME`: S3 bucket name
-   `USE_LOCAL_STORAGE`: Set to 'false' for production

## Cost Optimization

### Development Environment

-   Single NAT gateway (cost optimization)
-   Spot instances for workers (optional)
-   1-month log retention
-   DESTROY removal policy for development

### Production Considerations

-   Multiple NAT gateways for high availability
-   Reserved capacity for predictable workloads
-   Longer log retention
-   RETAIN removal policy for critical resources

## Cost Optimization

### Zero-Scale Workers (Major Cost Savings!)

-   **Workers scale to 0** when no work is queued
-   **Nighttime savings**: No idle workers during off-hours
-   **Automatic scaling**: Workers start within 30 seconds of new work
-   **Estimated savings**: 60-80% reduction in worker costs during idle periods

### Cost Examples

-   **Idle period (8 hours/night)**: $0 worker costs
-   **Light usage (1 worker)**: ~$8-9/month
-   **Heavy usage (5 workers)**: ~$40-45/month
-   **Controller (always on)**: ~$25-30/month

## Troubleshooting

### Common Issues

1. **Service won't start**:

    ```bash
    # Check ECS service events
    aws ecs describe-services --cluster web-scraper-cluster --services web-scraper-controller
    ```

2. **Images not found**:

    ```bash
    # Verify ECR repositories exist
    aws ecr describe-repositories --region us-east-1
    ```

3. **Permission errors**:
    ```bash
    # Check IAM role permissions
    aws iam get-role --role-name web-scraper-application-role
    ```

### Logs

```bash
# View controller logs
aws logs tail /ecs/web-scraper-controller --follow

# View worker logs
aws logs tail /ecs/web-scraper-worker --follow
```

## Scaling the System

### Manual Scaling

```bash
# Scale workers manually
aws ecs update-service \
    --cluster web-scraper-cluster \
    --service web-scraper-worker \
    --desired-count 5
```

### Auto-scaling Tuning

Modify the scaling configuration in `infra/infra_stack.py`:

-   Adjust `min_capacity` and `max_capacity`
-   Modify scaling thresholds
-   Add custom metrics

## Security Considerations

-   Workers run in private subnets (no public IP)
-   Controller accessible via ALB only
-   IAM roles follow principle of least privilege
-   All traffic encrypted in transit
-   S3 bucket blocks public access

## Next Steps

1. **SSL/TLS**: Add ACM certificate to ALB
2. **Custom Domain**: Configure Route 53 for custom domain
3. **Monitoring**: Set up CloudWatch alarms
4. **CI/CD**: Integrate with GitHub Actions or similar
5. **Multi-region**: Deploy to multiple regions for disaster recovery
