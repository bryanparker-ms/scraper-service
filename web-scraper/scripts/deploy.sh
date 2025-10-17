#!/bin/bash

# Deployment script for web scraper services
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
AWS_REGION=${AWS_REGION:-us-east-1}
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
CONTROLLER_REPO_URI="${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/web-scraper-controller"
WORKER_REPO_URI="${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/web-scraper-worker"

echo -e "${YELLOW}Deploying Web Scraper Services${NC}"
echo "Account ID: ${ACCOUNT_ID}"
echo "Region: ${AWS_REGION}"
echo "Controller Repo: ${CONTROLLER_REPO_URI}"
echo "Worker Repo: ${WORKER_REPO_URI}"
echo

# Function to build and push container image
build_and_push() {
    local service=$1
    local dockerfile=$2
    local repo_uri=$3

    echo -e "${YELLOW}Building and pushing ${service}...${NC}"

    # Login to ECR
    aws ecr get-login-password --region ${AWS_REGION} | podman login --username AWS --password-stdin ${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com

    # Build image for linux/amd64 platform
    podman build --platform linux/amd64 -f ${dockerfile} -t ${service}:latest .

    # Tag for ECR
    podman tag ${service}:latest ${repo_uri}:latest

    # Push to ECR
    podman push ${repo_uri}:latest

    echo -e "${GREEN}✓ ${service} image pushed successfully${NC}"
}

# Build and push controller
build_and_push "controller" "controller.dockerfile" "${CONTROLLER_REPO_URI}"

# # Build and push worker
build_and_push "worker" "worker.dockerfile" "${WORKER_REPO_URI}"

echo -e "${GREEN}✓ All images built and pushed successfully${NC}"

# Update ECS services
echo -e "${YELLOW}Updating ECS services...${NC}"

Update controller service
aws ecs update-service \
    --cluster web-scraper-cluster \
    --service web-scraper-controller \
    --force-new-deployment \
    --region ${AWS_REGION}

# Update worker service
aws ecs update-service \
    --cluster web-scraper-cluster \
    --service web-scraper-worker \
    --force-new-deployment \
    --region ${AWS_REGION}

echo -e "${GREEN}✓ ECS services updated${NC}"

# Get controller URL
CONTROLLER_URL=$(aws cloudformation describe-stacks \
    --stack-name InfraStack \
    --query 'Stacks[0].Outputs[?OutputKey==`ControllerLoadBalancerUrl`].OutputValue' \
    --output text \
    --region ${AWS_REGION})

echo
echo -e "${GREEN}Deployment complete!${NC}"
echo -e "Controller URL: ${CONTROLLER_URL}"
echo -e "Monitor services in ECS console: https://console.aws.amazon.com/ecs/home?region=${AWS_REGION}#/clusters/web-scraper-cluster/services"
