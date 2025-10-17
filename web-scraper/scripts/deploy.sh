#!/bin/bash

# Deployment script for web scraper services
# Usage: ./deploy.sh [controller|worker|all]
#   controller - Deploy only the controller service
#   worker     - Deploy only the worker service
#   all        - Deploy both services (default)

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Parse arguments
DEPLOY_TARGET=${1:-all}

if [[ ! "$DEPLOY_TARGET" =~ ^(controller|worker|all)$ ]]; then
    echo -e "${RED}Invalid argument: $DEPLOY_TARGET${NC}"
    echo "Usage: $0 [controller|worker|all]"
    exit 1
fi

# Configuration
AWS_REGION=${AWS_REGION:-us-east-1}
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
CONTROLLER_REPO_URI="${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/web-scraper-controller"
WORKER_REPO_URI="${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/web-scraper-worker"

echo -e "${YELLOW}Deploying Web Scraper Services (${DEPLOY_TARGET})${NC}"
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

# Build and push images based on target
if [[ "$DEPLOY_TARGET" == "controller" || "$DEPLOY_TARGET" == "all" ]]; then
    build_and_push "controller" "controller.dockerfile" "${CONTROLLER_REPO_URI}"
fi

if [[ "$DEPLOY_TARGET" == "worker" || "$DEPLOY_TARGET" == "all" ]]; then
    build_and_push "worker" "worker.dockerfile" "${WORKER_REPO_URI}"
fi

echo -e "${GREEN}✓ Images built and pushed successfully${NC}"

# Update services based on target
echo -e "${YELLOW}Updating services...${NC}"

if [[ "$DEPLOY_TARGET" == "controller" || "$DEPLOY_TARGET" == "all" ]]; then
    # Get App Runner service ARN for controller
    CONTROLLER_SERVICE_ARN=$(aws apprunner list-services \
        --region ${AWS_REGION} \
        --query 'ServiceSummaryList[?ServiceName==`web-scraper-controller`].ServiceArn' \
        --output text)

    if [ -n "$CONTROLLER_SERVICE_ARN" ]; then
        echo -e "${YELLOW}Triggering App Runner deployment for controller...${NC}"
        aws apprunner start-deployment \
            --service-arn ${CONTROLLER_SERVICE_ARN} \
            --region ${AWS_REGION}
        echo -e "${GREEN}✓ Controller deployment triggered${NC}"
    else
        echo -e "${RED}✗ Controller App Runner service not found${NC}"
    fi
fi

if [[ "$DEPLOY_TARGET" == "worker" || "$DEPLOY_TARGET" == "all" ]]; then
    # Update worker ECS service
    echo -e "${YELLOW}Updating worker ECS service...${NC}"
    aws ecs update-service \
        --cluster web-scraper-cluster \
        --service web-scraper-worker \
        --force-new-deployment \
        --region ${AWS_REGION}
    echo -e "${GREEN}✓ Worker ECS service updated${NC}"
fi

# Get controller URL from CloudFormation stack
CONTROLLER_URL=$(aws cloudformation describe-stacks \
    --stack-name web-scraper-infra \
    --query 'Stacks[0].Outputs[?OutputKey==`ControllerServiceUrl`].OutputValue' \
    --output text \
    --region ${AWS_REGION})

echo
echo -e "${GREEN}Deployment complete!${NC}"
echo -e "Controller URL: https://${CONTROLLER_URL}"
echo -e "Monitor App Runner: https://console.aws.amazon.com/apprunner/home?region=${AWS_REGION}#/services"
echo -e "Monitor ECS: https://console.aws.amazon.com/ecs/home?region=${AWS_REGION}#/clusters/web-scraper-cluster/services"
