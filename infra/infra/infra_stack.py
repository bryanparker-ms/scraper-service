from typing import Any
from aws_cdk import (
    Duration,
    Stack,
    aws_sqs as sqs,
    aws_dynamodb as dynamodb,
    RemovalPolicy,
    aws_iam as iam,
    aws_s3 as s3,
    CfnOutput,
)
from constructs import Construct

class InfraStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs: Any) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # DynamoDB table for scrape state management
        scrape_state_table = dynamodb.Table(
            self, 'JobTable',
            table_name='scrape-state',
            partition_key=dynamodb.Attribute(
                name='pk',
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name='sk',
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,  # For development - change to RETAIN for production
            point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=True
            ),
        )

        # Add GSI for querying by status
        scrape_state_table.add_global_secondary_index(
            index_name='status-index',
            partition_key=dynamodb.Attribute(
                name='status',
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name='sk',
                type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL
        )

        # SQS Queue for job processing
        job_queue = sqs.Queue(
            self, 'JobQueue',
            queue_name='web-scraper-job-queue',
            visibility_timeout=Duration.seconds(300),  # 5 minutes
            retention_period=Duration.days(14),  # Keep messages for 14 days
            receive_message_wait_time=Duration.seconds(20),  # Long polling
            removal_policy=RemovalPolicy.DESTROY,  # For development - change to RETAIN for production
        )

        # Dead Letter Queue for failed messages
        dead_letter_queue = sqs.Queue(
            self, 'DeadLetterQueue',
            queue_name='web-scraper-dlq',
            retention_period=Duration.days(14),
            removal_policy=RemovalPolicy.DESTROY,  # For development - change to RETAIN for production
        )

        # Configure the main queue to use the DLQ
        job_queue_with_dlq = sqs.Queue(
            self, 'JobQueueWithDLQ',
            queue_name='web-scraper-job-queue-with-dlq',
            visibility_timeout=Duration.seconds(300),
            retention_period=Duration.days(14),
            receive_message_wait_time=Duration.seconds(20),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3,  # Move to DLQ after 3 failed attempts
                queue=dead_letter_queue
            ),
            removal_policy=RemovalPolicy.DESTROY,  # For development - change to RETAIN for production
        )

        # Create IAM Role for the application (ECS Fargate recommended for web scraper)
        application_role = iam.Role(
            self, 'WebScraperApplicationRole',
            assumed_by=iam.ServicePrincipal('ecs-tasks.amazonaws.com'),  # For ECS Fargate deployment
            role_name='web-scraper-application-role'
        )

        # S3 Bucket for storing scrape results
        scrape_results_bucket = s3.Bucket(
            self, 'ScrapeResultsBucket',
            bucket_name=f'scrape-results-{self.account}-{self.region}',  # Make bucket name unique
            versioned=True,  # Enable versioning for data protection
            encryption=s3.BucketEncryption.S3_MANAGED,  # Server-side encryption
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,  # Block all public access
            removal_policy=RemovalPolicy.DESTROY,  # For development - change to RETAIN for production
            lifecycle_rules=[
                s3.LifecycleRule(
                    id='DeleteOldVersions',
                    noncurrent_version_expiration=Duration.days(30),  # Delete old versions after 30 days
                    enabled=True
                ),
                s3.LifecycleRule(
                    id='DeleteIncompleteMultipart',
                    abort_incomplete_multipart_upload_after=Duration.days(7),  # Clean up incomplete uploads
                    enabled=True
                )
            ]
        )

        # Grant specific permissions to the application role
        scrape_state_table.grant_read_write_data(application_role)
        job_queue.grant_send_messages(application_role)
        job_queue.grant_consume_messages(application_role)
        job_queue_with_dlq.grant_send_messages(application_role)
        job_queue_with_dlq.grant_consume_messages(application_role)
        dead_letter_queue.grant_send_messages(application_role)
        dead_letter_queue.grant_consume_messages(application_role)
        scrape_results_bucket.grant_read_write(application_role)  # Grant S3 read/write permissions

        # Create a deployment user (optional - for production environments)
        deployment_user = iam.User(
            self, 'WebScraperDeploymentUser',
            user_name='web-scraper-deployment-user'
        )

        # Create deployment policy with minimal required permissions
        deployment_policy = iam.Policy(
            self, 'WebScraperDeploymentPolicy',
            policy_name='web-scraper-deployment-policy',
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        'cloudformation:*',
                        's3:*',
                        'iam:CreateRole',
                        'iam:DeleteRole',
                        'iam:GetRole',
                        'iam:PutRolePolicy',
                        'iam:DeleteRolePolicy',
                        'iam:PassRole',
                        'dynamodb:*',
                        'sqs:*',
                        'logs:*',
                        'lambda:*',
                        'ec2:*',
                        'ecs:*',
                        'application-autoscaling:*'
                    ],
                    resources=['*']
                )
            ]
        )

        # Attach deployment policy to deployment user
        deployment_policy.attach_to_user(deployment_user)

        # Outputs
        CfnOutput(
            self, 'ApplicationRoleArn',
            value=application_role.role_arn,
            description='ARN of the application role',
            export_name='WebScraperApplicationRoleArn'
        )

        CfnOutput(
            self, 'DynamoDBTableName',
            value=scrape_state_table.table_name,
            description='DynamoDB table name',
            export_name='WebScraperTableName'
        )

        CfnOutput(
            self, 'JobQueueUrl',
            value=job_queue.queue_url,
            description='SQS job queue URL',
            export_name='WebScraperJobQueueUrl'
        )

        CfnOutput(
            self, 'JobQueueWithDLQUrl',
            value=job_queue_with_dlq.queue_url,
            description='SQS job queue with DLQ URL',
            export_name='WebScraperJobQueueWithDLQUrl'
        )

        CfnOutput(
            self, 'ScrapeResultsBucketName',
            value=scrape_results_bucket.bucket_name,
            description='S3 bucket name for scrape results',
            export_name='WebScraperResultsBucketName'
        )

        CfnOutput(
            self, 'ScrapeResultsBucketArn',
            value=scrape_results_bucket.bucket_arn,
            description='S3 bucket ARN for scrape results',
            export_name='WebScraperResultsBucketArn'
        )
