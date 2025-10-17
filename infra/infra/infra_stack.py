from typing import Any
import os
from aws_cdk import (
    Duration,
    Stack,
    aws_sqs as sqs,
    aws_dynamodb as dynamodb,
    RemovalPolicy,
    aws_iam as iam,
    aws_s3 as s3,
    aws_ecs as ecs,
    aws_ec2 as ec2,
    aws_ecr as ecr,
    aws_applicationautoscaling as autoscaling,
    aws_cloudwatch as cloudwatch,
    aws_logs as logs,
    aws_apprunner as apprunner,
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

        # Create IAM Role for ECS tasks
        ecs_task_role = iam.Role(
            self, 'EcsTaskRole',
            assumed_by=iam.ServicePrincipal('ecs-tasks.amazonaws.com'),  # type: ignore
            role_name='web-scraper-ecs-task-role'
        )

        # Create IAM Role for App Runner (access role for ECR)
        apprunner_access_role = iam.Role(
            self, 'AppRunnerAccessRole',
            assumed_by=iam.ServicePrincipal('build.apprunner.amazonaws.com'),  # type: ignore
            role_name='web-scraper-apprunner-access-role'
        )

        # Create IAM Role for App Runner Instance (runtime permissions)
        apprunner_instance_role = iam.Role(
            self, 'AppRunnerInstanceRole',
            assumed_by=iam.ServicePrincipal('tasks.apprunner.amazonaws.com'),  # type: ignore
            role_name='web-scraper-apprunner-instance-role'
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

        # Grant specific permissions to both roles
        scrape_state_table.grant_read_write_data(ecs_task_role)
        scrape_state_table.grant_read_write_data(apprunner_instance_role)
        job_queue.grant_send_messages(ecs_task_role)
        job_queue.grant_consume_messages(ecs_task_role)
        job_queue.grant_send_messages(apprunner_instance_role)
        job_queue_with_dlq.grant_send_messages(ecs_task_role)
        job_queue_with_dlq.grant_consume_messages(ecs_task_role)
        job_queue_with_dlq.grant_send_messages(apprunner_instance_role)
        dead_letter_queue.grant_send_messages(ecs_task_role)
        dead_letter_queue.grant_consume_messages(ecs_task_role)
        scrape_results_bucket.grant_read_write(ecs_task_role)
        scrape_results_bucket.grant_read_write(apprunner_instance_role)

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

        # Use existing VPC
        vpc = ec2.Vpc.from_lookup(
            self, 'TaxProperVpc',
            vpc_id='vpc-142e866e'
        )

        # Create ECS Cluster
        cluster = ecs.Cluster(
            self, 'WebScraperCluster',
            cluster_name='web-scraper-cluster',
            vpc=vpc,
            container_insights=True  # Enable CloudWatch Container Insights
        )

        # Create ECR repositories for container images
        controller_repo = ecr.Repository(
            self, 'ControllerRepository',
            repository_name='web-scraper-controller',
            removal_policy=RemovalPolicy.DESTROY  # For development
        )

        worker_repo = ecr.Repository(
            self, 'WorkerRepository',
            repository_name='web-scraper-worker',
            removal_policy=RemovalPolicy.DESTROY  # For development
        )

        # Grant ECR permissions to App Runner access role
        controller_repo.grant_pull(apprunner_access_role)

        # Create CloudWatch Log Groups
        controller_log_group = logs.LogGroup(
            self, 'ControllerLogGroup',
            log_group_name='/aws/apprunner/web-scraper-controller',
            retention=logs.RetentionDays.ONE_MONTH,
            removal_policy=RemovalPolicy.DESTROY
        )

        worker_log_group = logs.LogGroup(
            self, 'WorkerLogGroup',
            log_group_name='/ecs/web-scraper-worker',
            retention=logs.RetentionDays.ONE_MONTH,
            removal_policy=RemovalPolicy.DESTROY
        )

        # Grant log permissions to both roles
        controller_log_group.grant_write(apprunner_instance_role)
        worker_log_group.grant_write(ecs_task_role)

        # Create App Runner Service for Controller (much cheaper than Fargate + ALB)
        controller_service = apprunner.CfnService(
            self, 'ControllerService',
            service_name='web-scraper-controller',
            source_configuration=apprunner.CfnService.SourceConfigurationProperty(
                authentication_configuration=apprunner.CfnService.AuthenticationConfigurationProperty(
                    access_role_arn=apprunner_access_role.role_arn
                ),
                image_repository=apprunner.CfnService.ImageRepositoryProperty(
                    image_identifier=f'{self.account}.dkr.ecr.{self.region}.amazonaws.com/web-scraper-controller:latest',
                    image_configuration=apprunner.CfnService.ImageConfigurationProperty(
                        port='8000',
                        runtime_environment_variables=[
                            apprunner.CfnService.KeyValuePairProperty(
                                name='AWS_REGION',
                                value=self.region
                            ),
                            apprunner.CfnService.KeyValuePairProperty(
                                name='AWS_ACCOUNT_ID',
                                value=self.account
                            ),
                            apprunner.CfnService.KeyValuePairProperty(
                                name='TABLE_NAME',
                                value=scrape_state_table.table_name
                            ),
                            apprunner.CfnService.KeyValuePairProperty(
                                name='WEB_SCRAPER_QUEUE_URL',
                                value=job_queue_with_dlq.queue_url
                            ),
                            apprunner.CfnService.KeyValuePairProperty(
                                name='WEB_SCRAPER_RESULTS_BUCKET',
                                value=scrape_results_bucket.bucket_name
                            ),
                            apprunner.CfnService.KeyValuePairProperty(
                                name='USE_LOCAL_STORAGE',
                                value='false'
                            ),
                            apprunner.CfnService.KeyValuePairProperty(
                                name='API_KEY',
                                value=os.environ.get('API_KEY', '')
                            )
                        ]
                    ),
                    image_repository_type='ECR'
                ),
                auto_deployments_enabled=False
            ),
            instance_configuration=apprunner.CfnService.InstanceConfigurationProperty(
                cpu='0.25 vCPU',
                memory='0.5 GB',
                instance_role_arn=apprunner_instance_role.role_arn
            )
        )

        # Create Worker Service
        worker_task_definition = ecs.FargateTaskDefinition(
            self, 'WorkerTaskDefinition',
            family='web-scraper-worker',
            cpu=256,
            memory_limit_mib=512,
            task_role=ecs_task_role  # type: ignore
        )

        # Add container to worker task definition
        worker_task_definition.add_container(
            'worker',
            image=ecs.ContainerImage.from_ecr_repository(worker_repo, 'latest'),
            logging=ecs.LogDrivers.aws_logs(
                stream_prefix='worker',
                log_group=worker_log_group
            ),
            environment={
                'AWS_REGION': self.region,
                'AWS_ACCOUNT_ID': self.account,
                'TABLE_NAME': scrape_state_table.table_name,
                'WEB_SCRAPER_QUEUE_URL': job_queue_with_dlq.queue_url,
                'WEB_SCRAPER_RESULTS_BUCKET': scrape_results_bucket.bucket_name,
                'USE_LOCAL_STORAGE': 'false'
            }
        )

        # Create worker service
        worker_service = ecs.FargateService(
            self, 'WorkerService',
            cluster=cluster,
            service_name='web-scraper-worker',
            task_definition=worker_task_definition,
            desired_count=0,  # Start with 0 workers, scale up when needed
            assign_public_ip=True,  # Workers need public IP since no private subnets
            vpc_subnets=ec2.SubnetSelection(
                availability_zones=['us-east-1a', 'us-east-1b']
            )
        )

        # Create Auto Scaling for Worker Service
        worker_scaling = worker_service.auto_scale_task_count(
            min_capacity=0,  # Scale down to 0 when no work
            max_capacity=10
        )

        # Scale based on SQS queue depth
        worker_scaling.scale_on_metric(
            'SqsQueueDepth',
            metric=cloudwatch.Metric(
                namespace='AWS/SQS',
                metric_name='ApproximateNumberOfVisibleMessages',
                dimensions_map={'QueueName': 'web-scraper-job-queue-with-dlq'}
            ),
            scaling_steps=[
                autoscaling.ScalingInterval(upper=0, change=0),      # 0 messages = 0 workers
                autoscaling.ScalingInterval(upper=5, change=1),      # 1-5 messages = 1 worker
                autoscaling.ScalingInterval(upper=20, change=2),     # 6-20 messages = 2 workers
                autoscaling.ScalingInterval(lower=20, change=5)      # 20+ messages = 5 workers
            ],
            adjustment_type=autoscaling.AdjustmentType.CHANGE_IN_CAPACITY,
            cooldown=Duration.seconds(60)  # Wait 1 minute before scaling down
        )

        # Scale based on CPU utilization
        worker_scaling.scale_on_cpu_utilization(
            'CpuScaling',
            target_utilization_percent=70
        )

        # Outputs
        CfnOutput(
            self, 'EcsTaskRoleArn',
            value=ecs_task_role.role_arn,
            description='ARN of the ECS task role',
            export_name='WebScraperEcsTaskRoleArn'
        )

        CfnOutput(
            self, 'AppRunnerAccessRoleArn',
            value=apprunner_access_role.role_arn,
            description='ARN of the App Runner access role',
            export_name='WebScraperAppRunnerAccessRoleArn'
        )

        CfnOutput(
            self, 'AppRunnerInstanceRoleArn',
            value=apprunner_instance_role.role_arn,
            description='ARN of the App Runner instance role',
            export_name='WebScraperAppRunnerInstanceRoleArn'
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

        CfnOutput(
            self, 'ControllerServiceUrl',
            value=controller_service.attr_service_url,
            description='Controller service URL',
            export_name='WebScraperControllerUrl'
        )

        CfnOutput(
            self, 'ControllerRepositoryUri',
            value=controller_repo.repository_uri,
            description='ECR repository URI for controller',
            export_name='WebScraperControllerRepoUri'
        )

        CfnOutput(
            self, 'WorkerRepositoryUri',
            value=worker_repo.repository_uri,
            description='ECR repository URI for worker',
            export_name='WebScraperWorkerRepoUri'
        )

        CfnOutput(
            self, 'EcsClusterName',
            value=cluster.cluster_name,
            description='ECS cluster name',
            export_name='WebScraperClusterName'
        )

        CfnOutput(
            self, 'VpcId',
            value=vpc.vpc_id,
            description='VPC ID being used',
            export_name='WebScraperVpcId'
        )

        CfnOutput(
            self, 'VpcCidr',
            value=vpc.vpc_cidr_block,
            description='VPC CIDR block',
            export_name='WebScraperVpcCidr'
        )
