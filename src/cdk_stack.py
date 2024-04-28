from aws_cdk import (
    aws_lambda as _lambda,
    aws_sns as sns,
    aws_events as events,
    aws_events_targets as targets,
    aws_s3 as s3,
    aws_s3_notifications as s3_notifications,
    Duration,
)
from aws_cdk import Stack
from constructs import Construct


class CdkStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create S3 bucket for staging
        staging_bucket = s3.Bucket(self, "DataFoundryStagingLayer")

        # Create S3 bucket for raw data
        raw_bucket = s3.Bucket(self, "DataFoundryRawLayer")

        # Create SNS topic for notifications
        sns_topic = sns.Topic(self, "DataIngestionTopic")

        # Create Lambda function for data ingestion
        data_ingestion_lambda = _lambda.Function(
            self,
            "DataIngestionLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("./lambda_ingestion_api.zip"),
            environment={
                "STAGING_BUCKET": staging_bucket.bucket_name,
                "SNS_TOPIC_ARN": sns_topic.topic_arn,
            },
        )

        # Grant permissions to Lambda to write to staging bucket
        staging_bucket.grant_write(data_ingestion_lambda)

        # Grant permissions to Lambda to publish to SNS topic
        sns_topic.grant_publish(data_ingestion_lambda)

        # Create Lambda function for data quality check and partitioning
        data_quality_lambda = _lambda.Function(
            self,
            "DataQualityLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("./lambda_dq.zip"),
            environment={
                "RAW_BUCKET": raw_bucket.bucket_name,
                "SNS_TOPIC_ARN": sns_topic.topic_arn,
            },
        )

        # Grant permissions to Lambda to read from staging bucket
        staging_bucket.grant_read(data_quality_lambda)

        # Grant permissions to Lambda to write to raw bucket
        raw_bucket.grant_write(data_quality_lambda)

        # Grant permissions to Lambda to publish to SNS topic
        sns_topic.grant_publish(data_quality_lambda)

        # Create EventBridge rule to trigger Lambda for data ingestion
        event_rule = events.Rule(
            self, "DataIngestionRule", schedule=events.Schedule.rate(Duration.hours(1))
        )
        event_rule.add_target(targets.LambdaFunction(data_ingestion_lambda))

        # Define S3 event notification to trigger data quality Lambda when a new file is uploaded to staging bucket
        staging_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3_notifications.LambdaDestination(data_quality_lambda),
        )
