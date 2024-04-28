import boto3
import requests
import json
from datetime import datetime
import os

# Initialize S3 and SNS clients
s3_client = boto3.client("s3")
sns_client = boto3.client("sns")

# Define SNS topic ARN
sns_topic_arn = os.environ["SNS_TOPIC_ARN"]
s3_staging_bucket = os.environ["STAGING_BUCKET"]

# Define function to fetch data from API
def fetch_data():
    # Example API endpoint
    api_url = "https://api.weather.gov/gridpoints/OKX/36,36/forecast/hourly"

    try:
        response = requests.get(api_url)
        response.raise_for_status()  # Raise exception for HTTP errors
        data = response.json()
        return data
    except Exception as e:
        print(f"Error fetching data from API: {e}")
        # Publish failure notification to SNS
        sns_client.publish(
            TopicArn=sns_topic_arn,
            Subject="Data Ingestion Failed",
            Message=f"Error fetching data from API: {e}",
        )
        return None


# Define Lambda handler function
def lambda_handler(event, context):
    # Fetch data from API
    data = fetch_data()

    if data:
        # Generate S3 object key with timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        object_key = f"nyc/{timestamp}.json"

        # Upload data to staging S3 bucket
        try:
            s3_client.put_object(
                Bucket=s3_staging_bucket,
                Key=object_key,
                Body=json.dumps(data),
                ContentType="application/json",
            )
            print("Data ingested into staging S3 successfully")
            # Publish success notification to SNS
            sns_client.publish(
                TopicArn=sns_topic_arn,
                Subject="Data Ingestion Successful",
                Message="Data ingested into staging S3 successfully",
            )
            return {
                "statusCode": 200,
                "body": json.dumps("Data ingested into staging S3 successfully"),
            }
        except Exception as e:
            print(f"Error ingesting data into staging S3: {e}")
            # Publish failure notification to SNS
            sns_client.publish(
                TopicArn=sns_topic_arn,
                Subject="Data Ingestion Failed",
                Message=f"Error ingesting data into staging S3: {e}",
            )
            return {
                "statusCode": 500,
                "body": json.dumps("Error ingesting data into staging S3"),
            }
    else:
        print("No data fetched from API")
        # Publish failure notification to SNS
        sns_client.publish(
            TopicArn=sns_topic_arn,
            Subject="Data Ingestion Failed",
            Message="No data fetched from API",
        )
        return {"statusCode": 500, "body": json.dumps("No data fetched from API")}
