import boto3
import json
import os
from pandas import json_normalize
from io import StringIO
import pandas as pd

# Initialize S3 and SNS clients
s3_client = boto3.client('s3')
sns_client = boto3.client('sns')

# Retrieve S3 bucket name and partition base path from environment variables
raw_bucket_name = os.environ['RAW_BUCKET']

# Define SNS topic ARN
sns_topic_arn = os.environ['SNS_TOPIC_ARN']

def temperature_operations(df):
    """
    Perform temperature-related operations on the DataFrame.

    Args:
        df (DataFrame): DataFrame containing weather data.

    Returns:
        DataFrame: DataFrame with temperature in C and F.
    """
    # Rename 'temperature' column to 'temperature_F'
    df.rename(columns={'temperature': 'temperature_F'}, inplace=True)
    
    # Convert temperature from Fahrenheit to Celsius and store in a new column
    df['temperature_C'] = (df['temperature_F'] - 32) * 5/9
    
    # Drop the 'temperatureUnit' column
    df.drop(columns=['temperatureUnit'], inplace=True)
    
    return df


# Define Lambda handler function
def lambda_handler(event, context):
    # Extract bucket and object key from the S3 event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    
    obj_dir = os.path.dirname(object_key)

    file_ts = os.path.basename(object_key).split("-")
    partition_base_path = 'year={}/month={}/date={}/hour={}/'.format(file_ts[0], file_ts[1], file_ts[2], file_ts[3])

    # Read data from the staging S3 object
    try:
        response = s3_client.get_object(
            Bucket=bucket_name,
            Key=object_key
        )
        data = json.loads(response['Body'].read().decode('utf-8'))
    except Exception as e:
        print(f"Error reading data from staging S3: {e}")
        # Publish failure notification to SNS
        sns_client.publish(
            TopicArn=sns_topic_arn,
            Subject='Data Quality Check Failed',
            Message=f'Error reading data from staging S3: {e}'
        )
        return {
            'statusCode': 500,
            'body': json.dumps('Error reading data from staging S3')
        }

    # Perform data quality checks (example: check if data is not empty)
    if data:
        
        try:

            # Convert JSON data to DataFrame
            df = json_normalize(data['properties']['periods'])
            
            # Sanitize column names
            df.columns = df.columns.str.replace('.', '_')

            df = temperature_operations(df)

            # Add timestamp as a new column
            df['extract_time'] = pd.to_datetime(os.path.basename(object_key).split(".")[0], format='%Y-%m-%d-%H-%M-%S')
            # Define output file path including partition
            output_file_path = f"{obj_dir}/{partition_base_path}weather.csv"
            
            # Write DataFrame to Parquet file in the partitioned directory
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            s3_client.put_object(Bucket=raw_bucket_name, Key=output_file_path, Body=csv_buffer.getvalue())
            
            print("Data quality check passed. Data written to partitioned storage.")
            # Publish success notification to SNS
            sns_client.publish(
                TopicArn=sns_topic_arn,
                Subject='Data Quality Check Passed',
                Message='Data quality check passed. Data written to partitioned storage.'
            )
            return {
                'statusCode': 200,
                'body': json.dumps('Data quality check passed. Data written to partitioned storage.')
            }
        except Exception as e:
            print(f"Error writing data to partitioned storage: {e}")
            # Publish failure notification to SNS
            sns_client.publish(
                TopicArn=sns_topic_arn,
                Subject='Data Quality Check Failed',
                Message=f'Error writing data to partitioned storage: {e}'
            )
            return {
                'statusCode': 500,
                'body': json.dumps('Error writing data to partitioned storage')
            }
    else:
        print("Data quality check failed. Data is empty.")
        # Publish failure notification to SNS
        sns_client.publish(
            TopicArn=sns_topic_arn,
            Subject='Data Quality Check Failed',
            Message='Data quality check failed. Data is empty.'
        )
        return {
            'statusCode': 500,
            'body': json.dumps('Data quality check failed. Data is empty.')
        }
