import os
import boto3
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Retrieve AWS credentials and configuration from environment variables
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
default_region_name = os.getenv('DEFAULT_REGION_NAME')

# Create a Boto3 session using the loaded credentials
session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=default_region_name
)

# Example: Create an S3 client
s3_client = session.client('s3')

# Now you can use s3_client to interact with S3 without needing to run aws configure