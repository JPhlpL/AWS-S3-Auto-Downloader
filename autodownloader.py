import os
import boto3
import logging
import csv
import json
import re
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Setup logging to include time, level, and message
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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

# Create an S3 client
s3_client = session.client('s3')

def download_specific_file(bucket_name, file_key, download_dir):
    """
    Downloads a specific file from S3.
    """
    os.makedirs(download_dir, exist_ok=True)
    local_file_path = os.path.join(download_dir, os.path.basename(file_key))
    
    try:
        logging.info(f"Downloading {file_key} to {local_file_path}...")
        s3_client.download_file(bucket_name, file_key, local_file_path)
        logging.info("Download completed successfully.")
    except Exception as e:
        logging.error(f"Error downloading file {file_key}: {e}")

def get_date_range(start_date_str, end_date_str):
    """
    Generate a list of date strings between start_date and end_date (inclusive).
    Dates are formatted as YYYY-MM-DD.
    """
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    date_list = []
    current_date = start_date
    while current_date <= end_date:
        date_list.append(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)
    return date_list

def download_files_by_date_range(bucket_name, device_id, start_date, end_date, download_dir):
    """
    Downloads all files in the device's sensor-data folder for each date within the specified range.
    
    Parameters:
        bucket_name (str): Name of the S3 bucket.
        device_id (str): Device identifier (used as part of the S3 key).
        start_date (str): Starting date (e.g. "2025-03-01").
        end_date (str): Ending date (e.g. "2025-03-09").
        download_dir (str): Local directory where files will be downloaded.
    """
    os.makedirs(download_dir, exist_ok=True)
    date_range = get_date_range(start_date, end_date)
    
    for date_str in date_range:
        prefix = f"{device_id}/sensor-data/{date_str}"
        logging.info(f"Listing objects in bucket '{bucket_name}' with prefix '{prefix}'")
        
        try:
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        except Exception as e:
            logging.error(f"Error listing objects with prefix {prefix}: {e}")
            continue
        
        if 'Contents' not in response:
            logging.info(f"No files found for date {date_str} with prefix {prefix}")
            continue
        
        for obj in response['Contents']:
            file_key = obj['Key']
            local_file_path = os.path.join(download_dir, os.path.basename(file_key))
            try:
                logging.info(f"Downloading {file_key} to {local_file_path}...")
                s3_client.download_file(bucket_name, file_key, local_file_path)
                logging.info(f"Downloaded {file_key} successfully.")
            except Exception as e:
                logging.error(f"Error downloading file {file_key}: {e}")

def process_json_files_to_csv(directory):
    """
    Processes all JSON files in the specified directory.
    For each JSON file:
      - Reads the JSON content.
      - Removes specific keys from the data.
      - Extracts the date and sensor set ID from the filename.
      - Appends the data as a row into a CSV file named 'YYYY-MM-DD-<sensor_set_id>.csv'.
      - Deletes the JSON file after processing.
    """
    # Regex pattern to extract date and sensor set id from filename.
    # Expected filename format: "YYYY-MM-DD_00-01-18-<sensor_set_id>-sensor-data.json"
    pattern = r"(\d{4}-\d{2}-\d{2})_.*-([0-9A-Za-z]+)-sensor-data\.json"
    
    # Define keys that should be excluded from the CSV
    keys_to_exclude = ["DATA_GREENHOUSE_ID", "DATA_SENSOR_NAME", "DATA_MOBILE_NUM", "DATA_DATETIMESTAMP"]
    
    # Iterate over all files in the directory
    for filename in os.listdir(directory):
        if filename.endswith(".json"):
            json_filepath = os.path.join(directory, filename)
            try:
                with open(json_filepath, 'r') as f:
                    data = json.load(f)
                
                # Remove unwanted keys if they exist
                for key in keys_to_exclude:
                    data.pop(key, None)
                
                # Extract date and sensor set id from filename
                match = re.match(pattern, filename)
                if not match:
                    logging.error(f"Filename {filename} does not match expected pattern. Skipping file.")
                    continue
                file_date, sensor_id = match.groups()
                
                # Define CSV filename based on date and sensor set id
                csv_filename = f"{file_date}-{sensor_id}.csv"
                csv_filepath = os.path.join(directory, csv_filename)
                
                # Check if CSV file exists to decide if header is needed
                file_exists = os.path.isfile(csv_filepath)
                with open(csv_filepath, 'a', newline='') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=list(data.keys()))
                    if not file_exists:
                        writer.writeheader()
                    writer.writerow(data)
                
                logging.info(f"Processed {filename} and appended data to {csv_filename}.")
                # Delete the JSON file after processing
                os.remove(json_filepath)
                logging.info(f"Deleted JSON file {filename} after processing.")
            except Exception as e:
                logging.error(f"Error processing file {filename}: {e}")

if __name__ == "__main__":
    # Specify your parameters
    bucket_name = 'solwer-device-bucket'
    download_dir = './downloaded_json_files'
    
    # Download JSON files by date range
    device_id = '10000000837b7735'
    start_date = '2025-03-01'
    end_date = '2025-03-09'
    download_files_by_date_range(bucket_name, device_id, start_date, end_date, download_dir)
    
    # Process downloaded JSON files into CSV files and delete JSON files afterward
    process_json_files_to_csv(download_dir)
