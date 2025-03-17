import os
import boto3
import logging
import csv
import json
import re
from datetime import datetime, timedelta
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
default_region_name = os.getenv('DEFAULT_REGION_NAME')

session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=default_region_name
)

s3_client = session.client('s3')

def download_specific_file(bucket_name, file_key, download_dir):
    os.makedirs(download_dir, exist_ok=True)
    local_file_path = os.path.join(download_dir, os.path.basename(file_key))
    
    try:
        logging.info(f"Downloading {file_key} to {local_file_path}...")
        s3_client.download_file(bucket_name, file_key, local_file_path)
        logging.info("Download completed successfully.")
    except Exception as e:
        logging.error(f"Error downloading file {file_key}: {e}")

def get_date_range(start_date_str, end_date_str):
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    date_list = []
    current_date = start_date
    
    while current_date <= end_date:
        date_list.append(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)
    return date_list

def download_files_by_date_range(bucket_name, device_id, start_date, end_date, download_dir):
    os.makedirs(download_dir, exist_ok=True)
    date_range = get_date_range(start_date, end_date)
    
    for date_str in date_range:
        folder_name = os.getenv('FOLDER_NAME')
        prefix = f"{device_id}/{folder_name}/{date_str}"
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
    # Expected filename format: "YYYY-MM-DD_00-01-18-<sensor_set_id>-sensor-data.json"
    pattern = r"(\d{4}-\d{2}-\d{2})_.*-([0-9A-Za-z]+)-sensor-data\.json"
    
    keys_to_exclude = ["DATA_GREENHOUSE_ID", "DATA_SENSOR_NAME", "DATA_MOBILE_NUM", "DATA_DATETIMESTAMP"]
    for filename in os.listdir(directory):
        if filename.endswith(".json"):
            json_filepath = os.path.join(directory, filename)
            try:
                with open(json_filepath, 'r') as f:
                    data = json.load(f)
                
                for key in keys_to_exclude:
                    data.pop(key, None)
                
                match = re.match(pattern, filename)
                if not match:
                    logging.error(f"Filename {filename} does not match expected pattern. Skipping file.")
                    continue
                file_date, sensor_id = match.groups()
                
                csv_filename = f"{file_date}-{sensor_id}.csv"
                csv_filepath = os.path.join(directory, csv_filename)
                
                file_exists = os.path.isfile(csv_filepath)
                with open(csv_filepath, 'a', newline='') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=list(data.keys()))
                    if not file_exists:
                        writer.writeheader()
                    writer.writerow(data)
                
                logging.info(f"Processed {filename} and appended data to {csv_filename}.")
                os.remove(json_filepath)
                logging.info(f"Deleted JSON file {filename} after processing.")
            except Exception as e:
                logging.error(f"Error processing file {filename}: {e}")

def validate(date_text):
    try:
        datetime.fromisoformat(date_text)
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY-MM-DD")
        
if __name__ == "__main__":
    bucket_name = 'solwer-device-bucket'
    download_dir = './downloaded_json_files'
    
    device_id = os.getenv('DEVICE_ID')
    print("Enter start date (YYYY-MM-DD):")
    start_date = input()
    validate(start_date)
    print("Enter end date (YYYY-MM-DD):")
    end_date = input()
    validate(end_date)
    
    download_files_by_date_range(bucket_name, device_id, start_date, end_date, download_dir)
    process_json_files_to_csv(download_dir)