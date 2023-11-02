import json
import requests
from concurrent.futures import ThreadPoolExecutor
import os
import boto3

# Constants
JSON_FILE = 'all_files_linode.json'
#JSON_FILE = 'test.json'
LOG_FILE = 'error_log.json'
BASE_URL = 'http://us-ord-1.linodeobjects.com'  # Add your base URL here
MAX_WORKERS = 40  # You can adjust this number based on your server's capabilities and requirements
BUCKET_NAME = 'bucket-content-1'


# AWS Configuration
REGION = os.environ.get('LREGION')  # adjust to your region
ACCESS_KEY = os.environ.get('S3_ACCESS_KEY')
SECRET_KEY = os.environ.get('S3_SECRET_KEY')

session = boto3.Session(aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY, region_name=REGION)
s3 = session.client('s3', region_name=REGION, endpoint_url=BASE_URL)  # if you have a custom endpoint

# Use a session for connection pooling
http_session = requests.Session()

def generate_presigned_url(path):
    try:
        url = s3.generate_presigned_url(
            'head_object',
            Params={'Bucket': BUCKET_NAME, 'Key': path},
            ExpiresIn=300,
            HttpMethod='GET'
        )
        return url
    except Exception as e:
        print(f"Error generating signed URL for path: {path}. Error: {str(e)}")
        return None

def check_url(entry, errors):
    signed_url = generate_presigned_url(entry["Path"])
    if not signed_url:
        return

    try:
        # Stream the content
        with http_session.get(signed_url, stream=True) as response:
            # Check the status without reading the content
            if response.status_code == 200:
                print(f"URL: {signed_url} - 200 OK Successful")
                # Discard the content by streaming in small chunks
                for _ in response.iter_content(chunk_size=8192):
                    pass
            elif response.status_code in (404, 429, 403, 501, 502, 503, 504):
                errors.append(entry)
                print(f"Error {response.status_code} for path: {entry['Path']}")
            else:
                errors.append(entry)
                print(f"Unexpected status {response.status_code} for path: {entry['Path']}")
    except Exception as e:
        errors.append(entry)
        print(f"Error for path: {entry['Path']}. Error: {str(e)}")


def main():
    with open(JSON_FILE, 'r') as f:
        data = json.load(f)

    # Read pre-existing errors
    try:
        with open(LOG_FILE, 'r') as f:
            errors = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        errors = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        executor.map(lambda entry: check_url(entry, errors), data)

    # Write errors back to log file
    with open(LOG_FILE, 'w') as f:
        json.dump(errors, f, indent=4)

if __name__ == "__main__":
    main()
