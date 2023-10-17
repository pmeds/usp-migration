import pandas as pd
import os
import json
import subprocess
from xml.etree import ElementTree as ET
import urllib.parse
import re
import requests
import logging
import boto3

# Logging configuration
logging.basicConfig(filename='process_logs.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
MP4_DIR = "/home/admin/scripts/mp4s/bad-mp4s"
GOOD_MP4_DIR = "/home/admin/scripts/mp4s/good-mp4s"
ISM_OUTPUT_DIR = "/home/admin/scripts/ism/output"
LICENSE_KEY_PATH = "/home/admin/scripts/mp4s/usp-license.key"
BASE_URL = "https://webmd-a.akamaihd.net/delivery/"
UPLOAD_BUCKET_NAME = "webmd-usp-poc-content-1"

upload_session = boto3.Session(
    aws_access_key_id=os.environ['POC_S3_ACCESS_KEY'],
    aws_secret_access_key=os.environ['POC_S3_SECRET_KEY']
)
upload_client = upload_session.client('s3', region_name='us-ord-1', endpoint_url='https://us-ord-1.linodeobjects.com')

def decode_and_reencode_filename(url):
    # Split the URL to get the path
    path = urllib.parse.urlparse(url).path
    # Split the path to get directory and filename
    directory, filename = os.path.split(path)
    # Decode the filename
    decoded_filename = urllib.parse.unquote(filename)
    # Re-encode the filename
    encoded_filename = urllib.parse.quote(decoded_filename, safe=' +')
    # Return the new URL with the directory and re-encoded filename
    return os.path.join(directory, encoded_filename)

def create_csv_from_json(json_file):
    with open(json_file, 'r') as f:
        data = json.load(f)
        filtered_data = [entry for entry in data if entry.get("Name", "").endswith(".mp4")]
        df = pd.json_normalize(filtered_data)
        csv_path = "output.csv"
        df.to_csv(csv_path, index=False)
        return csv_path

def download_from_akamai(file_key, local_path):
    encoded_file_key = decode_and_reencode_filename(file_key)
    full_url = BASE_URL + encoded_file_key
    try:
        response = requests.get(full_url)
        print(f"Downloading file {full_url}")
        if response.status_code == 200:
            with open(local_path, 'wb') as file:
                file.write(response.content)
            logging.info(f"Successfully downloaded {file_key} to {local_path}")
        else:
            logging.error(f"Failed to download {file_key}. Status code: {response.status_code}")
    except Exception as e:
        logging.error(f"Error downloading {file_key}. Reason: {e}")

def generate_ism_filename_from_mp4(mp4_filename):
    # Use regex to match everything up to the last underscore
    match = re.match(r"^(.*)_.*\.mp4$", mp4_filename)
    if match:
        base_name = match.group(1)
        return f"{base_name}.ism"
    return None

def run_mp4split(input_file_path, output_file_path, license_key_path):
    #print(input_file_path)
    #print(output_file_path)
    mp4split_command = ["mp4split", f"--license-key={license_key_path}", "-o", output_file_path, input_file_path]
    try:
        subprocess.run(mp4split_command, check=True)
        logging.info(f"mp4split successful for: {os.path.basename(input_file_path)}")
        print(f"mp4split successful for: {os.path.basename(input_file_path)}")
        print(f"uploading for: {os.path.basename(output_file_path)}")
        upload_mp4_to_linode_boto3(output_file_path, os.path.basename(output_file_path))
    except subprocess.CalledProcessError as e:
        logging.error(f"mp4split failed for: {os.path.basename(input_file_path)}, Error: {e}")

def generate_ism(mp4_local_paths):
    # Extract the ISM filename from the first MP4 name
    ism_filename = generate_ism_filename_from_mp4(os.path.basename(mp4_local_paths[0]))
    if not ism_filename:
        logging.error(f"Couldn't generate ISM filename for {mp4_local_paths[0]}")
        return False
    cmd = [
        "mp4split",
        f"--license-key={LICENSE_KEY_PATH}",
        "-o",
        os.path.join(ISM_OUTPUT_DIR, ism_filename)
    ] + mp4_local_paths

    try:
        subprocess.run(cmd, check=True)
        return True
    except subprocess.CalledProcessError:
        logging.error(f"ISM generation failed for {mp4_local_paths[0]}")
        return False

def generate_upload_path(mp4_path, ism_filename):
    dir_path = os.path.dirname(mp4_path)
    return os.path.join(dir_path, ism_filename)

def upload_mp4_to_linode_boto3(file_key, local_path):
    try:
        with open(local_path, 'rb') as file:
            upload_client.upload_fileobj(file, UPLOAD_BUCKET_NAME, file_key)
        logging.info(f"Successfully uploaded {local_path} to {file_key}")
    except Exception as e:
        logging.error(f"Error uploading {local_path}. Reason: {e}")

def clean_directory(directory_path):
    print("Cleaning good-mp4s directory")
    for filename in os.listdir(directory_path):
        os.remove(os.path.join(directory_path, filename))

def get_key_from_mp4_path(path):
    """
    Derive the key from the mp4 path by stripping everything after the last underscore
    """
    return re.sub(r"_.*\.mp4$", "", path)

def process_single_key_group(group_df):
    """
    Process a group of rows from the CSV that share the same key.
    """
    # 1. Download all MP4s listed in the group.
    mp4_local_paths = []
    for _, row in group_df.iterrows():
        local_path = os.path.join(MP4_DIR, os.path.basename(row['Path']))
        download_from_akamai(row['Path'], local_path)
        mp4_local_paths.append(local_path)

    # 2. Run mp4split for each downloaded MP4 and move them to good-mp4s directory
    for input_file_path in mp4_local_paths:
        output_file_path = os.path.join(GOOD_MP4_DIR, os.path.basename(input_file_path))
        run_mp4split(input_file_path, output_file_path, LICENSE_KEY_PATH)

    # 3. Generate ISM after mp4split
    for input_mp4_path in mp4_local_paths:
        generate_ism([input_mp4_path])

    # Cleanup
    #clean_directory(MP4_DIR)

def main():
    json_file = "partial-missing-mp4s.json"
    csv_path = create_csv_from_json(json_file)

    df = pd.read_csv(csv_path)

    # Get unique keys from the paths
    df['key'] = df['Path'].apply(get_key_from_mp4_path)
    unique_keys = df['key'].unique()

    for key in unique_keys:
        group_df = df[df['key'] == key]

        # 1. Download MP4 files from Akamai
        mp4_local_paths = []
        for _, row in group_df.iterrows():
            local_path = os.path.join(MP4_DIR, os.path.basename(row['Path']))
            download_from_akamai(row['Path'], local_path)
            mp4_local_paths.append(local_path)

        # 2. Run mp4split for each downloaded MP4 and move them to good-mp4s directory
        for input_file_path in mp4_local_paths:
            output_file_path = os.path.join(GOOD_MP4_DIR, os.path.basename(input_file_path))
            run_mp4split(input_file_path, output_file_path, LICENSE_KEY_PATH)

        # 3. Generate ISM after mp4split
        generate_ism(mp4_local_paths)

        # 4. Process the group of MP4 files
        process_single_key_group(group_df)

if __name__ == "__main__":
    main()
