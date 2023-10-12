import pandas as pd
import os
import json
import boto3
import logging
import subprocess
from xml.etree import ElementTree as ET
import urllib.parse
import argparse
import urllib.parse
import re

# Logging configuration
logging.basicConfig(filename='process_logs.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
MP4_DIR = "/home/admin/scripts/mp4s/good-mp4s"
ISM_OUTPUT_DIR = "/home/admin/scripts/ism/output"
LICENSE_KEY_PATH = "/home/admin/scripts/mp4s/usp-license.key"
UPLOAD_BUCKET_NAME = "prod-webmd-usp-content-1"
DOWNLOAD_BUCKET_NAME = "prod-webmd-usp-content-1"

# Configure boto3 for Linode Object Storage
download_session = boto3.Session(
    aws_access_key_id=os.environ['S3_ACCESS_KEY'],
    aws_secret_access_key=os.environ['S3_SECRET_KEY']
)
download_client = download_session.client('s3', region_name='us-ord-1', endpoint_url='https://us-ord-1.linodeobjects.com')

# The function is repeated in case it is being downloaded from one bucket, and then upload to a different one.
# Remember to updat the variables for both buckets.
upload_session = boto3.Session(
    aws_access_key_id=os.environ['S3_ACCESS_KEY'],
    aws_secret_access_key=os.environ['S3_SECRET_KEY']
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
    encoded_filename = urllib.parse.quote(decoded_filename, safe='+')
    # Return the new URL with the directory and re-encoded filename
    return os.path.join(directory, encoded_filename)

def create_csv_from_json(json_file):
    with open(json_file, 'r') as f:
        data = json.load(f)
        df = pd.json_normalize(data)
        csv_path = "output.csv"
        df.to_csv(csv_path, index=False)
        return csv_path

def download_from_linode(file_key, local_path):
    # Use the decode and reencode function to process URL
    encoded_file_key = decode_and_reencode_filename(file_key)
    try:
        download_client.download_file(DOWNLOAD_BUCKET_NAME, encoded_file_key, local_path)
        logging.info(f"Successfully downloaded {file_key} to {local_path}")
    except Exception as e:
        logging.error(f"Error downloading {file_key} {encoded_file_key} {DOWNLOAD_BUCKET_NAME}. Reason: {e}")


def generate_ism_filename_from_mp4(mp4_filename):
    # Use regex to match everything up to the last underscore
    match = re.match(r"^(.*)_.*\.mp4$", mp4_filename)
    if match:
        base_name = match.group(1)
        return f"{base_name}.ism"
    return None

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

def modify_ism_to_relative_path(ism_filename):
    ism_file_path = os.path.join(ISM_OUTPUT_DIR, ism_filename)
    tree = ET.parse(ism_file_path)
    root = tree.getroot()

    # Remove the namespaces from the XML file
    for elem in root.iter():
        if not hasattr(elem.tag, 'find'): continue  # Skip if it's not an element (like a comment)
        i = elem.tag.find('}')
        if i >= 0:
            elem.tag = elem.tag[i+1:]

    for audio in root.findall(".//audio"):
        src = audio.get('src')
        if src:
            # Decoding the URL to make it human-readable
            decoded_src = urllib.parse.unquote(src)
            audio.set('src', os.path.basename(decoded_src))

    for video in root.findall(".//video"):
        src = video.get('src')
        if src:
            # Decoding the URL to make it human-readable
            decoded_src = urllib.parse.unquote(src)
            video.set('src', os.path.basename(decoded_src))

    tree.write(ism_file_path, xml_declaration=True)


def generate_upload_path(mp4_path, ism_filename):
    dir_path = os.path.dirname(mp4_path)
    return os.path.join(dir_path, ism_filename)


def upload_to_linode(file_key, local_path):
    try:
        download_client.upload_file(local_path, UPLOAD_BUCKET_NAME, file_key)
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
        download_from_linode(row['Path'], local_path)
        mp4_local_paths.append(local_path)

    # 2. Use these MP4s to generate a single ISM file.
    ism_generated = generate_ism(mp4_local_paths)

    if ism_generated:
        # Get the correct ISM filename from the first downloaded MP4.
        ism_filename = generate_ism_filename_from_mp4(os.path.basename(mp4_local_paths[0]))

        # Ensure we have the filename, else skip the iteration.
        if not ism_filename:
            logging.error(f"Couldn't derive ISM filename for {mp4_local_paths[0]}")
            return

        # Modify the ISM to set relative paths.
        modify_ism_to_relative_path(ism_filename)

        # 3. Upload the ISM file
        first_mp4_path = group_df.iloc[0]['Path']  # Get the path of the first mp4 from the dataframe
        upload_path = generate_upload_path(first_mp4_path, ism_filename)  # Derive the upload path
        upload_to_linode(upload_path, os.path.join(ISM_OUTPUT_DIR, ism_filename))

    # Cleanup
    clean_directory(MP4_DIR)

def main():
    json_file = "partial_special_characters.json"
    csv_path = create_csv_from_json(json_file)

    df = pd.read_csv(csv_path)

    # Get unique keys from the paths
    df['key'] = df['Path'].apply(get_key_from_mp4_path)
    unique_keys = df['key'].unique()

    for key in unique_keys:
        group_df = df[df['key'] == key]
        process_single_key_group(group_df)

if __name__ == "__main__":
    main()
