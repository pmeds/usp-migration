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
BASE_URL = "https://your-hostname.com/delivery/"
UPLOAD_BUCKET_NAME = "your-bucket-usp-content-1"
BASE_PATH = "base_path/"

# Create session to upload content to linode bucket prod-webmd-usp-content-1
upload_session = boto3.Session(
    aws_access_key_id=os.environ['S3_ACCESS_KEY'],
    aws_secret_access_key=os.environ['S3_SECRET_KEY']
)
upload_client = upload_session.client('s3', region_name='us-ord-1', endpoint_url='https://us-ord-1.linodeobjects.com')

# Function to extract the S3 upload path from CSV data
def extract_upload_path(csv_row):
    # Assuming that 'Path' in the CSV contains the full S3 path
    return csv_row['Path']

# Function to encode and reencode the URLs to avoid having issues when generating the Object Store authorization headers
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

# Function to create the csv file from th json file
def create_csv_from_json(json_file):
    with open(json_file, 'r') as f:
        data = json.load(f)
        filtered_data = [entry for entry in data if entry.get("Name", "").endswith(".mp4")]
        df = pd.json_normalize(filtered_data)
        csv_path = "output.csv"
        df.to_csv(csv_path, index=False)
        return csv_path

# function to download content from netstorage
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

# Function to extract the mp4 filename
def generate_ism_filename_from_mp4(mp4_filename):
    # Use regex to match everything up to the last underscore
    match = re.match(r"^(.*)_.*\.mp4$", mp4_filename)
    if match:
        base_name = match.group(1)
        return f"{base_name}.ism"
    return None

# Function to repackage the mp4s that were donwloaded previously
def run_mp4split(input_file_path, output_file_path, license_key_path):
    #print(input_file_path)
    #print(output_file_path)
    mp4split_command = ["mp4split", f"--license-key={license_key_path}", "-o", output_file_path, input_file_path]
    try:
        subprocess.run(mp4split_command, check=True)
        logging.info(f"mp4split successful for: {os.path.basename(input_file_path)}")
        print(f"mp4split successful for: {os.path.basename(input_file_path)}")
    except subprocess.CalledProcessError as e:
        logging.error(f"mp4split failed for: {os.path.basename(input_file_path)}, Error: {e}")

# Function to generate the ism file using the mp4s stored in GOOD_MP4_DIR
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

# Function to modify the generated ism file. The ism file will grab the path that is used from the output and will make it the path when the url is being called. This
# To avoid this from happening the ism file has to be modified so it only includes the file name as a relative path. As long as the mp4s are stored in object store 
# within the same directory this will work
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

# Function to generate the ism upload path
def generate_upload_path(mp4_path, ism_filename):
    dir_path = os.path.dirname(mp4_path)
    return os.path.join(BASE_PATH, dir_path, ism_filename)

# Function to upload the mp4 files to linode
def upload_mp4_to_linode_boto3(file_key, local_path):
    print(f"Trying to upload {file_key} {local_path}")
    try:
        with open(local_path, 'rb') as file:
            upload_client.upload_fileobj(file, UPLOAD_BUCKET_NAME, file_key, ExtraArgs={'ACL': 'authenticated-read'})
        logging.info(f"Successfully uploaded {local_path} to {file_key}")
    except Exception as e:
        logging.error(f"Error uploading {local_path}. Reason: {e}")

# Function to generate the mp4 upload path
def generate_mp4_upload_path(local_file_path, s3_upload_path):
    """
    Generate the correct upload path for Linode based on the local_file_path and the s3_upload_path.
    """
    clean_file_name = os.path.basename(local_file_path)  # Extract the file name from the local path
    # Remove the file name from the s3_upload_path
    s3_directory_path = os.path.dirname(s3_upload_path)
    # Construct the new upload path by appending the clean_file_name
    linode_upload_path = os.path.join(BASE_PATH, s3_directory_path, clean_file_name)
    return linode_upload_path

# Function to upload the ism file to linode
def upload_to_linode(file_key, local_path):
    #print(local_path)
    print(file_key)
    if os.path.exists(local_path):
        try:
            upload_client.upload_file(local_path, UPLOAD_BUCKET_NAME, file_key, ExtraArgs={'ACL': 'authenticated-read'})
            logging.info(f"Successfully uploaded {local_path} to {file_key}")
        except Exception as e:
            logging.error(f"Error uploading {local_path}. Reason: {e}")
    else:
        logging.error(f"upload_to_linode_ File not found: {local_path}")

# Function to delete the mp4 files 
def clean_directory(directory_path):
    print("Cleaning good-mp4s directory")
    for filename in os.listdir(directory_path):
        os.remove(os.path.join(directory_path, filename))

# Function to generate the key to group all files
def get_key_from_mp4_path(path):
    """
    Derive the key from the mp4 path by stripping everything after the last underscore
    """
    return re.sub(r"_.*\.mp4$", "", path)

# Function to proces one key at a time
def process_single_key_group(group_df, s3_upload_path):
    """
    Process a group of rows from the CSV that share the same key.
    """
    # Create a list to store the downloaded MP4 files
    mp4_local_paths = []

    for _, row in group_df.iterrows():
        local_path = os.path.join(MP4_DIR, os.path.basename(row['Path']))
        
        # Check if the MP4 file has already been downloaded
        if not os.path.exists(local_path):
            download_from_akamai(row['Path'], local_path)
        
        mp4_local_paths.append(local_path)

    # Generate ISM filename once for the group
    ism_filename = generate_ism_filename_from_mp4(os.path.basename(mp4_local_paths[0]))

    # Run mp4split for each downloaded MP4 and move them to good-mp4s directory
    for input_file_path in mp4_local_paths:
        output_file_path = os.path.join(GOOD_MP4_DIR, os.path.basename(input_file_path))
        run_mp4split(input_file_path, output_file_path, LICENSE_KEY_PATH)
        # Add BASE_PATH to s3_upload_path
        #mp4_linode_upload_path = os.path.join(BASE_PATH, s3_upload_path, output_file_path) 
        

        # Extract the file name from the local path
        clean_file_name = os.path.basename(output_file_path)
        # Generate the correct upload path for Linode
        linode_upload_path = generate_mp4_upload_path(output_file_path, s3_upload_path)
        print(linode_upload_path) 
        # Upload the file
        upload_mp4_to_linode_boto3(linode_upload_path, output_file_path)
        
    

    # Generate ISM after mp4split
    generate_ism(mp4_local_paths)
    if generate_ism:
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

    # Cleanup. Remove the old mp4s.
    clean_directory(MP4_DIR)


def main():
    json_file = "mising-mp4s.json"
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
            # Update s3_upload_path for each row (mp4 file)
            s3_upload_path = extract_upload_path(row)
            local_path = os.path.join(MP4_DIR, os.path.basename(row['Path']))
            download_from_akamai(row['Path'], local_path)
            mp4_local_paths.append(local_path)

        # 2. Run mp4split for each downloaded MP4 and move them to good-mp4s directory
        for input_file_path in mp4_local_paths:
            output_file_path = os.path.join(GOOD_MP4_DIR, os.path.basename(input_file_path))
            run_mp4split(input_file_path, output_file_path, LICENSE_KEY_PATH)

        # 3. Process the group of MP4 files
        process_single_key_group(group_df, s3_upload_path)

if __name__ == "__main__":
    main()
