import subprocess
import os
import json
from xml.etree import ElementTree as ET
import logging
import time
import urllib.parse

# Set up logging
logging.basicConfig(filename='ism-generation-errors.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')
success_log = open("success.log", "a")

# Access environment variables
lregion = os.environ.get('LREGION')
s3AccessKey = os.environ.get('S3_ACCESS_KEY')
s3SecretKey = os.environ.get('S3_SECRET_KEY')

# Check if environment variables are present
if not lregion or not s3AccessKey or not s3SecretKey:
    raise ValueError("Please set LREGION, S3_ACCESS_KEY, and S3_SECRET_KEY environment variables.")

OUTPUT_DIR = "/home/admin/scripts/ism/output"

def decode_and_normalize(filename):
    """Decode URLs and replace special characters."""
    decoded_name = urllib.parse.unquote(filename)
    normalized_name = decoded_name.replace('+', ' ')
    return normalized_name

def get_file_group_key(file_name):
    """Extracts the grouping key for the filename."""
    normalized_name = decode_and_normalize(os.path.basename(file_name))
    underscore_position = normalized_name.rfind('_', 0, normalized_name.rfind('k.mp4'))
    return normalized_name[:underscore_position] if underscore_position != -1 else normalized_name

def crawl_linode_bucket(json_name):
    with open(json_name) as data:
        json_data = json.load(data)
        return json_data

def generate_manifest(file_group, command_filename, json_entry):
    url_appended_file_group = [f"https://prod-webmd-usp-content-1.us-ord-1.linodeobjects.com/{file_path}" for file_path in file_group]
    output_path = os.path.join(OUTPUT_DIR, command_filename)
    cmd = ["mp4split", f"--license-key=/home/admin/scripts/mp4s/usp-license.key", f"--s3_access_key={s3AccessKey}", f"--s3_secret_key={s3SecretKey}", f"--s3_region={lregion}", "-o", output_path] + url_appended_file_group
    try:
        subprocess.run(cmd, check=True)
        success_log.write(json.dumps(json_entry) + "\n")
    except subprocess.CalledProcessError:
        logging.error(json.dumps(json_entry))

def serialize_without_ns(element):
    """Serializes the XML element without namespace prefixes."""
    from xml.etree.ElementTree import tostring
    from xml.dom.minidom import parseString

    rough_string = tostring(element).decode()
    reparsed = parseString(rough_string)
    for node in reparsed.getElementsByTagName('*'):
        node.tagName = node.tagName.split(':')[-1]
        node.nodeName = node.nodeName.split(':')[-1]
    return reparsed.toxml()

def modify_ism_to_relative_path(ism_filename):
    ism_file_path = os.path.join(OUTPUT_DIR, ism_filename)
    tree = ET.parse(ism_file_path)
    root = tree.getroot()

    # Function to update src attribute
    def update_src(element):
        src = element.get('src')
        if src:
            new_src = os.path.basename(src).replace('%20', ' ')
            element.set('src', new_src)

    # Find the <audio> and <video> tags and extract the file names
    for audio in root.findall(".//{http://www.w3.org/2001/SMIL20/Language}audio"):
        update_src(audio)

    for video in root.findall(".//{http://www.w3.org/2001/SMIL20/Language}video"):
        update_src(video)

    # Save the modified XML content back to the same .ism file without namespace prefixes
    with open(ism_file_path, 'w') as f:
        xml_str = serialize_without_ns(root)
        f.write(xml_str)

def upload_manifest_to_bucket(filename_prefix, mp4_files, bucket_name):
    command_filename = f"{OUTPUT_DIR}/{filename_prefix}.ism"
    common_prefix = os.path.commonprefix(mp4_files).rstrip("_")
    ism_filename = f"{common_prefix}.ism"
    destination_path = os.path.join(bucket_name, ism_filename)
    ism_cmd = ["rclone", "copyto", "--progress", command_filename, f"webmd-prod-chicago:{destination_path}"]
    
    logging.info(f"Attempting to upload using the command: {' '.join(ism_cmd)}")  # Add this to log the command
    
    try:
        subprocess.run(ism_cmd, check=True)
    except subprocess.CalledProcessError as e:
        logging.error(f"Error while uploading {command_filename} to {destination_path}. Command: {' '.join(ism_cmd)}. Error: {e.output}")

def process_files_in_parallel(filename_prefix, mp4_files, json_entry):
    command_filename = f"{filename_prefix}.ism"
    try:
        generate_manifest(mp4_files, command_filename, json_entry)
    except Exception as e:
        logging.error(f"Error during manifest generation for {filename_prefix}: {str(e)}")
        return  # Exit the function if generate_manifest fails

    try:
        modify_ism_to_relative_path(command_filename)
    except Exception as e:
        logging.error(f"Error during ISM modification for {filename_prefix}: {str(e)}")
        return  # Exit the function if modify_ism_to_relative_path fails

    try:
        upload_manifest_to_bucket(filename_prefix, mp4_files, s3_bucket_name)
    except Exception as e:
        logging.error(f"Error during manifest upload for {filename_prefix}: {str(e)}")

if __name__ == "__main__":
    json_file = "minus-modified_new_all_files_linode.json"
    s3_bucket_name = "prod-webmd-usp-content-1"

    all_entries = crawl_linode_bucket(json_file)
    file_names = [entry['Path'] for entry in all_entries if entry['Path'].endswith(".mp4")]

    file_groups = {}
    json_entries = {}

    for entry in all_entries:
        file_name = entry["Path"]
        if file_name.endswith('.mp4'):
            filename_prefix = get_file_group_key(file_name)
            if filename_prefix not in file_groups:
                file_groups[filename_prefix] = []
            file_groups[filename_prefix].append(file_name)
            json_entries[filename_prefix] = entry

    for filename_prefix, mp4_files in file_groups.items():
        process_files_in_parallel(filename_prefix, mp4_files, json_entries[filename_prefix])
