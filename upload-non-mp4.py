import argparse
import pandas as pd
import json
import os
import requests
import subprocess
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote
import gc
import shutil


def create_csv(json_file):
    with open(json_file) as data:
        json_data = json.load(data)
        filtered_data = [entry for entry in json_data if not entry.get("Name", "").endswith(".mp4")]
        df = pd.json_normalize(filtered_data)
        print("Printing JSON to CSV")
        print(df)
        df.to_csv("missing_flattened_output.csv", index=False)
        del df
        gc.collect()
        return "missing_flattened_output.csv"

def split_csv(input_csv, output_dir, chunk_size):
    os.makedirs(output_dir, exist_ok=True)
    for i, chunk in enumerate(pd.read_csv(input_csv, chunksize=chunk_size)):
        chunk.to_csv(os.path.join(output_dir, f"output_{i}.csv"), index=False)
        del chunk
        gc.collect()


def get_details_from_csv(csv_file):
    df = pd.read_csv(csv_file, usecols=["Name", "Path"])
    names, paths = df["Name"], df["Path"]
    del df
    gc.collect()
    return names, paths


def download_file(base_url, path, download_dir, error_dir):
    download_errors_file = os.path.join(error_dir, "download_errors")
    session = requests.Session()
    file_name = os.path.basename(path)
    encoded_path = os.path.join(os.path.dirname(path), quote(file_name))  # encoding only the file name
    full_url = f"{base_url}/{encoded_path}"
    try:
        response = session.get(full_url)
        if response.status_code == 200:
            print(f"Successfully downloaded: {file_name}")
            download_path = os.path.join(download_dir, file_name)
            with open(download_path, 'wb') as file:
                file.write(response.content)
            print(f"Saved {file_name} to {download_path}")
        else:
            print(f"Failed to download: {file_name}, Status code: {response.status_code}")
            with open(download_errors_file, 'a') as err_file:
                err_file.write(f"Failed to download: {file_name}, Status code: {response.status_code}\n")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Request for {file_name} failed: {e}")
        with open(download_errors_file, 'a') as err_file:
            err_file.write(f"Request for {file_name} failed: {e}\n")
        return False


def make_http_requests(base_url, paths, download_dir, error_dir, max_requests=10):
    with ThreadPoolExecutor(max_workers=max_requests) as executor:
        results = list(executor.map(download_file, [base_url] * len(paths), paths, [download_dir] * len(paths),
                                    [error_dir] * len(paths)))



def upload_to_linode_object_storage(source_dir, linode_remote, error_dir, csv_file, names):
    linode_errors_file = os.path.join(error_dir, "linode_errors")
    try:
        for name, path in zip(names, paths):
            if not name.endswith(".mp4"):
                source_file_path = os.path.join(source_dir, name)
                remote_file_path = os.path.join(path)
                upload_path = os.path.dirname(remote_file_path)
                upload_command = ["rclone", "copy", "--progress", "--bwlimit", "150M", "--s3-acl", "authenticated-read",
                                  source_file_path, f"{linode_remote}{upload_path}"]
                subprocess.run(upload_command, check=True)
                print(f"Upload to Linode Object Storage successful for: {name} {upload_path} {linode_remote}")
    except subprocess.CalledProcessError as e:
        print(f"Upload to Linode Object Storage failed. Error: {e}")
        with open(linode_errors_file, 'a') as err_file:
            err_file.write(f"Upload to Linode Object Storage failed. Error: {e}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process smaller CSVs in a specified range")
    parser.add_argument("--generate-csvs", action="store_true", help="Generate CSVs and exit")
    parser.add_argument("--start", type=int, help="Start index of CSV range")
    parser.add_argument("--end", type=int, help="End index of CSV range")
    args = parser.parse_args()

    license_key_path = "/home/admin/scripts/mp4s/usp-license.key"
    linode_remote = "your-bucket-prod-chicago:prod-your-bucket-usp-content-1/delivery/"

    if args.generate_csvs:
        json_file_name = "missing_files_linode.json"
        create_csv(json_file_name)
        chunk_size = 75
        output_dir = "missing_smaller_csvs"
        split_csv("missing_flattened_output.csv", output_dir, chunk_size)
        print("CSV generation and splitting completed.")
    else:
        if args.start is None or args.end is None:
            print("Error: Both --start and --end arguments are required for processing CSVs.")
        else:
            base_url = "https://your-host-a.akamaihd.net/delivery/"
            smaller_csv_files = sorted([f for f in os.listdir("missing_smaller_csvs") if f.endswith('.csv')])
            download_dir = "bad-mp4s"
            os.makedirs(download_dir, exist_ok=True)
            error_dir = "errors"
            os.makedirs(error_dir, exist_ok=True)
            smaller_csv_files.sort(key=lambda x: int(x.split('_')[-1].split('.')[0]))

            if args.start < 0 or args.start >= len(smaller_csv_files) or args.end < 0 or args.end >= len(smaller_csv_files):
                print("Invalid start and/or end index. Please provide valid indices.")
            else:
                for i in range(args.start, args.end + 1):
                    csv_file = smaller_csv_files[i]
                    print(csv_file)
                    csv_file_path = os.path.join("missing_smaller_csvs", csv_file)

                    names, paths = get_details_from_csv(csv_file_path)

                    make_http_requests(base_url, paths, download_dir, error_dir)
                    # Assuming you still want to run the mp4split function, though it's not provided in the recent code snippet.
                    # run_mp4split(download_dir, "good-mp4s", names, license_key_path, error_dir)
                    upload_to_linode_object_storage("bad-mp4s", linode_remote, error_dir, csv_file_path, names)

                # Clearing the bad-mp4s and good-mp4s directories
                #clear_directory("bad-mp4s")
                #clear_directory("good-mp4s")
