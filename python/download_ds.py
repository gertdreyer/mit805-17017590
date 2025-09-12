import os
import requests
import boto3
from botocore.client import Config
from botocore import UNSIGNED
import sys
import re
import zipfile
import shutil
import glob

s3 = boto3.resource("s3", config=Config(signature_version=UNSIGNED))
bucket = s3.Bucket("tripdata")


def test_regexp(name):
    if re.match(r"^\d{4,6}-citibike-tripdata(\.csv)?\.zip$", name):
        return True
    return False


def list_files_in_s3():
    objs = bucket.objects.all()
    for obj in objs:
        if test_regexp(obj.key):
            yield obj.key


def download_file_from_s3(file_key, dest_path):
    bucket.download_file(file_key, dest_path)
    print(f"Downloaded {file_key} to {dest_path}")


def extract_zip(zip_path, extract_to):
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_to)
    print(f"Extracted {zip_path} to {extract_to}")


def main():
    bucket_name = os.getenv("BUCKET_NAME")
    data_dir = os.getenv("DATA_DIR", "./data")

    if not bucket_name:
        print("BUCKET_NAME environment variable not set.")
        sys.exit(1)
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    for file_key in list_files_in_s3():
        dest_path = os.path.join(data_dir, file_key)
        if os.path.exists(dest_path):
            print(f"File {dest_path} already exists. Skipping download.")
        else:
            download_file_from_s3(file_key, dest_path)
        if dest_path.endswith(".zip"):
            extract_zip(dest_path, os.path.join(data_dir, "temp"))
            # move all csv files to data_dir using globs
            for csv_file in glob.glob(
                os.path.join(data_dir, "temp", "**/*.csv"), recursive=True
            ):
                shutil.move(csv_file, data_dir)
            shutil.rmtree(os.path.join(data_dir, "temp"))


if __name__ == "__main__":
    main()
