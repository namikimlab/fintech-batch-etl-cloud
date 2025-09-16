import os
import boto3
import pyarrow.parquet as pq
import argparse
from urllib.parse import urlparse

def validate_parquet_files(s3_bucket, s3_prefix):
    """
    Validates Parquet files in a specified S3 location by trying to read them.
    Deletes corrupted files and their checksums upon failure.
    """
    s3 = boto3.client('s3')

    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix)

    for page in pages:
        for obj in page.get('Contents', []):
            file_key = obj['Key']
            # Only validate the .parquet files and ignore the _SUCCESS marker
            if file_key.endswith('.parquet') and not file_key.endswith('_SUCCESS'):
                print(f"Validating file: {file_key}")
                try:
                    # PyArrow can read directly from S3, which is more efficient
                    # than downloading to a temporary file.
                    s3_path = f"s3://{s3_bucket}/{file_key}"
                    pq.read_table(s3_path)
                    print(f"File {file_key} is valid.")

                except Exception as e:
                    print(f"Error: File {file_key} is corrupted.")
                    print(f"Details: {e}")
                    
                    # Remove corrupted file and its checksum
                    print(f"Attempting to delete corrupted file and its checksum from S3.")
                    s3.delete_object(Bucket=s3_bucket, Key=file_key)
                    
                    # The .crc file is not always present, so we'll just try to delete it
                    crc_key = file_key + '.crc'
                    try:
                        s3.delete_object(Bucket=s3_bucket, Key=crc_key)
                    except Exception as crc_e:
                        print(f"Could not delete checksum file {crc_key}. It may not exist.")
                        
                    # Raise an exception to fail the Airflow task
                    raise Exception(f"Validation failed for {file_key}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate Parquet files in an S3 path.")
    parser.add_argument(
        '--s3-path', 
        required=True, 
        help='Full S3 path to the directory containing Parquet files (e.g., s3://bucket/path/)'
    )
    args = parser.parse_args()

    # Parse the S3 URI to get the bucket name and prefix
    parsed_s3_path = urlparse(args.s3_path)
    bucket_name = parsed_s3_path.netloc
    prefix = parsed_s3_path.path.lstrip('/')
    
    validate_parquet_files(bucket_name, prefix)