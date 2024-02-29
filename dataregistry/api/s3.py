import json

import boto3
import os

S3_REGION = 'us-east-1'
BASE_BUCKET = os.environ.get('DATA_REGISTRY_BUCKET', 'dig-data-registry')


def create_record_directory(record_name):
    _create_directory(f'{record_name}/')


def create_dataset_directory(record_name, bucket_name):
    _create_directory(f'{record_name}/{bucket_name}/')


def _create_directory(directory):
    s3_client = boto3.client('s3', region_name=S3_REGION)
    s3_client.put_object(Bucket=BASE_BUCKET, Key=directory)


def upload_metadata(metadata, path):
    s3_client = boto3.client('s3', region_name=S3_REGION)
    s3_client.put_object(Bucket=BASE_BUCKET, Key=f"{path}/metadata", Body=json.dumps(metadata).encode('utf-8'))


def initiate_multi_part(directory: str, filename: str):
    s3_client = boto3.client('s3', region_name=S3_REGION)
    return s3_client.create_multipart_upload(Bucket=BASE_BUCKET, Key=f"{directory}/{filename}")


def put_bytes(directory, file_name, contents, upload, part_number):
    s3_client = boto3.resource('s3', region_name=S3_REGION)
    upload_part = s3_client.MultipartUploadPart(
        BASE_BUCKET, f"{directory}/{file_name}", upload['UploadId'], part_number
    )
    upload_part_response = upload_part.upload(
        Body=contents,
    )
    return upload_part_response


def get_file_path(directory, file_name):
    return f"s3://{BASE_BUCKET}/{directory}/{file_name}"


def list_files_in_bioindex_path(prefix):
    """
    List files and their sizes in a specific path in an S3 bucket.

    Parameters:
    - bucket_name (str): Name of the S3 bucket.
    - prefix (str): The path (or prefix) in the S3 bucket to list files from.

    Returns:
    - List of tuples containing filenames and their sizes.
    """
    s3 = boto3.client('s3')

    results = []
    paginator = s3.get_paginator('list_objects_v2')

    # Using a paginator to handle the case where there are more than 1000 files in the path
    for page in paginator.paginate(Bucket="dig-analysis-data", Prefix=prefix):
        for obj in page.get('Contents', []):
            filename = obj['Key']
            size = obj['Size']  # Size is in bytes
            results.append((filename.split('/')[-1], size))

    return results


def get_file_obj(path: str, bucket: str):
    s3_client = boto3.client('s3', region_name=S3_REGION)
    return s3_client.get_object(Bucket=bucket, Key=path)


def delete_record_directory(record_name):
    _delete_directory(record_name)


def _delete_directory(directory):
    s3_client = boto3.client('s3', region_name=S3_REGION)
    s3_client.put_object(Bucket=BASE_BUCKET, Key=f'{directory}/_DELETED')


def finalize_upload(directory, name, parts, multipart_upload):
    s3_client = boto3.client('s3', region_name=S3_REGION)
    s3_client.complete_multipart_upload(
        Bucket=BASE_BUCKET,
        Key=f"{directory}/{name}",
        MultipartUpload={
            'Parts': parts
        },
        UploadId=multipart_upload['UploadId'],
    )
