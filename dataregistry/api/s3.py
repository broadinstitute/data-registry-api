import json

import boto3
import os

S3_REGION = 'us-east-1'
BASE_BUCKET = os.environ.get('DATA_REGISTRY_BUCKET', 'dig-data-registry')


def create_record_directory(record_name):
    _create_directory(f'{record_name}/')


def clear_variants_raw():
    clear_dir('hermes/variants_raw')


def clear_variants():
    clear_dir('hermes/variants')


def clear_variants_processed():
    clear_dir('hermes/variants_processed')


def clear_meta_analysis():
    clear_dir('hermes/out/metaanalysis')


def clear_dir(prefix: str):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=BASE_BUCKET, Prefix=prefix)

    for page in page_iterator:
        if "Contents" in page:
            delete_keys = {'Objects': [{'Key': obj['Key']} for obj in page['Contents']]}
            s3.delete_objects(Bucket=BASE_BUCKET, Delete=delete_keys)


def copy_files_for_meta_analysis(source_prefix, destination_prefix):
    s3_client = boto3.client('s3')

    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=BASE_BUCKET, Prefix=source_prefix):
        for obj in page.get('Contents', []):
            source_key = obj['Key']
            rest_of_path = source_key[len(source_prefix):]
            destination_key = f"{destination_prefix}{rest_of_path}"
            copy_source = {
                'Bucket': BASE_BUCKET,
                'Key': source_key
            }
            s3_client.copy(copy_source, BASE_BUCKET, destination_key)


def create_dataset_directory(record_name, bucket_name):
    _create_directory(f'{record_name}/{bucket_name}/')


def _create_directory(directory):
    s3_client = boto3.client('s3', region_name=S3_REGION)
    s3_client.put_object(Bucket=BASE_BUCKET, Key=directory)


def get_full_s3_path(path, file):
    return f's3://{BASE_BUCKET}/{path}/{file}'


def upload_metadata(metadata, path):
    s3_client = boto3.client('s3', region_name=S3_REGION)
    s3_client.put_object(Bucket=BASE_BUCKET, Key=f"{path}/metadata", Body=json.dumps(metadata).encode('utf-8'))


def initiate_multi_part(directory: str, filename: str):
    s3_client = boto3.client('s3', region_name=S3_REGION)
    return s3_client.create_multipart_upload(Bucket=BASE_BUCKET, Key=f"{directory}/{filename}")

def get_signed_url(bucket, path):
    s3_client = boto3.client('s3', region_name=S3_REGION)
    return s3_client.generate_presigned_url('get_object', Params={'Bucket': bucket,
                                                                  'Key': path,
                                                                  'ResponseContentDisposition': f'attachment; filename="{path.split("/")[-1]}"'}, ExpiresIn=7200)

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
