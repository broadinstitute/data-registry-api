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


def get_file_obj(path: str):
    s3_client = boto3.client('s3', region_name=S3_REGION)
    return s3_client.get_object(Bucket=BASE_BUCKET, Key=path)


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
