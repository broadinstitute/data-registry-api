import boto3

S3_REGION = 'us-east-1'
BASE_BUCKET = 'dig-data-registry'


def create_record_directory(record_name):
    _create_directory(f'{record_name}/')


def create_dataset_directory(record_name, bucket_name):
    _create_directory(f'{record_name}/{bucket_name}/')


def _create_directory(directory):
    s3_client = boto3.client('s3', region_name=S3_REGION)
    s3_client.put_object(Bucket=BASE_BUCKET, Key=directory)


def delete_record_directory(record_name):
    _delete_directory(record_name)


def _delete_directory(directory):
    s3_client = boto3.client('s3', region_name=S3_REGION)
    s3_client.put_object(Bucket=BASE_BUCKET, Key=f'{directory}/_DELETED')
