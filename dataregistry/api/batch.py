import time

import boto3

from dataregistry.api.model import HermesFileStatus
from dataregistry.api.s3 import S3_REGION


def submit_aggregator_job(branch, method, extra_args):
    batch_client = boto3.client('batch', region_name=S3_REGION)

    response = batch_client.submit_job(
        jobName='aggregator-web',
        jobQueue='aggregator-web-api-queue',
        jobDefinition='aggregator-web-job',
        parameters={'branch': branch, 'method': method, 'args': extra_args},
    )
    job_id = response['jobId']
    return job_id


def submit_and_await_job(engine, job_config, db_callback, identifier, is_qc=True):
    batch_client = boto3.client('batch', region_name=S3_REGION)

    response = batch_client.submit_job(**job_config)
    job_id = response['jobId']
    logs_client = boto3.client('logs', region_name=S3_REGION)
    while True:
        response = batch_client.describe_jobs(jobs=[job_id])
        job_status = response['jobs'][0]['status']
        if job_status in ['SUCCEEDED', 'FAILED']:
            log_stream_name = response['jobs'][0]['container']['logStreamName']
            log_group_name = '/aws/batch/job'
            log_events = logs_client.get_log_events(
                logGroupName=log_group_name,
                logStreamName=log_stream_name
            )
            log_messages = [event['message'] for event in log_events['events']]
            complete_log = '\n'.join(log_messages)
            if is_qc:
                db_callback(engine, complete_log, identifier,
                            HermesFileStatus.READY_FOR_REVIEW if job_status == 'SUCCEEDED' else
                            HermesFileStatus.FAILED_QC)
            else:
                db_callback(engine, complete_log, identifier, job_status)
            break
        time.sleep(60)
