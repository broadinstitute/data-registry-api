import json
import os
import time

import boto3

from dataregistry.api import query, s3
from dataregistry.api.model import QCRun, QCStepResult

QC_JOB_QUEUE = os.getenv('QC_JOB_QUEUE', 'qc-pipeline-queue')
QC_JOB_DEFINITION = os.getenv('QC_JOB_DEFINITION', 'qc-pipeline-job')
QC_PIPELINE_REPO_URL = os.getenv('QC_PIPELINE_REPO_URL', '')
QC_PIPELINE_DEFAULT_COMMIT = os.getenv('QC_PIPELINE_DEFAULT_COMMIT', '')


def kick_off_qc_run(engine, background_tasks, input_s3_path, pipeline, params,
                    submitted_by, pinned_commit=None):
    commit = pinned_commit or QC_PIPELINE_DEFAULT_COMMIT
    run = QCRun(input_s3_path=input_s3_path, pipeline=pipeline, pinned_commit=commit,
                status='SUBMITTED', submitted_by=submitted_by)
    run_id = query.insert_qc_run(engine, run)
    background_tasks.add_task(_submit_and_await, engine, run_id, input_s3_path,
                             pipeline, params, commit)
    return run_id


def _submit_and_await(engine, run_id, input_s3_path, pipeline, params, commit):
    output_prefix_key = f"qc/runs/{run_id}"
    try:
        batch_client = boto3.client('batch', region_name=s3.S3_REGION)
        response = batch_client.submit_job(
            jobName=f"qc-run-{run_id[:16]}",
            jobQueue=QC_JOB_QUEUE,
            jobDefinition=QC_JOB_DEFINITION,
            parameters={
                'input-s3-uri': f"s3://{s3.BASE_BUCKET}/{input_s3_path}",
                'output-s3-prefix': f"s3://{s3.BASE_BUCKET}/{output_prefix_key}",
                'pipeline': pipeline,
                'params-json': json.dumps(params),
                'repo-url': QC_PIPELINE_REPO_URL,
                'repo-commit': commit,
            },
        )
        batch_job_id = response['jobId']
        query.update_qc_run_status(engine, run_id, 'RUNNING', batch_job_id=batch_job_id)

        while True:
            desc = batch_client.describe_jobs(jobs=[batch_job_id])
            job_status = desc['jobs'][0]['status']
            if job_status in ('SUCCEEDED', 'FAILED'):
                break
            time.sleep(10)

        _ingest_result(engine, run_id, output_prefix_key, job_status)
    except Exception as e:
        query.update_qc_run_status(engine, run_id, 'FAILED', error_message=str(e))


def _ingest_result(engine, run_id, output_prefix_key, job_status):
    s3_client = boto3.client('s3', region_name=s3.S3_REGION)
    try:
        obj = s3_client.get_object(Bucket=s3.BASE_BUCKET,
                                   Key=f"{output_prefix_key}/result.json")
        result = json.loads(obj['Body'].read().decode('utf-8'))
    except Exception:
        query.update_qc_run_status(engine, run_id, 'FAILED',
                                   error_message='result.json missing or unreadable')
        return

    for idx, step in enumerate(result.get('steps', [])):
        query.insert_qc_step_result(engine, QCStepResult(
            run_id=run_id,
            step=step.get('step', ''),
            verdict=step.get('verdict'),
            metrics=step.get('metrics'),
            messages=step.get('messages'),
            artifacts=step.get('artifacts'),
            step_index=idx,
        ))

    succeeded = result.get('status') == 'completed' and job_status == 'SUCCEEDED'
    outputs = result.get('outputs') or {}
    gwas_key = f"{output_prefix_key}/{outputs['gwas_filtered']}" if outputs.get('gwas_filtered') else None
    report_key = f"{output_prefix_key}/{outputs['qc_report']}" if outputs.get('qc_report') else None
    query.update_qc_run_status(
        engine, run_id, 'COMPLETED' if succeeded else 'FAILED',
        overall_verdict=result.get('overall_verdict'),
        gwas_filtered_s3_key=gwas_key, qc_report_s3_key=report_key,
    )
