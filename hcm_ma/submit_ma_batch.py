"""Submit one bottom-line HCM MA Batch job for a recorded run."""
import os

from dataregistry.api import hcm_query

JOB_QUEUE = os.getenv("HCM_MA_JOB_QUEUE", "sgc-gwas-qc-plots-queue")
JOB_DEFINITION = os.getenv("HCM_MA_JOB_DEFINITION", "hcm-gwas-ma-job")


def submit_run(*, engine, batch, run_id, bucket, db_name):
    resp = batch.submit_job(
        jobName=f"hcm-ma-{run_id}"[:120],
        jobQueue=JOB_QUEUE, jobDefinition=JOB_DEFINITION,
        parameters={"run-id": run_id, "bucket": bucket},
        containerOverrides={"environment": [{"name": "DATA_REGISTRY_DB_NAME", "value": db_name}]},
    )
    hcm_query.update_hcm_ma_result(engine, run_id, status="PENDING", batch_job_id=resp["jobId"])
    return resp["jobId"]
