"""One-shot driver: insert PENDING rows + submit Batch jobs for SGC GWAS files.

Idempotent: re-running skips files whose plot_result is already SUCCEEDED
unless --force is set. Re-picks anything in PENDING or FAILED.

RUNNING rows are NOT auto-retried by the default path. If a Fargate task
dies without writing a terminal status (OOM, hardware eviction, etc.) the
row stays RUNNING indefinitely. Recovery options:
  - Run with --force (re-submits everything including RUNNING)
  - Manually flip rows: UPDATE sgc_gwas_plot_results SET status='PENDING'
    WHERE status='RUNNING' AND updated_at < <cutoff>

This is deliberate — we don't auto-retry RUNNING because legitimately-slow
jobs would double our Fargate spend.

Usage:
  python -m sgc_qc_plots.submit_qc_plots_batch --bucket dig-data-registry [--limit N] [--force] [--dry-run]
"""
import json
import os
from typing import Optional

import boto3
import click
from sqlalchemy import text

from dataregistry.api import query
from dataregistry.api.db import DataRegistryReadWriteDB


JOB_QUEUE = os.getenv("SGC_QC_PLOTS_JOB_QUEUE", "sgc-gwas-qc-plots-queue")
JOB_DEFINITION = os.getenv("SGC_QC_PLOTS_JOB_DEFINITION", "sgc-gwas-qc-plots-job")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# The Batch job definition is env-agnostic; the submitter propagates the DB
# target (and other per-env settings) into each job via containerOverrides.
# This lets a single CloudFormation stack serve both QA and prod.
TARGET_DB_NAME = os.getenv("DATA_REGISTRY_DB_NAME", "dataregistry_qa")


def _list_files(engine, force: bool, limit: Optional[int]):
    """Return rows to enqueue. By default skip SUCCEEDED; --force overrides."""
    sql = """
        SELECT
            CAST(f.id AS CHAR) AS file_id,
            f.s3_path,
            f.column_mapping,
            f.dataset,
            f.phenotype
        FROM sgc_gwas_files f
        LEFT JOIN sgc_gwas_plot_results p ON p.file_id = f.id
        WHERE :force = 1 OR p.status IS NULL OR p.status IN ('PENDING','FAILED')
        ORDER BY f.uploaded_at
    """
    if limit is not None:
        # int() cast is required — do not interpolate strings here without it
        sql += f" LIMIT {int(limit)}"
    with engine.connect() as c:
        rs = c.execute(text(sql), {"force": 1 if force else 0})
        return [dict(r._mapping) for r in rs]


def _coerce_column_mapping(raw) -> dict:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, (bytes, bytearray)):
        return json.loads(raw.decode())
    return json.loads(raw)


@click.command()
@click.option("--bucket", required=True,
              help="S3 bucket the worker should read/write (e.g. dig-data-registry-qa)")
@click.option("--limit", type=int, default=None, help="Cap the number of jobs submitted")
@click.option("--force", is_flag=True, default=False,
              help="Re-submit even SUCCEEDED files")
@click.option("--dry-run", is_flag=True, default=False,
              help="Print what would be submitted without inserting rows or calling Batch")
def main(bucket: str, limit: Optional[int], force: bool, dry_run: bool):
    engine = DataRegistryReadWriteDB().get_engine()
    batch = boto3.client("batch", region_name=AWS_REGION)

    files = _list_files(engine, force, limit)
    click.echo(f"Found {len(files)} file(s) to enqueue (bucket={bucket}, force={force}, limit={limit})")

    submitted = 0
    for f in files:
        file_id = f["file_id"]
        col_map = _coerce_column_mapping(f["column_mapping"])

        click.echo(f"  {file_id[:8]}  {f['phenotype']:<25}  {f['s3_path']}")
        if dry_run:
            continue

        # If batch.submit_job raises mid-loop, the row inserted just above
        # is left with status=PENDING and batch_job_id=NULL, which is
        # exactly the state _list_files re-picks on the next run. Crashes
        # here are safely recoverable by re-running the script.
        query.insert_sgc_plot_result_pending(engine, file_id)

        resp = batch.submit_job(
            jobName=f"sgc-qc-plots-{file_id[:16]}",
            jobQueue=JOB_QUEUE,
            jobDefinition=JOB_DEFINITION,
            parameters={
                "s3-path": f["s3_path"],
                "column-mapping": json.dumps(col_map),
                "bucket": bucket,
                "file-id": file_id,
                "output-prefix": f"sgc/qc/plots/{file_id}",
            },
            containerOverrides={
                "environment": [
                    {"name": "DATA_REGISTRY_DB_NAME", "value": TARGET_DB_NAME},
                ],
            },
        )

        # Status stays PENDING; we're only stamping the batch_job_id.
        # update_sgc_plot_result requires status as a keyword arg.
        query.update_sgc_plot_result(
            engine, file_id, status="PENDING",
            batch_job_id=resp["jobId"],
        )
        submitted += 1

    click.echo(f"Submitted {submitted} job(s)")


if __name__ == "__main__":
    main()
