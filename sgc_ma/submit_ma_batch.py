"""Submit one bottom-line MA Batch job for a (phenotype, ancestry)."""
import os

import boto3
import click

from dataregistry.api import query
from sgc_ma.select import select_cohorts

JOB_QUEUE = os.getenv("SGC_QC_PLOTS_JOB_QUEUE", "sgc-gwas-qc-plots-queue")
JOB_DEFINITION = os.getenv("SGC_MA_JOB_DEFINITION", "sgc-gwas-ma-job")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")


def submit(*, engine, batch, phenotype, ancestry, bucket, db_name, dry_run):
    cohorts = select_cohorts(engine, phenotype, ancestry)
    click.echo(f"{phenotype}/{ancestry}: {len(cohorts)} cohorts")
    for c in cohorts:
        click.echo(f"  {c['dataset']}")
    if dry_run:
        return None
    query.insert_sgc_ma_pending(engine, phenotype, ancestry)
    resp = batch.submit_job(
        jobName=f"sgc-ma-{phenotype}-{ancestry}"[:120],
        jobQueue=JOB_QUEUE, jobDefinition=JOB_DEFINITION,
        parameters={"phenotype": phenotype, "ancestry": ancestry, "bucket": bucket},
        containerOverrides={"environment": [{"name": "DATA_REGISTRY_DB_NAME", "value": db_name}]},
    )
    query.update_sgc_ma_result(engine, phenotype, ancestry, status="PENDING",
                               batch_job_id=resp["jobId"])
    return resp["jobId"]


@click.command()
@click.option("--phenotype", required=True)
@click.option("--ancestry", required=True)
@click.option("--bucket", required=True)
@click.option("--db-name", required=True)
@click.option("--dry-run", is_flag=True, default=False)
def main(phenotype, ancestry, bucket, db_name, dry_run):
    os.environ["DATA_REGISTRY_DB_NAME"] = db_name
    from dataregistry.api.db import DataRegistryReadWriteDB
    engine = DataRegistryReadWriteDB().get_engine()
    batch = boto3.client("batch", region_name=AWS_REGION)
    job = submit(engine=engine, batch=batch, phenotype=phenotype, ancestry=ancestry,
                 bucket=bucket, db_name=db_name, dry_run=dry_run)
    click.echo(f"submitted {job}" if job else "(dry-run)")


if __name__ == "__main__":
    main()
