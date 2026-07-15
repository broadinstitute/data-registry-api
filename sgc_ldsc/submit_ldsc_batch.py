"""One-shot submitter: enqueue LDSC Batch jobs for the matched-panel, already-QC'd
GWAS (the 87). Stamps only ldsc_* columns; never resets the shared QC row.

Usage:
  python -m sgc_ldsc.submit_ldsc_batch --bucket dig-data-registry \
      --ref-bucket dig-ldsc-server --db-name dataregistry [--limit N] [--dry-run]
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import boto3
import click
from sqlalchemy import text

from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api import query

# Reuse the QC stack's queue/compute-env; only the job definition differs.
JOB_QUEUE = os.getenv("SGC_QC_PLOTS_JOB_QUEUE", "sgc-gwas-qc-plots-queue")
JOB_DEFINITION = os.getenv("SGC_LDSC_JOB_DEFINITION", "sgc-gwas-ldsc-job")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

LIST_SQL = """
    SELECT CAST(f.id AS CHAR) AS file_id, f.s3_path, f.column_mapping,
           f.ancestry, f.phenotype, f.dataset,
           JSON_UNQUOTE(JSON_EXTRACT(gc.metadata, '$.genome_build')) AS genome_build
    FROM sgc_gwas_files f
    JOIN sgc_gwas_plot_results p ON p.file_id = f.id
    LEFT JOIN sgc_gwas_cohorts gc ON gc.cohort_id = f.cohort_id
    WHERE f.ancestry NOT IN ('Combined','MID')
      AND (p.ldsc_status IS NULL OR p.ldsc_status IN ('PENDING','FAILED'))
    ORDER BY f.ancestry, f.dataset, f.phenotype
"""


# Free-text genome_build labels seen in prod that denote canonical builds.
# Closed map; unknown values pass through unchanged so the build skip-check in
# main() still rejects them. "GRCh37 liftover to GRCh38" data is already on
# GRCh38 coordinates, so it maps to GRCh38 (drives which snpmap the worker loads).
_BUILD_ALIASES = {
    "GRCh38": "GRCh38",
    "GRCh37": "GRCh37",
    "GRCh38 / hg38": "GRCh38",
    "GRCh37 liftover to GRCh38": "GRCh38",
}


def _normalize_build(raw):
    return _BUILD_ALIASES.get(raw, raw)


def _coerce_mapping(raw) -> dict:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, (bytes, bytearray)):
        return json.loads(raw.decode())
    return json.loads(raw)


def _submit_one(engine, batch, row, *, bucket, ref_bucket, db_name):
    fid = row["file_id"]
    col_map = _coerce_mapping(row["column_mapping"])
    query.update_sgc_ldsc_pending(engine, fid)
    resp = batch.submit_job(
        jobName=f"sgc-ldsc-{fid[:16]}",
        jobQueue=JOB_QUEUE,
        jobDefinition=JOB_DEFINITION,
        parameters={
            "s3-path": row["s3_path"],
            "column-mapping": json.dumps(col_map),
            "bucket": bucket,
            "file-id": fid,
            "ancestry": row["ancestry"],
            "genome-build": row["genome_build"],
            "ref-bucket": ref_bucket,
        },
        containerOverrides={"environment": [
            {"name": "DATA_REGISTRY_DB_NAME", "value": db_name},
        ]},
    )
    query.update_sgc_ldsc_result(engine, fid, ldsc_status="PENDING",
                                 ldsc_batch_job_id=resp["jobId"])


@click.command()
@click.option("--bucket", required=True, help="GWAS data bucket (prod: dig-data-registry)")
@click.option("--ref-bucket", required=True, help="LDSC reference bucket (from Phase 0)")
@click.option("--db-name", required=True, help="dataregistry (prod) or dataregistry_qa")
@click.option("--limit", type=int, default=None)
@click.option("--dry-run", is_flag=True, default=False)
def main(bucket, ref_bucket, db_name, limit, dry_run):
    os.environ["DATA_REGISTRY_DB_NAME"] = db_name
    engine = DataRegistryReadWriteDB().get_engine()
    with engine.connect() as c:
        rows = [dict(r._mapping) for r in c.execute(text(LIST_SQL))]
    if limit:
        rows = rows[:limit]
    click.echo(f"candidates: {len(rows)} (bucket={bucket}, ref={ref_bucket}, db={db_name})")
    batch = boto3.client("batch", region_name=AWS_REGION)
    n = 0
    for row in rows:
        row["genome_build"] = _normalize_build(row["genome_build"])
        skip = row["genome_build"] not in ("GRCh38", "GRCh37")
        flag = " SKIP(build)" if skip else ""
        click.echo(f"  {row['file_id'][:8]} {row['ancestry']:4} {str(row['genome_build']):8} "
                   f"{row['phenotype']}{flag}")
        if dry_run or skip:
            continue
        _submit_one(engine, batch, row, bucket=bucket, ref_bucket=ref_bucket, db_name=db_name)
        n += 1
    click.echo(f"submitted {n}" if not dry_run else "(dry-run)")


if __name__ == "__main__":
    main()
