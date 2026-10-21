"""Driver: insert PENDING rows + submit one munge Batch job per SGC GWAS file.

Eligible files: QC plots SUCCEEDED, effective genome build GRCh38, and munge
status NULL/PENDING/FAILED (any status with --force or an explicit --file-id).
RUNNING rows are not auto-retried (same policy as the QC plots driver).

PENDING rows are re-picked even when they already carry a batch_job_id, so
running the driver twice within the queue latency window (before the worker
flips the row to RUNNING) submits a duplicate Batch job for that file. This is
harmless: both jobs share the deterministic output prefix and the second
overwrites the first. It is deliberate: rows whose job died before the worker
ran (e.g. an image-pull failure) are recovered automatically rather than being
stranded behind a batch_job_id IS NULL guard.

Usage:
  python -m sgc_munge.submit_munge_batch --bucket dig-data-registry-qa --db-name dataregistry_qa \
      [--limit N] [--force] [--dry-run] [--phenotype P] [--file-id ID ...]
"""
import json
import os
from typing import Optional

import boto3
import click
from sqlalchemy import bindparam, text

from dataregistry.api import query
from sgc_ma.select import normalize_build

JOB_QUEUE = os.getenv("SGC_QC_PLOTS_JOB_QUEUE", "sgc-gwas-qc-plots-queue")
JOB_DEFINITION = os.getenv("SGC_MUNGE_JOB_DEFINITION", "sgc-gwas-munge-job")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
DEFAULT_REF_PREFIX = os.getenv("SGC_REF_PREFIX", "s3://dig-data-registry/sgc/ref/grch38-dbsnp155/")

_LIST_SQL = """
    SELECT
        CAST(f.id AS CHAR) AS file_id,
        f.s3_path,
        f.column_mapping,
        f.dataset,
        f.phenotype,
        f.cases,
        f.controls,
        COALESCE(JSON_UNQUOTE(JSON_EXTRACT(f.metadata, '$.genome_build')), JSON_UNQUOTE(JSON_EXTRACT(gc.metadata, '$.genome_build'))) AS genome_build,
        mu.status AS munge_status
    FROM sgc_gwas_files f
    JOIN sgc_gwas_plot_results p ON p.file_id = f.id AND p.status = 'SUCCEEDED'
    LEFT JOIN sgc_gwas_munge_results mu ON mu.file_id = f.id
    LEFT JOIN sgc_gwas_cohorts gc ON gc.cohort_id = f.cohort_id
    WHERE (:force = 1 OR mu.status IS NULL OR mu.status IN ('PENDING','FAILED'))
      AND (:phenotype IS NULL OR f.phenotype = :phenotype)
    ORDER BY f.uploaded_at
"""


_EXPLAIN_SQL = """
    SELECT
        CAST(f.id AS CHAR) AS file_id,
        f.phenotype,
        COALESCE(JSON_UNQUOTE(JSON_EXTRACT(f.metadata, '$.genome_build')), JSON_UNQUOTE(JSON_EXTRACT(gc.metadata, '$.genome_build'))) AS genome_build,
        p.status AS plot_status
    FROM sgc_gwas_files f
    LEFT JOIN sgc_gwas_plot_results p ON p.file_id = f.id
    LEFT JOIN sgc_gwas_cohorts gc ON gc.cohort_id = f.cohort_id
    WHERE f.id IN :file_ids
"""


def _list_files(engine, *, force: bool, limit: Optional[int], phenotype: Optional[str],
                file_ids: tuple = ()) -> list:
    """Files to munge. Explicit file_ids always re-submit, but the GRCh38 and
    QC-SUCCEEDED gates still apply."""
    params = {"force": 1 if (force or file_ids) else 0, "phenotype": phenotype}
    with engine.connect() as c:
        rows = [dict(r._mapping) for r in c.execute(text(_LIST_SQL), params)]
    rows = [r for r in rows if normalize_build(r.get("genome_build")) == "GRCh38"]
    if file_ids:
        wanted = set(file_ids)
        rows = [r for r in rows if r["file_id"] in wanted]
    if limit is not None:
        rows = rows[:int(limit)]
    return rows


def _dropped_file_ids(engine, file_ids: tuple, listed: list,
                      phenotype: Optional[str]) -> dict:
    """Why each explicitly requested file id is missing from ``listed`` (the output of
    ``_list_files``), in request order: {file_id: reason}."""
    kept = {r["file_id"] for r in listed}
    missing = [fid for fid in dict.fromkeys(file_ids) if fid not in kept]
    if not missing:
        return {}
    stmt = text(_EXPLAIN_SQL).bindparams(bindparam("file_ids", expanding=True))
    with engine.connect() as c:
        found = {r["file_id"]: r for r in
                 (dict(row._mapping) for row in c.execute(stmt, {"file_ids": missing}))}
    reasons = {}
    for fid in missing:
        r = found.get(fid)
        if r is None:
            reasons[fid] = "unknown file id"
        elif r.get("plot_status") != "SUCCEEDED":
            reasons[fid] = f"QC plots not SUCCEEDED (status={r.get('plot_status') or 'none'})"
        elif normalize_build(r.get("genome_build")) != "GRCh38":
            reasons[fid] = f"genome build {r.get('genome_build')!r} is not GRCh38"
        elif phenotype is not None and r.get("phenotype") != phenotype:
            reasons[fid] = f"phenotype {r.get('phenotype')!r} does not match --phenotype {phenotype!r}"
        else:
            reasons[fid] = "cut by --limit"
    return reasons


def _coerce_column_mapping(raw) -> dict:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, (bytes, bytearray)):
        return json.loads(raw.decode())
    return json.loads(raw)


def _count(v) -> str:
    return str(v) if v is not None else "NA"


def submit_file(*, engine, batch, row: dict, bucket: str, db_name: str, ref_prefix: str) -> str:
    file_id = row["file_id"]
    # If submit_job raises, the PENDING row with NULL batch_job_id is exactly
    # what _list_files re-picks on the next run.
    query.insert_sgc_munge_result_pending(engine, file_id)
    resp = batch.submit_job(
        jobName=f"sgc-munge-{file_id[:16]}",
        jobQueue=JOB_QUEUE,
        jobDefinition=JOB_DEFINITION,
        parameters={
            "s3-path": row["s3_path"],
            "column-mapping": json.dumps(_coerce_column_mapping(row.get("column_mapping"))),
            "bucket": bucket,
            "file-id": file_id,
            "output-prefix": f"sgc/munge/{file_id}",
            "ref-prefix": ref_prefix,
            "cases": _count(row.get("cases")),
            "controls": _count(row.get("controls")),
        },
        containerOverrides={
            "environment": [{"name": "DATA_REGISTRY_DB_NAME", "value": db_name}],
        },
    )
    query.update_sgc_munge_result(engine, file_id, status="PENDING", batch_job_id=resp["jobId"])
    return resp["jobId"]


@click.command()
@click.option("--bucket", required=True, help="S3 bucket the worker reads/writes")
@click.option("--db-name", required=True, help="Database the worker and this driver target")
@click.option("--limit", type=int, default=None, help="Cap the number of jobs submitted")
@click.option("--force", is_flag=True, default=False, help="Re-submit regardless of munge status")
@click.option("--dry-run", is_flag=True, default=False, help="List only; no rows, no Batch calls")
@click.option("--phenotype", default=None, help="Restrict to one phenotype")
@click.option("--file-id", "file_ids", multiple=True, help="Restrict to these file ids (re-submits)")
@click.option("--ref-prefix", default=DEFAULT_REF_PREFIX, show_default=True)
def main(bucket, db_name, limit, force, dry_run, phenotype, file_ids, ref_prefix):
    os.environ["DATA_REGISTRY_DB_NAME"] = db_name
    from dataregistry.api.db import DataRegistryReadWriteDB
    engine = DataRegistryReadWriteDB().get_engine()
    batch = boto3.client("batch", region_name=AWS_REGION)

    files = _list_files(engine, force=force, limit=limit, phenotype=phenotype, file_ids=tuple(file_ids))
    if file_ids:
        for fid, reason in _dropped_file_ids(engine, tuple(file_ids), files, phenotype).items():
            click.echo(f"  dropped --file-id {fid}: {reason}")
    click.echo(f"Found {len(files)} file(s) to munge (bucket={bucket}, db={db_name}, force={force}, limit={limit})")

    submitted = 0
    for f in files:
        click.echo(f"  {f['file_id'][:8]}  {str(f.get('phenotype')):<25}  {f['s3_path']}")
        if dry_run:
            continue
        submit_file(engine=engine, batch=batch, row=f, bucket=bucket, db_name=db_name, ref_prefix=ref_prefix)
        submitted += 1
    click.echo(f"Submitted {submitted} job(s)")


if __name__ == "__main__":
    main()
