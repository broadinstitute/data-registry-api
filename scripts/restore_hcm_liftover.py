#!/usr/bin/env python3
"""
Reverse a replace-in-place HCM liftover: copy the archived original back over the
file's own S3 path, flip the genome_build back, and delete the liftover job row so
the file reads "Needs liftover" and is liftable again.

The worker archives the untouched original (server-side S3 copy) BEFORE it
overwrites the file, and records that archive path in
hcm_liftover_jobs.original_s3_path -- so this restore is a clean, byte-identical undo.

The build is set to --to-build (default GRCh37). That is sufficient because MA
eligibility keys off normalize_build(); pass the file's pre-lift genome_build
string explicitly if you captured it and want an exact restore.

Scope notes (deliberate, documented limitations): the genome_build flip and the
job-row delete are two separate DB statements, not one transaction -- a crash
between them is fixed by simply re-running restore() (idempotent: re-flips the same
build, and DELETE ... WHERE file_id=... matches zero rows if already gone).
copy_object() is a single S3 server-side copy, capped at 5 GB by S3 -- symmetric
with how gwas_liftover.py created the archive in the first place.

Usage:
  python scripts/restore_hcm_liftover.py --file-id <uuid> --bucket dig-data-registry [--to-build GRCh37] [--dry-run]
"""
import os
import sys

import boto3
import click
from botocore.exceptions import ClientError
from sqlalchemy import text

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dataregistry.api import hcm_query as query
from dataregistry.api.db import DataRegistryReadWriteDB

_FILE_TABLE = "hcm_gwas_files"
_JOB_TABLE = "hcm_liftover_jobs"
_TERMINAL_STATUSES = ("SUCCEEDED", "FAILED")
_NOT_FOUND_CODES = {"404", "NoSuchKey", "NotFound"}


def _parse_s3(path: str) -> tuple[str, str]:
    """Split 's3://bucket/key' into (bucket, key)."""
    rest = path.removeprefix("s3://")
    bucket, _, key = rest.partition("/")
    return bucket, key


def _get_file_s3_path(engine, file_id: str) -> str | None:
    with engine.connect() as conn:
        row = conn.execute(
            text(f"SELECT s3_path FROM {_FILE_TABLE} WHERE id = :id"),
            {"id": str(file_id).replace("-", "")},
        ).first()
    return row[0] if row else None


def _delete_liftover_jobs_for_file(engine, file_id: str) -> None:
    """Delete ALL liftover job rows for this file (not just the latest) -- a file can
    have older FAILED/terminal rows from a prior attempt that must not survive a
    restore, or get_*_liftover_job_for_file would keep surfacing them afterward."""
    with engine.connect() as conn:
        conn.execute(
            text(f"DELETE FROM {_JOB_TABLE} WHERE file_id = :file_id"),
            {"file_id": str(file_id).replace("-", "")},
        )
        conn.commit()


def restore(engine, s3_client, file_id, bucket, to_build="GRCh37", dry_run=False) -> dict:
    file_id = str(file_id).replace("-", "")
    job = query.get_hcm_liftover_job_for_file(engine, file_id)
    if job is None:
        raise SystemExit(f"No liftover job for file {file_id}; nothing to restore.")
    if job.get("status") not in _TERMINAL_STATUSES:
        raise SystemExit(
            f"Liftover job for {file_id} is {job.get('status')!r} (in flight); "
            f"refusing to restore. Wait for it to finish or mark it FAILED first."
        )
    archive_s3 = job.get("original_s3_path")
    if not archive_s3:
        raise SystemExit(f"Liftover job for {file_id} has no original_s3_path; cannot restore.")
    job_id = job["id"]

    s3_path = _get_file_s3_path(engine, file_id)
    if s3_path is None:
        raise SystemExit(f"No {_FILE_TABLE} row for {file_id}.")
    target_s3 = f"s3://{bucket}/{s3_path}"

    a_bucket, a_key = _parse_s3(archive_s3)
    if bucket != a_bucket:
        raise SystemExit(
            f"--bucket {bucket!r} does not match the archive bucket {a_bucket!r}; "
            f"refusing to write to the wrong bucket."
        )
    # Guard: the archive must exist -- fail friendly on a real not-found; re-raise
    # anything else (e.g. 403/throttling) so it isn't silently mislabeled.
    try:
        s3_client.head_object(Bucket=a_bucket, Key=a_key)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in _NOT_FOUND_CODES:
            raise SystemExit(f"Archive not found at {archive_s3}; cannot restore.")
        raise

    plan = {"file_id": str(file_id), "job_id": str(job_id), "archive": archive_s3,
            "target": target_s3, "to_build": to_build, "dry_run": dry_run}
    if dry_run:
        return plan

    t_bucket, t_key = _parse_s3(target_s3)
    s3_client.copy_object(
        Bucket=t_bucket,
        Key=t_key,
        CopySource={"Bucket": a_bucket, "Key": a_key},
    )
    query.set_hcm_gwas_file_build(engine, file_id, to_build)
    _delete_liftover_jobs_for_file(engine, file_id)
    return plan


@click.command()
@click.option("--file-id", required=True, help="HCM GWAS file id (uuid) to restore")
@click.option("--bucket", required=True, help="S3 bucket the file lives in (e.g. dig-data-registry)")
@click.option("--to-build", default="GRCh37", show_default=True, help="genome_build to restore")
@click.option("--dry-run", is_flag=True, default=False, help="Print the plan; touch nothing")
def main(file_id, bucket, to_build, dry_run):
    engine = DataRegistryReadWriteDB().get_engine()
    s3_client = boto3.client("s3")
    plan = restore(engine, s3_client, file_id, bucket, to_build=to_build, dry_run=dry_run)
    verb = "[DRY RUN] would restore" if dry_run else "restored"
    click.echo(f"{verb}: {plan['archive']} -> {plan['target']}")
    click.echo(f"  genome_build -> {plan['to_build']}; delete job {str(plan['job_id'])[:8]}")
    if dry_run:
        click.echo("[DRY RUN] no changes made.")


if __name__ == "__main__":
    main()
