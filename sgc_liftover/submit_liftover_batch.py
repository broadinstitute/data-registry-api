"""Operator driver: lift build-37 SGC GWAS files to GRCh38, reusing the shared
HERMES gwas-liftover-job. Liftover is a precondition for QC (the QC-plots driver
skips non-GRCh38 files), so run this first, then submit_qc_plots_batch.

Replace-in-place: the worker overwrites the file's own s3_path with the lifted
data and archives the hg19 original. On success we flip the file's per-file
build marker to GRCh38, after which submit_qc_plots_batch picks it up.

Idempotent: files with a SUCCEEDED/PENDING/RUNNING sgc_liftover_jobs row are
skipped. An interrupted run can leave a RUNNING row orphaned; recover by
manually flipping it to FAILED (or deleting it) and re-running — same stance as
submit_qc_plots_batch's RUNNING handling.

Usage:
  python -m sgc_liftover.submit_liftover_batch --bucket dig-data-registry-qa [--limit N] [--dry-run]
"""
import json
import os
import re
from typing import Optional

import click
from sqlalchemy import text

from dataregistry.api import batch, query
from dataregistry.api.db import DataRegistryReadWriteDB
from sgc_ma.select import normalize_build

_SGC_ARCHIVE_PREFIX = "sgc/liftover/_archive"

# SGC column_mapping (col_* keys) -> the worker's {chromosome,position,ref,alt}.
# ref=non-effect, alt=effect (consistent; the worker complements on strand flips,
# it does not swap effect/other, so effect direction is preserved).
_REQUIRED_SGC_KEYS = ["col_chromosome", "col_position", "col_effect_allele", "col_non_effect_allele"]


def to_worker_column_mapping(sgc_map: dict) -> dict:
    missing = [k for k in _REQUIRED_SGC_KEYS if not sgc_map.get(k)]
    if missing:
        raise ValueError(f"column_mapping missing keys for liftover: {missing}")
    return {
        "chromosome": sgc_map["col_chromosome"],
        "position": sgc_map["col_position"],
        "ref": sgc_map["col_non_effect_allele"],
        "alt": sgc_map["col_effect_allele"],
    }


def ucsc_source_build(normalized_build: Optional[str]) -> str:
    """Map a normalize_build() value to the worker's UCSC source name. Only a
    liftable (non-GRCh38) build is valid here."""
    if normalized_build == "GRCh37":
        return "hg19"
    raise ValueError(f"not a liftable source build: {normalized_build!r}")


# Files that are NOT GRCh38-effective and have no SUCCEEDED/in-flight lift.
# Build is filtered in Python (normalize_build) after this coarse SQL.
_LIFT_LIST_SQL = """
    SELECT
        CAST(f.id AS CHAR) AS file_id,
        f.s3_path,
        f.column_mapping,
        f.dataset,
        f.phenotype,
        COALESCE(JSON_UNQUOTE(JSON_EXTRACT(f.metadata, '$.genome_build')), JSON_UNQUOTE(JSON_EXTRACT(gc.metadata, '$.genome_build'))) AS genome_build
    FROM sgc_gwas_files f
    LEFT JOIN sgc_gwas_cohorts gc ON gc.cohort_id = f.cohort_id
    LEFT JOIN sgc_liftover_jobs lj ON lj.file_id = f.id
        AND lj.status IN ('SUCCEEDED','PENDING','RUNNING')
    WHERE lj.id IS NULL
    ORDER BY f.uploaded_at
"""


def _coerce_map(raw) -> dict:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, (bytes, bytearray)):
        return json.loads(raw.decode())
    return json.loads(raw)


def _partition_liftable(rows):
    """Pure partition of _LIFT_LIST_SQL rows into (liftable, unrecognized).
    liftable: effective build normalizes to GRCh37 (needs lifting).
    unrecognized: build normalizes to neither GRCh37 nor GRCh38 -- e.g. an
    unrecognized label or a missing build. These are not lifted (nothing to
    lift them to a known source build), so without reporting they'd be
    silently dropped: never lifted, never QC'd, never in the MA."""
    liftable, unrecognized = [], []
    for r in rows:
        nb = normalize_build(r.get("genome_build"))
        if nb == "GRCh37":
            liftable.append(r)
        elif nb != "GRCh38":
            unrecognized.append(r)
    return liftable, unrecognized


def _select_liftable(engine, limit):
    """Return (liftable, unrecognized). `limit` caps only the liftable list --
    unrecognized files are always reported in full regardless of --limit."""
    with engine.connect() as c:
        rs = c.execute(text(_LIFT_LIST_SQL))
        rows = [dict(r._mapping) for r in rs]
    liftable, unrecognized = _partition_liftable(rows)
    return (liftable[:limit] if limit is not None else liftable), unrecognized


def make_liftover_callback(file_id: str):
    """Closure for batch.submit_and_await_job(is_qc=False):
    callback(engine, complete_log, liftover_id, job_status)."""
    def _cb(cb_engine, complete_log: str, liftover_id: str, job_status: str):
        summary = None
        # Greedy (not `.*?`): the summary JSON is nested (unmapped_breakdown,
        # per_chromosome are dicts themselves), so a non-greedy match would stop
        # at the first inner `}`. Safe because the worker prints the summary as
        # compact JSON on a single line and `.` doesn't match newline (no re.DOTALL).
        m = re.search(r'LIFTOVER_SUMMARY_JSON:\s*(\{.*\})', complete_log or '')
        if m:
            try:
                summary = json.loads(m.group(1))
            except json.JSONDecodeError:
                pass
        status = "SUCCEEDED" if job_status == "SUCCEEDED" else "FAILED"
        query.update_sgc_liftover_job(cb_engine, liftover_id, status=status,
                                      log=complete_log, summary=summary, completed=True)
        if job_status == "SUCCEEDED":
            # Flip the per-file build so submit_qc_plots_batch picks it up + MA includes it.
            query.set_sgc_gwas_file_build(cb_engine, file_id, "GRCh38")
    return _cb


@click.command()
@click.option("--bucket", required=True, help="S3 bucket the worker reads/writes (e.g. dig-data-registry-qa)")
@click.option("--limit", type=int, default=None, help="Cap the number of files lifted")
@click.option("--dry-run", is_flag=True, default=False, help="Print what would be submitted; no DB writes, no Batch calls")
def main(bucket: str, limit: Optional[int], dry_run: bool):
    engine = DataRegistryReadWriteDB().get_engine()
    files, unrecognized = _select_liftable(engine, limit)
    click.echo(f"Found {len(files)} build-37 file(s) to lift (bucket={bucket})")
    for r in unrecognized:
        click.echo(f"  WARNING: {r['file_id'][:8]}  genome_build={r.get('genome_build')!r}"
                   f"  -- unrecognized build, skipped (not lifted, not QC'd, not in MA)")

    for f in files:
        file_id = f["file_id"]
        s3_key = f["s3_path"]
        file_name = os.path.basename(s3_key)
        src = ucsc_source_build(normalize_build(f["genome_build"]))
        worker_map = to_worker_column_mapping(_coerce_map(f["column_mapping"]))

        input_s3 = f"s3://{bucket}/{s3_key}"
        output_s3 = input_s3                                   # replace-in-place
        archive_s3 = f"s3://{bucket}/{_SGC_ARCHIVE_PREFIX}/{file_id}/{file_name}"
        unmapped_s3 = f"s3://{bucket}/sgc/liftover/{file_id}/unmapped.tsv"
        summary_s3 = f"s3://{bucket}/sgc/liftover/{file_id}/summary.json"

        click.echo(f"  {file_id[:8]}  {f['phenotype']:<20}  {src}->hg38  {s3_key}")
        if dry_run:
            continue

        lid = query.insert_sgc_liftover_pending(
            engine, file_id, src, "hg38", archive_s3, unmapped_s3, "system")
        query.update_sgc_liftover_job(engine, lid, status="RUNNING")

        job_config = {
            "jobName": f"sgc-liftover-{file_id[:16]}",
            "jobQueue": "gwas-liftover-job-queue",
            "jobDefinition": "gwas-liftover-job",
            "parameters": {
                "input-s3-path": input_s3, "output-s3-path": output_s3,
                "archive-s3-path": archive_s3, "unmapped-s3-path": unmapped_s3,
                "summary-s3-path": summary_s3, "source-build": src, "target-build": "hg38",
                "column-mapping": json.dumps(worker_map), "job-id": lid,
            },
        }
        # Blocking submit + poll (the worker doesn't self-report); the callback
        # does the writeback. is_qc=False -> raw status string in the callback.
        batch.submit_and_await_job(engine, job_config, make_liftover_callback(file_id), lid, False)
        click.echo(f"    done: liftover job {lid[:8]}")

    click.echo("liftover run complete")


if __name__ == "__main__":
    main()
