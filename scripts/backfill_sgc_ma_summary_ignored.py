#!/usr/bin/env python3
"""Backfill stored MA summary.json artifacts to match the fixed run summary.

summary.json is written once by the Batch worker and never regenerated, so runs
that completed before V1.0.58 keep the old shape forever:
  * the ignored list was NOT scoped to the run's ancestry/sex bucket, so it
    includes files that were never candidates (e.g. a sex='Female' file in an
    All-sex run);
  * ignored rows carry "dataset": None, blanking the column that distinguishes
    an ignored row from a used row of the same cohort;
  * no row carries ancestry/sex at all, so the UI's Ancestry/Sex columns render
    as em-dashes.

This rewrites those three things in place. It does NOT re-run the meta-analysis:
every computed statistic (n_variants_in/used, cases, controls, lambda, loci)
is read from the existing summary and preserved untouched.

Scope: only rows the ignore-list injected (reason starts with "MA ignore-list: ")
are recomputed. Rows skipped by the worker for other reasons -- an extract
failure -- are preserved verbatim, only enriched with ancestry/sex.

Safety:
  * dry-run by default; --execute is required to write anything;
  * totals are recomputed from the rewritten rows and compared against the
    stored total_cases/total_controls -- a run whose totals would change is
    refused, never written;
  * the original object is copied to <key>.pre-backfill before overwriting;
  * idempotent -- a run already carrying ancestry/sex on its used rows and a
    correctly-scoped ignored list is reported as current and skipped.

Usage:
  # preview (read-only) against prod:
  python scripts/backfill_sgc_ma_summary_ignored.py --bucket dig-data-registry --db-name dataregistry
  # write:
  python scripts/backfill_sgc_ma_summary_ignored.py --bucket dig-data-registry --db-name dataregistry --execute
  # single run:
  python scripts/backfill_sgc_ma_summary_ignored.py --bucket dig-data-registry --db-name dataregistry --run-id d89831da...
"""
import json
import os
import sys

# Running `python scripts/<this>.py` puts scripts/ on sys.path, not the repo
# root, so the dataregistry package wouldn't import. Add the repo root first.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import boto3
import click
from botocore.exceptions import ClientError
from sqlalchemy import text

from dataregistry.api.db import DataRegistryReadWriteDB
from sgc_ma import select as sel
from sgc_ma.run_ma import totals_from_per_cohort

AWS_REGION = "us-east-1"
IGNORE_PREFIX = "MA ignore-list: "


def _succeeded_runs(engine, run_id=None):
    """Runs with a stored summary.json. The s3 key is read, never constructed:
    pre-multi-run rows use sgc/ma/<pheno>/<anc>/summary.json with no run id."""
    sql = """
        SELECT CAST(id AS CHAR) AS run_id, phenotype, ancestry, sex,
               summary_json_s3_key, total_cases, total_controls, created_at
        FROM sgc_gwas_ma_results
        WHERE status = 'SUCCEEDED' AND summary_json_s3_key IS NOT NULL
    """
    params = {}
    if run_id:
        sql += " AND id = :rid"
        params["rid"] = run_id
    sql += " ORDER BY created_at"
    with engine.connect() as c:
        return [dict(r._mapping) for r in c.execute(text(sql), params)]


def _is_ignore_row(row):
    return bool(row.get("skipped")) and str(row.get("reason", "")).startswith(IGNORE_PREFIX)


def rebuild_per_cohort(engine, run, per_cohort):
    """Return (new_per_cohort, old_ignored, new_ignored).

    Non-ignore rows keep their original order and every computed field; they
    only gain ancestry/sex. The ignore-list rows are dropped and replaced with
    the bucket-scoped set, matching what meta_analyze() now emits.
    """
    kept = [r for r in per_cohort if not _is_ignore_row(r)]
    old_ignored = [r for r in per_cohort if _is_ignore_row(r)]

    # Enrich kept rows from the files they reference. Rows predating file_id,
    # or whose file was deleted, simply keep whatever they already had.
    file_ids = [r["file_id"] for r in kept if r.get("file_id")]
    by_id = {}
    if file_ids:
        by_id = {r["file_id"]: r for r in sel.select_cohorts_by_file_ids(engine, file_ids)}
    for r in kept:
        src = by_id.get(r.get("file_id"))
        if src:
            r["ancestry"] = src.get("ancestry")
            r["sex"] = src.get("sex")

    new_ignored = [{"cohort": ig.get("cohort"), "dataset": ig.get("dataset"),
                    "file_id": ig.get("file_id"),
                    "ancestry": ig.get("ancestry"), "sex": ig.get("sex"),
                    "skipped": True,
                    "reason": f"{IGNORE_PREFIX}{ig.get('reason')}"}
                   for ig in sel.ignored_cohorts(engine, run["phenotype"],
                                                 run["ancestry"], run["sex"] or "All")]
    return kept + new_ignored, old_ignored, new_ignored


def _describe(row):
    """One line per ignored row. Pre-backfill rows carry only cohort+reason, so
    a row-level old/new diff is not possible -- print both lists instead of
    inventing DROP/KEEP pairings that the old data cannot support."""
    anc, sex = row.get("ancestry"), row.get("sex")
    bucket = f"[{anc}/{sex}] " if (anc or sex) else ""
    fid = row.get("file_id")
    # The short id is itself one of the things this backfill adds, so print it:
    # without it two colliding datasets render as identical before/after lines.
    name = f"{row.get('dataset') or '(no dataset)'}{'-' + str(fid)[:6] if fid else ' (no file id)'}"
    reason = str(row.get("reason", "")).removeprefix(IGNORE_PREFIX)
    return f"{row.get('cohort')} {bucket}{name} -- {reason}"


@click.command()
@click.option("--bucket", required=True,
              help="S3 bucket holding the artifacts (prod: dig-data-registry)")
@click.option("--db-name", required=True,
              help="target DB/env: dataregistry (prod) or dataregistry_qa")
@click.option("--run-id", default=None, help="restrict to a single MA run id")
@click.option("--execute", is_flag=True, default=False,
              help="actually rewrite summary.json (default: dry-run preview only)")
def main(bucket, db_name, run_id, execute):
    os.environ["DATA_REGISTRY_DB_NAME"] = db_name
    engine = DataRegistryReadWriteDB().get_engine()
    s3 = boto3.client("s3", region_name=AWS_REGION)

    runs = _succeeded_runs(engine, run_id)
    click.echo(f"succeeded runs with a stored summary: {len(runs)} "
               f"(bucket={bucket}, db={db_name}, execute={execute})\n")

    changed = skipped = refused = missing = 0
    for run in runs:
        key = run["summary_json_s3_key"]
        label = (f"{run['phenotype']}/{run['ancestry']}/{run['sex']} "
                 f"({run['run_id'][:8]})")
        try:
            body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "unknown")
            click.echo(f"!! {label}: cannot read s3://{bucket}/{key} ({code})")
            missing += 1
            continue

        summary = json.loads(body)
        before = summary.get("per_cohort") or []
        new_rows, old_ig, new_ig = rebuild_per_cohort(engine, run, json.loads(json.dumps(before)))

        if new_rows == before:
            click.echo(f"== {label}: already current, nothing to do")
            skipped += 1
            continue

        # Ignored rows never carried cases/controls, so totals must be identical.
        # If they are not, something about this summary violates our assumptions.
        new_cases, new_controls = totals_from_per_cohort(new_rows)
        if (new_cases, new_controls) != (run["total_cases"], run["total_controls"]):
            click.echo(f"!! {label}: REFUSED -- totals would change "
                       f"{(run['total_cases'], run['total_controls'])} -> "
                       f"{(new_cases, new_controls)}")
            refused += 1
            continue

        enriched = sum(1 for a, b in zip(before, new_rows)
                       if not _is_ignore_row(a) and (a.get("ancestry"), a.get("sex")) !=
                       (b.get("ancestry"), b.get("sex")))

        click.echo(f"-- {label}  s3://{bucket}/{key}")
        click.echo(f"     ignored rows: {len(old_ig)} -> {len(new_ig)}; "
                   f"used/other rows gaining ancestry+sex: {enriched}")
        if len(old_ig) != len(new_ig) or old_ig:
            for r in old_ig:
                click.echo(f"       before  {_describe(r)}")
            for r in new_ig:
                click.echo(f"       after   {_describe(r)}")

        if not execute:
            continue

        # Preserve the original alongside so a bad backfill is recoverable
        # without relying on bucket versioning being enabled.
        s3.copy_object(Bucket=bucket, Key=f"{key}.pre-backfill",
                       CopySource={"Bucket": bucket, "Key": key})
        summary["per_cohort"] = new_rows
        s3.put_object(Bucket=bucket, Key=key,
                      Body=json.dumps(summary, indent=2).encode(),
                      ContentType="application/json")
        click.echo(f"       written (original -> {key}.pre-backfill)")
        changed += 1

    click.echo(f"\n{changed} rewritten, {skipped} already current, "
               f"{refused} refused, {missing} unreadable"
               if execute else
               f"\n(dry-run: nothing written) {skipped} already current, "
               f"{refused} would be refused, {missing} unreadable")


if __name__ == "__main__":
    main()
