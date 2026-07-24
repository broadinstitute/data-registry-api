#!/usr/bin/env python3
"""Backfill total_cases/total_controls on existing sgc_gwas_ma_results rows.

For each SUCCEEDED MA row that has a summary.json, sum the per-cohort cases and
controls over the cohorts actually used and write the two totals. Everything
else on the row is left untouched (COALESCE), and status is re-written to its
current value (a no-op).

Safety: dry-run by default. Pass --execute to write. --db-name is the single
source of truth for which environment is targeted.

Usage:
  python scripts/backfill_sgc_ma_totals.py --bucket dig-data-registry --db-name dataregistry
  python scripts/backfill_sgc_ma_totals.py --bucket dig-data-registry --db-name dataregistry --execute
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import boto3
import click

from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api import query
from sgc_ma.run_ma import totals_from_per_cohort


def needs_backfill(row: dict) -> bool:
    """A completed MA run that actually has a summary artifact to read totals from."""
    return row.get("status") == "SUCCEEDED" and bool(row.get("summary_json_s3_key"))


@click.command()
@click.option("--bucket", required=True, help="S3 bucket holding MA summary.json artifacts")
@click.option("--db-name", required=True, help="target DB/env: dataregistry or dataregistry_qa")
@click.option("--execute", is_flag=True, default=False,
              help="actually write totals (default: dry-run preview only)")
def main(bucket, db_name, execute):
    os.environ["DATA_REGISTRY_DB_NAME"] = db_name
    engine = DataRegistryReadWriteDB().get_engine()
    s3 = boto3.client("s3", region_name="us-east-1")

    rows = [r for r in query.get_sgc_ma_results(engine) if needs_backfill(r)]
    click.echo(f"SUCCEEDED MA rows with a summary: {len(rows)} "
               f"(bucket={bucket}, db={db_name}, execute={execute})")

    written = 0
    failed = 0
    for r in rows:
        key = r["summary_json_s3_key"]
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            summary = json.loads(obj["Body"].read())
            tc, tk = totals_from_per_cohort(summary.get("per_cohort", []))
            click.echo(
                f"  {r['phenotype']:<16} {r['ancestry']:<8} "
                f"current(total_cases={r.get('total_cases')}, total_controls={r.get('total_controls')}) "
                f"-> computed(total_cases={tc}, total_controls={tk})"
            )
            if execute:
                query.update_sgc_ma_result(engine, r["phenotype"], r["ancestry"],
                                           status=r["status"], total_cases=tc, total_controls=tk)
            written += 1
        except Exception as e:
            failed += 1
            click.echo(f"  FAILED {r['phenotype']:<16} {r['ancestry']:<8}: {e}")
            continue

    click.echo(
        f"processed {len(rows)} row(s): "
        f"{written} {'written' if execute else 'previewed'}, {failed} failed"
    )
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
