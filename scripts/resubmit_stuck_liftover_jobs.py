#!/usr/bin/env python3
"""
Find liftover jobs that appear stuck (status='SUBMITTED' with no completed_at and no log
after a configurable threshold) and surface the information needed to re-submit them.

NOTE — v1 stub: There is no rerun-liftover API endpoint yet. In live mode this script
prints the file_id, relevant metadata, and the manual SQL needed to reset the row so the
operator can re-POST to /api/validate-hermes. A proper rerun endpoint is tracked as a
follow-up to the GWAS liftover v1 feature.

Usage:
    python scripts/resubmit_stuck_liftover_jobs.py [--dry-run] [--threshold-minutes 60] [--api-base http://localhost:8080]
"""

import argparse
import os
import sys

# Make the repo root importable when this script is run directly.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text

from dataregistry.api.db import DataRegistryReadWriteDB


def get_stuck_jobs(engine, threshold_minutes: int) -> list[dict]:
    """Return liftover_jobs rows that appear stuck."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT
                lj.id          AS job_id,
                lj.file_id,
                lj.source_genome_build,
                lj.target_genome_build,
                lj.submitted_at,
                lj.submitted_by,
                fu.dataset,
                fu.file_name
            FROM liftover_jobs lj
            JOIN file_uploads fu ON fu.id = lj.file_id
            WHERE lj.status = 'SUBMITTED'
              AND lj.log IS NULL
              AND lj.submitted_at < NOW() - INTERVAL :mins MINUTE
            ORDER BY lj.submitted_at
        """), {"mins": threshold_minutes}).fetchall()

    return [
        {
            "job_id": r.job_id.decode("ascii") if isinstance(r.job_id, bytes) else r.job_id,
            "file_id": r.file_id.decode("ascii") if isinstance(r.file_id, bytes) else r.file_id,
            "source_build": r.source_genome_build,
            "target_build": r.target_genome_build,
            "submitted_at": r.submitted_at,
            "submitted_by": r.submitted_by,
            "dataset": r.dataset,
            "file_name": r.file_name,
        }
        for r in rows
    ]


def _format_uuid(raw: str) -> str:
    h = raw.replace("-", "")
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:]}"


def print_job(job: dict) -> None:
    fid = _format_uuid(job["file_id"])
    jid = _format_uuid(job["job_id"])
    print(f"  file_id      : {fid}")
    print(f"  job_id       : {jid}")
    print(f"  dataset      : {job['dataset']}")
    print(f"  file_name    : {job['file_name']}")
    print(f"  source_build : {job['source_build']}")
    print(f"  target_build : {job['target_build']}")
    print(f"  submitted_at : {job['submitted_at']}")
    print(f"  submitted_by : {job['submitted_by']}")
    print()
    print("  To manually reset and retry, run:")
    print(f"    UPDATE liftover_jobs SET status='FAILED', log='manually reset', completed_at=NOW()")
    print(f"      WHERE id=UNHEX('{job['job_id'].replace('-', '')}');")
    print(f"    UPDATE file_uploads SET qc_status='LIFTOVER FAILED'")
    print(f"      WHERE id=UNHEX('{job['file_id'].replace('-', '')}');")
    print("  Then re-POST to /api/validate-hermes with the original file metadata.")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Surface stuck liftover jobs for manual recovery.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would be done without prompting.")
    parser.add_argument("--threshold-minutes", type=int, default=60,
                        help="Minutes after which a SUBMITTED job is considered stuck (default: 60).")
    parser.add_argument("--api-base", default="http://localhost:8080",
                        help="API base URL (informational only; not used in v1 stub).")
    args = parser.parse_args()

    engine = DataRegistryReadWriteDB().get_engine()
    stuck = get_stuck_jobs(engine, args.threshold_minutes)

    if not stuck:
        print("No stuck liftover jobs found.")
        return 0

    print(f"Found {len(stuck)} stuck liftover job(s) "
          f"(threshold: {args.threshold_minutes} min, api-base: {args.api_base}):\n")
    for i, job in enumerate(stuck, 1):
        print(f"[{i}/{len(stuck)}]")
        print_job(job)
        print("-" * 60)

    if args.dry_run:
        print("\n[DRY RUN] --dry-run set; no changes made.")
        return 0

    print("\nNo automated rerun endpoint exists yet (v1 stub).")
    print("Use the SQL above to reset each job, then re-POST to /api/validate-hermes.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
