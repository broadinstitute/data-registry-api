#!/usr/bin/env python3
"""
SGC GWAS row-level validation batch job.

Downloads a GWAS file from S3, validates every data row against the column
mapping rules from the SGC cohort analysis plan, and writes periodic progress
updates to an S3 JSON file so the API can report percent-complete.

Validation rules (per mapped column):
  CHR  (col_chromosome)          : 1-22 or "X"
  BP   (col_position)            : positive integer
  EA   (col_effect_allele)       : non-empty, chars in {A,C,T,G,-,N}
  OA   (col_non_effect_allele)   : same as EA
  EAF  (col_effect_allele_freq)  : numeric, 0 <= v <= 1
  BETA (col_beta)                : numeric (any real)
  SE   (col_se)                  : numeric, >= 0
  P    (col_pvalue)              : numeric, 0 <= v <= 1
  INFO (col_imputation_quality)  : numeric, 0 <= v <= 1
  ID   (col_variant_id)          : optional; if present, non-empty string
"""

import csv
import gzip
import json
import os
import sys
import tempfile
import time
from datetime import datetime, timezone

import boto3
import click

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

VALID_CHR = {str(i) for i in range(1, 23)} | {"X"}
ALLELE_CHARS = set("ACTGactg-N")
MAX_ERROR_SAMPLES = 100
PROGRESS_INTERVAL = 1000  # rows between S3 progress writes


# ---------------------------------------------------------------------------
# Validators — one per logical field
# ---------------------------------------------------------------------------

def _is_numeric(value: str) -> bool:
    try:
        float(value)
        return True
    except (ValueError, TypeError):
        return False


def validate_chromosome(value: str) -> str | None:
    if value.upper() in VALID_CHR:
        return None
    return "Must be 1-22 or X"


def validate_position(value: str) -> str | None:
    try:
        v = int(value)
        if v > 0:
            return None
        return "Must be a positive integer"
    except (ValueError, TypeError):
        return "Must be a positive integer"


def validate_allele(value: str) -> str | None:
    if not value:
        return "Must be non-empty"
    if set(value) <= ALLELE_CHARS:
        return None
    return "Must contain only A, C, T, G, -, or N characters"


def validate_eaf(value: str) -> str | None:
    if not _is_numeric(value):
        return "Must be numeric"
    v = float(value)
    if 0.0 <= v <= 1.0:
        return None
    return "Must be between 0 and 1"


def validate_beta(value: str) -> str | None:
    if _is_numeric(value):
        return None
    return "Must be numeric"


def validate_se(value: str) -> str | None:
    if not _is_numeric(value):
        return "Must be numeric"
    if float(value) >= 0.0:
        return None
    return "Must be >= 0"


def validate_pvalue(value: str) -> str | None:
    if not _is_numeric(value):
        return "Must be numeric"
    v = float(value)
    if 0.0 <= v <= 1.0:
        return None
    return "Must be between 0 and 1"


def validate_info(value: str) -> str | None:
    if not _is_numeric(value):
        return "Must be numeric"
    v = float(value)
    if 0.0 <= v <= 1.0:
        return None
    return "Must be between 0 and 1"


def validate_variant_id(value: str) -> str | None:
    if value and value.strip():
        return None
    return "Must be non-empty when present"


# Map from column-mapping key -> (validator_func, is_required)
FIELD_VALIDATORS = {
    "col_chromosome":          (validate_chromosome, True),
    "col_position":            (validate_position, True),
    "col_effect_allele":       (validate_allele, True),
    "col_non_effect_allele":   (validate_allele, True),
    "col_effect_allele_freq":  (validate_eaf, True),
    "col_beta":                (validate_beta, True),
    "col_se":                  (validate_se, True),
    "col_pvalue":              (validate_pvalue, True),
    "col_imputation_quality":  (validate_info, True),
    "col_variant_id":          (validate_variant_id, False),
}


# ---------------------------------------------------------------------------
# Progress helper
# ---------------------------------------------------------------------------

class ProgressTracker:
    """Accumulates validation state and writes periodic progress to S3."""

    def __init__(self, s3_client, bucket: str, progress_key: str, total_rows: int):
        self._s3 = s3_client
        self._bucket = bucket
        self._progress_key = progress_key
        self.total_rows = total_rows
        self.rows_processed = 0
        self.errors_found = 0
        self.error_samples: list[dict] = []
        self.started_at = datetime.now(timezone.utc).isoformat()
        self.status = "running"

    def record_error(self, row_num: int, column: str, value: str, error: str):
        self.errors_found += 1
        if len(self.error_samples) < MAX_ERROR_SAMPLES:
            self.error_samples.append({
                "row": row_num,
                "column": column,
                "value": value[:100],  # truncate long values
                "error": error,
            })

    def _build_payload(self) -> dict:
        pct = (self.rows_processed / self.total_rows * 100) if self.total_rows > 0 else 0.0
        return {
            "status": self.status,
            "total_rows": self.total_rows,
            "rows_processed": self.rows_processed,
            "percent_complete": round(pct, 2),
            "errors_found": self.errors_found,
            "error_samples": self.error_samples,
            "started_at": self.started_at,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

    def write_progress(self):
        payload = json.dumps(self._build_payload())
        self._s3.put_object(
            Bucket=self._bucket,
            Key=self._progress_key,
            Body=payload.encode("utf-8"),
            ContentType="application/json",
        )

    def finalize(self, status: str = "completed"):
        self.status = status
        self.write_progress()


# ---------------------------------------------------------------------------
# Core validation
# ---------------------------------------------------------------------------

def count_lines(file_path: str) -> int:
    """Count data rows (excluding header) in a possibly-gzipped file."""
    opener = gzip.open if _is_gzipped(file_path) else open
    count = 0
    with opener(file_path, "rt") as fh:
        for _ in fh:
            count += 1
    return max(count - 1, 0)  # subtract header


def _is_gzipped(file_path: str) -> bool:
    with open(file_path, "rb") as f:
        return f.read(2) == b"\x1f\x8b"


def validate_file(file_path: str, column_mapping: dict, tracker: ProgressTracker):
    """Stream through the GWAS file and validate every row."""

    # Build a lookup: file_header_name -> (validator_func, mapping_key)
    validators = {}
    opener = gzip.open if _is_gzipped(file_path) else open

    with opener(file_path, "rt") as fh:
        reader = csv.DictReader(fh, delimiter="\t")

        if reader.fieldnames is None:
            tracker.finalize("failed")
            return

        # Build validators for columns present in this file
        for mapping_key, header_name in column_mapping.items():
            if mapping_key in FIELD_VALIDATORS and header_name in reader.fieldnames:
                validator_fn, _required = FIELD_VALIDATORS[mapping_key]
                validators[header_name] = (validator_fn, mapping_key)

        # Validate rows
        for row_idx, row in enumerate(reader, start=2):  # row 1 is header
            for header_name, (validator_fn, mapping_key) in validators.items():
                value = row.get(header_name, "")
                if value == "" or value is None:
                    # For optional fields, skip empty values
                    _, required = FIELD_VALIDATORS[mapping_key]
                    if not required:
                        continue
                    tracker.record_error(row_idx, header_name, "", "Value is missing")
                    continue

                error = validator_fn(value)
                if error:
                    tracker.record_error(row_idx, header_name, value, error)

            tracker.rows_processed += 1

            if tracker.rows_processed % PROGRESS_INTERVAL == 0:
                tracker.write_progress()

    tracker.finalize("completed")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

@click.command()
@click.option("--s3-path", required=True, help="S3 key of the GWAS file")
@click.option("--column-mapping", required=True, help="JSON string of column mappings")
@click.option("--progress-s3-key", required=True, help="S3 key for progress JSON output")
@click.option("--bucket", required=True, help="S3 bucket name")
def main(s3_path: str, column_mapping: str, progress_s3_key: str, bucket: str):
    s3_client = boto3.client("s3")

    # Parse column mapping
    try:
        col_map = json.loads(column_mapping)
    except json.JSONDecodeError as e:
        print(f"Invalid column_mapping JSON: {e}", file=sys.stderr)
        sys.exit(1)

    # Download file to temp directory
    with tempfile.TemporaryDirectory() as tmpdir:
        local_file = os.path.join(tmpdir, os.path.basename(s3_path))
        print(f"Downloading s3://{bucket}/{s3_path} -> {local_file}")
        s3_client.download_file(bucket, s3_path, local_file)

        # Count total rows for progress tracking
        print("Counting rows...")
        total_rows = count_lines(local_file)
        print(f"Total data rows: {total_rows}")

        # Set up progress tracker and run validation
        tracker = ProgressTracker(s3_client, bucket, progress_s3_key, total_rows)
        tracker.write_progress()  # initial "running" status

        try:
            validate_file(local_file, col_map, tracker)
        except Exception as e:
            print(f"Validation failed: {e}", file=sys.stderr)
            tracker.finalize("failed")
            sys.exit(1)

    print(f"Validation complete. {tracker.errors_found} errors in {tracker.rows_processed} rows.")


if __name__ == "__main__":
    main()
