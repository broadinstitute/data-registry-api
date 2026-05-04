#!/usr/bin/env python3
"""
SGC GWAS row-level validation batch job.

Downloads a GWAS file from S3, validates every data row against the column
mapping rules from the SGC cohort analysis plan, and writes periodic progress
updates to an S3 JSON file so the API can report percent-complete.

Validation rules (per mapped column):
  CHR  (col_chromosome)          : 1-23 or "X" (23 accepted as alias for X)
  BP   (col_position)            : positive integer
  EA   (col_effect_allele)       : non-empty, chars in {A,C,T,G,-,N}
  OA   (col_non_effect_allele)   : same as EA
  EAF  (col_effect_allele_freq)  : numeric, 0 <= v <= 1
  BETA (col_beta)                : numeric (any real)
  SE   (col_se)                  : numeric, >= 0
  P    (col_pvalue)              : numeric, 0 <= v <= 1
  INFO (col_imputation_quality)  : numeric, >= 0 (no upper bound; some methods exceed 1)
  ID   (col_variant_id)          : optional; if present, non-empty string
  N    (col_variant_n)           : optional; numeric, > 0
"""

import csv
import gzip
import json
import os
import sys
import tempfile
import threading
import time
from datetime import datetime, timezone

import boto3
import click

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

VALID_CHR = {str(i) for i in range(1, 24)} | {"X"}
ALLELE_CHARS = set("ACTGactg-N")
MAX_ERROR_SAMPLES = 100
PROGRESS_INTERVAL = 1000          # rows between S3 progress writes
DOWNLOAD_PROGRESS_INTERVAL = 10 * 1024 * 1024  # bytes between S3 progress writes during download (10 MB)


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
    return "Must be 1-23 or X"


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
    if float(value) >= 0.0:
        return None
    return "Must be >= 0"


def validate_variant_id(value: str) -> str | None:
    if value and value.strip():
        return None
    return "Must be non-empty when present"


def validate_variant_n(value: str, max_n: int = 0) -> str | None:
    if not _is_numeric(value):
        return "Must be numeric"
    v = float(value)
    if v <= 0:
        return "Must be > 0"
    if max_n > 0 and v > max_n:
        return f"N ({int(v)}) exceeds cases + controls ({max_n})"
    return None


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
    "col_variant_n":           (validate_variant_n, False),
}


# ---------------------------------------------------------------------------
# Progress helper
# ---------------------------------------------------------------------------

class ProgressTracker:
    """Accumulates validation state and writes periodic progress to S3."""

    BACKGROUND_WRITE_INTERVAL = 5  # seconds between background S3 progress flushes

    def __init__(self, s3_client, bucket: str, progress_key: str, total_rows: int,
                 error_file_path: str):
        self._s3 = s3_client
        self._bucket = bucket
        self._progress_key = progress_key
        self._errors_key = progress_key.replace("progress.json", "errors.tsv")
        self._error_file_path = error_file_path
        self._error_file = open(error_file_path, "w", newline="", encoding="utf-8")
        self._error_file.write("row\tcolumn\tvalue\terror\n")
        self.total_rows = total_rows
        self.rows_processed = 0
        self.errors_found = 0
        self.error_samples: list[dict] = []
        self.started_at = datetime.now(timezone.utc).isoformat()
        self.status = "running"
        self.download_bytes = 0
        self.download_total = 0
        self._dirty = False
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._writer_thread = threading.Thread(target=self._background_writer, daemon=True)
        self._writer_thread.start()

    def _background_writer(self):
        """Flush dirty progress to S3 every BACKGROUND_WRITE_INTERVAL seconds."""
        while not self._stop_event.wait(timeout=self.BACKGROUND_WRITE_INTERVAL):
            with self._lock:
                if not self._dirty:
                    continue
                self._dirty = False
                payload = self._build_payload()
            self._put_progress(payload)  # S3 call outside the lock

    def mark_dirty(self):
        """Signal that progress state has changed and should be flushed on the next interval."""
        with self._lock:
            self._dirty = True

    def record_error(self, row_num: int, column: str, value: str, error: str):
        self.errors_found += 1
        # Write every error to the full TSV log
        self._error_file.write(f"{row_num}\t{column}\t{value[:100]}\t{error}\n")
        # Keep a capped sample for the progress JSON
        if len(self.error_samples) < MAX_ERROR_SAMPLES:
            self.error_samples.append({
                "row": row_num,
                "column": column,
                "value": value[:100],
                "error": error,
            })

    def _build_payload(self) -> dict:
        pct = (self.rows_processed / self.total_rows * 100) if self.total_rows > 0 else 0.0
        payload = {
            "status": self.status,
            "total_rows": self.total_rows,
            "rows_processed": self.rows_processed,
            "percent_complete": round(pct, 2),
            "errors_found": self.errors_found,
            "error_samples": self.error_samples,
            "started_at": self.started_at,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        if self.status == "downloading":
            payload["download_bytes"] = self.download_bytes
            payload["download_total"] = self.download_total
        return payload

    def _put_progress(self, payload: dict):
        """Write a progress payload to S3 (blocking). Always called outside the lock."""
        self._s3.put_object(
            Bucket=self._bucket,
            Key=self._progress_key,
            Body=json.dumps(payload).encode("utf-8"),
            ContentType="application/json",
        )

    def write_progress(self):
        """Synchronously write progress to S3. Use for explicit status transitions."""
        with self._lock:
            self._dirty = False
            payload = self._build_payload()
        self._put_progress(payload)

    def finalize(self, status: str = "completed"):
        self._stop_event.set()
        self._writer_thread.join()
        self.status = status
        self._error_file.close()
        if self.errors_found > 0:
            with open(self._error_file_path, "rb") as f:
                self._s3.put_object(
                    Bucket=self._bucket,
                    Key=self._errors_key,
                    Body=f,
                    ContentType="text/tab-separated-values",
                )
            print(f"Full error log written to s3://{self._bucket}/{self._errors_key}")
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


def validate_file(file_path: str, column_mapping: dict, tracker: ProgressTracker,
                  max_n: int = 0):
    """Stream through the GWAS file and validate every row."""

    # Build a lookup: file_header_name -> (validator_func, mapping_key)
    validators = {}
    n_header = column_mapping.get("col_variant_n")
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

                if mapping_key == "col_variant_n":
                    error = validator_fn(value, max_n)
                else:
                    error = validator_fn(value)
                if error:
                    tracker.record_error(row_idx, header_name, value, error)

            tracker.rows_processed += 1

            if tracker.rows_processed % PROGRESS_INTERVAL == 0:
                tracker.mark_dirty()

    tracker.finalize("completed")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

@click.command()
@click.option("--s3-path", required=True, help="S3 key of the GWAS file")
@click.option("--column-mapping", required=True, help="JSON string of column mappings")
@click.option("--progress-s3-key", required=True, help="S3 key for progress JSON output")
@click.option("--bucket", required=True, help="S3 bucket name")
@click.option("--max-n", default=0, type=int, help="Maximum allowed N (cases + controls); 0 = no limit")
def main(s3_path: str, column_mapping: str, progress_s3_key: str, bucket: str, max_n: int):
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
        error_file_path = os.path.join(tmpdir, "errors.tsv")

        # Create tracker early so we can report status during download and count phases
        tracker = ProgressTracker(s3_client, bucket, progress_s3_key, 0, error_file_path)

        # Phase 1: download with periodic progress updates
        file_size = s3_client.head_object(Bucket=bucket, Key=s3_path)["ContentLength"]
        tracker.download_total = file_size
        tracker.status = "downloading"
        tracker.write_progress()
        print(f"Downloading s3://{bucket}/{s3_path} -> {local_file} ({file_size / (1024*1024):.1f} MB)")

        def _download_callback(bytes_transferred):
            tracker.download_bytes += bytes_transferred
            tracker.mark_dirty()

        s3_client.download_file(bucket, s3_path, local_file, Callback=_download_callback)

        # Phase 2: count rows
        tracker.status = "counting"
        tracker.write_progress()
        print("Counting rows...")
        total_rows = count_lines(local_file)
        tracker.total_rows = total_rows
        print(f"Total data rows: {total_rows}")

        # Phase 3: validate
        tracker.status = "running"
        tracker.write_progress()

        try:
            validate_file(local_file, col_map, tracker, max_n=max_n)
        except Exception as e:
            print(f"Validation failed: {e}", file=sys.stderr)
            tracker.finalize("failed")
            sys.exit(1)

    print(f"Validation complete. {tracker.errors_found} errors in {tracker.rows_processed} rows.")


if __name__ == "__main__":
    main()
