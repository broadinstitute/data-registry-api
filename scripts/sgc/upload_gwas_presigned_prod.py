#!/usr/bin/env python3
"""
Upload GWAS file to production via presigned URL (3-step process):
  1. Request presigned URL from /api/sgc/gwas-upload-url
  2. Upload file directly to S3 using presigned URL
  3. Confirm upload via /api/sgc/confirm-gwas-upload

File must be tab-delimited (TSV format) - may be compressed (.gz) or uncompressed.
Filename extension does not matter; file content will be validated in step 3.
This method is better for large files as it uploads directly to S3.
Column mapping is pulled automatically from the cohort's saved GWAS metadata.
Upload is rejected if no GWAS metadata has been saved for the cohort.

Authentication:
  By default, the script will prompt for username and password. To skip the
  prompt, create a file called .credentials in the same directory as this script
  (i.e. scripts/.credentials) with the following format:
    Line 1: username
    Line 2: password

Metadata JSON file:
  The --metadata flag points to a JSON file containing dataset-level metadata.
  All fields below are required. Additional fields may be included and will be
  stored alongside the required metadata.
    {
      "cohort_name": "My Cohort",          # exact cohort name as registered in the portal
      "phenotype": "acne",                 # phenotype code (see /sgc/phenotypes for valid values)
      "ancestry": "EUR",                   # ancestry code (e.g. EUR, AFR, AMR, EAS, SAS, Combined)
      "sex": "All",                        # or "Male" or "Female"
      "codes_used": "L20, L21",           # diagnosis codes for case definition
      "cases": 1000,                       # number of cases (integer)
      "controls": 10000,                   # number of controls (integer)
      "male_proportion_cases": 0.5,        # proportion of cases that are male (0-1)
      "male_proportion_controls": 0.5,     # proportion of controls that are male (0-1)
      "assoc_test_software_and_version": "regenie v4.1",
      "assoc_test_model_and_settings": "mixed model logistic"
    }

Usage example:
  ./upload_gwas_presigned_prod.py \
    --dataset my_dataset \
    --metadata metadata.json \
    file.tsv.gz
"""

import argparse
import getpass
import gzip
import json
import os
import sys
import time
from typing import Dict, Optional

import requests
from tqdm import tqdm

# Required col_* fields that must be present in GWAS metadata (col_variant_id is optional)
REQUIRED_COL_FIELDS = [
    'col_chromosome', 'col_position', 'col_effect_allele', 'col_non_effect_allele',
    'col_beta', 'col_se', 'col_pvalue', 'col_effect_allele_freq', 'col_imputation_quality',
]

API_BASE = "https://api.kpndataregistry.org"
UI_BASE = "https://kpndataregistry.org"
USER_SERVICE_URL = "https://users.kpndataregistry.org"
AUTH_GROUP = "sgc-prod"


class ProgressFileReader:
    """Wraps a file object to track read progress for streaming uploads."""

    def __init__(self, file_obj, total_size: int, desc: str):
        self._file = file_obj
        self._total_size = total_size
        self._pbar = tqdm(
            total=total_size,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            desc=desc,
        )

    def read(self, size: int = -1) -> bytes:
        data = self._file.read(size)
        self._pbar.update(len(data))
        return data

    def __len__(self):
        return self._total_size

    def close(self):
        self._pbar.close()


def login(username: str, password: str) -> Optional[str]:
    """Authenticate and return JWT access token (or None)."""
    try:
        r = requests.post(
            f"{USER_SERVICE_URL}/api/auth/login/",
            json={"username": username, "password": password, "group": AUTH_GROUP},
            timeout=30,
        )
        if r.status_code == 200:
            return r.json().get("access")
        else:
            sys.stderr.write(f"Login failed ({r.status_code}): {r.text}\n")
            return None
    except requests.RequestException as e:
        sys.stderr.write(f"Login error: {e}\n")
        return None


def load_credentials() -> Optional[tuple]:
    """Load credentials from .credentials file if it exists.

    File format: username on first line, password on second line.
    """
    cred_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".credentials")
    if os.path.isfile(cred_path):
        with open(cred_path, "r") as f:
            lines = f.read().strip().splitlines()
        if len(lines) >= 2:
            return lines[0].strip(), lines[1].strip()
    return None


def fetch_valid_sex_values(token: str) -> list:
    """Fetch the list of valid sex stratification values from the API."""
    try:
        resp = requests.get(
            f"{API_BASE}/api/sgc/gwas-sex-values",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        sys.stderr.write(f"Error fetching sex values: {e}\n")
        return []


def fetch_valid_ancestries(token: str) -> list:
    """Fetch the list of valid ancestry codes from the API."""
    try:
        resp = requests.get(
            f"{API_BASE}/api/sgc/gwas-ancestries",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        sys.stderr.write(f"Error fetching ancestries: {e}\n")
        return []


def fetch_valid_phenotypes(token: str) -> set:
    """Fetch the set of valid phenotype codes from the API."""
    try:
        resp = requests.get(
            f"{API_BASE}/api/sgc/phenotypes",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        resp.raise_for_status()
        return {p["phenotype_code"] for p in resp.json()}
    except requests.RequestException as e:
        sys.stderr.write(f"Error fetching phenotypes: {e}\n")
        return set()


def lookup_cohort_id(token: str, cohort_name: str) -> Optional[str]:
    """Look up cohort ID by name from the API."""
    try:
        resp = requests.get(
            f"{API_BASE}/api/sgc/cohorts",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        resp.raise_for_status()
        for cohort in resp.json():
            if cohort["name"] == cohort_name:
                return cohort["id"]
        return None
    except requests.RequestException as e:
        sys.stderr.write(f"Error looking up cohort: {e}\n")
        return None


def check_for_existing_gwas_file(token: str, cohort_id: str,
                                  dataset: str, phenotype: str, filename: str) -> Optional[Dict]:
    """Return the existing GWAS file record if one matches dataset/phenotype/filename, else None."""
    try:
        resp = requests.get(
            f"{API_BASE}/api/sgc/gwas-files/{cohort_id}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        resp.raise_for_status()
        for f in resp.json():
            if f.get("dataset") == dataset and f.get("phenotype") == phenotype and f.get("file_name") == filename:
                return f
        return None
    except requests.RequestException as e:
        sys.stderr.write(f"Error checking for existing GWAS file: {e}\n")
        return None


def fetch_gwas_metadata(token: str, cohort_id: str) -> Optional[Dict]:
    """Fetch GWAS metadata for a cohort. Returns None if not saved yet."""
    try:
        resp = requests.get(
            f"{API_BASE}/api/sgc/cohorts/{cohort_id}/gwas-metadata",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        sys.stderr.write(f"Error fetching GWAS metadata: {e}\n")
        return None


def extract_column_mapping(gwas_metadata: Dict) -> Dict[str, str]:
    """Extract col_* fields from GWAS metadata as the column mapping."""
    meta = gwas_metadata.get('metadata') or {}
    return {k: v for k, v in meta.items() if k.startswith('col_') and v}


def read_first_line(file_path: str) -> str:
    """Read the first line of a file, handling gzip compression via magic bytes."""
    with open(file_path, 'rb') as raw:
        magic = raw.read(2)
    if magic == b'\x1f\x8b':
        with gzip.open(file_path, 'rt') as f:
            return f.readline().strip()
    else:
        with open(file_path, 'r') as f:
            return f.readline().strip()


# Required metadata fields and their expected types
REQUIRED_METADATA_FIELDS = {
    'cohort_name': str,
    'phenotype': str,
    'ancestry': str,
    'sex': str,
    'codes_used': str,
    'cases': int,
    'controls': int,
    'male_proportion_cases': (int, float),
    'male_proportion_controls': (int, float),
    'assoc_test_software_and_version': str,
    'assoc_test_model_and_settings': str,
}


def validate_metadata(metadata: Dict, valid_sex_values: list) -> list:
    """Validate that the metadata dict contains all required fields with correct types.

    Returns a list of error messages (empty if valid).
    """
    errors = []
    for field, expected_type in REQUIRED_METADATA_FIELDS.items():
        if field not in metadata:
            errors.append(f"Missing required field: '{field}'")
        elif not isinstance(metadata[field], expected_type):
            errors.append(
                f"Field '{field}' must be {expected_type.__name__ if isinstance(expected_type, type) else ' or '.join(t.__name__ for t in expected_type)}, "
                f"got {type(metadata[field]).__name__}"
            )
    if 'sex' in metadata and metadata['sex'] not in valid_sex_values:
        errors.append(f"Field 'sex' must be one of: {valid_sex_values}")

    # Cross-field validation: sex vs male_proportion_cases/controls
    sex = metadata.get('sex')
    mpc = metadata.get('male_proportion_cases')
    mpctrl = metadata.get('male_proportion_controls')
    if sex == 'Male':
        if mpc is not None and mpc != 1:
            errors.append("male_proportion_cases must be 1 for sex='Male'")
        if mpctrl is not None and mpctrl != 1:
            errors.append("male_proportion_controls must be 1 for sex='Male'")
    elif sex == 'Female':
        if mpc is not None and mpc != 0:
            errors.append("male_proportion_cases must be 0 for sex='Female'")
        if mpctrl is not None and mpctrl != 0:
            errors.append("male_proportion_controls must be 0 for sex='Female'")
    elif sex == 'All':
        if mpc is not None and mpc in (0, 1):
            errors.append(
                "male_proportion_cases cannot be 0 or 1 for sex='All' "
                "(use sex='Male' or sex='Female' for single-sex cohorts)"
            )
        if mpctrl is not None and mpctrl in (0, 1):
            errors.append(
                "male_proportion_controls cannot be 0 or 1 for sex='All' "
                "(use sex='Male' or sex='Female' for single-sex cohorts)"
            )
    return errors


def validate_file_headers(file_path: str, column_mapping: Dict[str, str]) -> tuple:
    """Check that the file's TSV header row contains all required column names.

    Returns (is_valid, missing) where missing is a list of (field, expected_header) tuples.
    """
    file_headers = set(read_first_line(file_path).split('\t'))
    missing = [
        (field, header)
        for field, header in column_mapping.items()
        if field in REQUIRED_COL_FIELDS and header not in file_headers
    ]
    return len(missing) == 0, missing


def main():
    p = argparse.ArgumentParser(description="Upload GWAS to production via presigned URL (3-step)")
    p.add_argument("file", help="Path to GWAS file (e.g., .tsv.gz)")
    p.add_argument("--dataset", required=True)
    p.add_argument("--metadata", required=True,
                   help="Path to JSON file with dataset-level metadata (must include cohort_name, phenotype, ancestry)")
    p.add_argument("--validate", action="store_true",
                   help="Submit a batch QA validation job after upload and poll until it completes")

    args = p.parse_args()

    if not os.path.isfile(args.file):
        sys.stderr.write(f"File not found: {args.file}\n")
        sys.exit(1)

    file_size = os.path.getsize(args.file)
    filename = os.path.basename(args.file)

    with open(args.metadata, "r") as f:
        metadata = json.load(f)

    # Credentials
    creds = load_credentials()
    if creds:
        username, password = creds
        print("Using credentials from .credentials file")
    else:
        username = input("Username: ").strip()
        password = getpass.getpass("Password: ")

    token = login(username, password)
    if not token:
        sys.stderr.write("Authentication failed\n")
        sys.exit(1)

    # Fetch valid sex values and validate metadata
    valid_sex_values = fetch_valid_sex_values(token)
    if not valid_sex_values:
        sys.stderr.write("Could not fetch valid sex values from API\n")
        sys.exit(1)

    errors = validate_metadata(metadata, valid_sex_values)
    if errors:
        sys.stderr.write(f"Metadata validation errors in {args.metadata}:\n")
        for err in errors:
            sys.stderr.write(f"  - {err}\n")
        sys.exit(1)

    cohort_name = metadata.pop("cohort_name")
    phenotype = metadata.pop("phenotype")
    ancestry = metadata.pop("ancestry")
    cases = metadata.pop("cases")
    controls = metadata.pop("controls")

    # Validate ancestry against allowed values
    print("Validating ancestry...")
    valid_ancestries = fetch_valid_ancestries(token)
    if not valid_ancestries:
        sys.stderr.write("Could not fetch valid ancestries from API\n")
        sys.exit(1)
    if ancestry not in valid_ancestries:
        sys.stderr.write(
            f"Invalid ancestry: '{ancestry}'\n"
            f"Valid ancestries: {valid_ancestries}\n"
            f"If the ancestry you need is not listed, please contact the SGC so it can be added.\n"
        )
        sys.exit(1)
    print(f"Ancestry '{ancestry}' is valid.")

    # Validate phenotype against allowed values
    print("Validating phenotype...")
    valid_phenotypes = fetch_valid_phenotypes(token)
    if not valid_phenotypes:
        sys.stderr.write("Could not fetch valid phenotypes from API\n")
        sys.exit(1)
    if phenotype not in valid_phenotypes:
        sys.stderr.write(
            f"Invalid phenotype: '{phenotype}'\n"
            f"Valid phenotypes: {sorted(valid_phenotypes)}\n"
            f"See {UI_BASE}/sgc/phenotypes for the full list of allowed phenotypes.\n"
        )
        sys.exit(1)
    print(f"Phenotype '{phenotype}' is valid.")

    # Look up cohort ID by name
    cohort_id = lookup_cohort_id(token, cohort_name)
    if not cohort_id:
        sys.stderr.write(f"Cohort not found: '{cohort_name}'\n")
        sys.exit(1)
    print(f"Found cohort '{cohort_name}' -> {cohort_id}")

    # Check for an existing GWAS file with the same dataset/phenotype/filename before uploading
    existing = check_for_existing_gwas_file(token, cohort_id, args.dataset, phenotype, filename)
    if existing:
        sys.stderr.write(
            f"A GWAS file already exists for dataset='{args.dataset}', phenotype='{phenotype}', "
            f"file='{filename}' (id: {existing['id']}).\n"
            f"Delete the existing file before uploading a new one.\n"
        )
        sys.exit(1)

    # Fetch GWAS metadata and extract column mapping
    print("Fetching GWAS metadata for cohort...")
    gwas_metadata = fetch_gwas_metadata(token, cohort_id)
    if not gwas_metadata:
        sys.stderr.write(
            "No GWAS metadata found for this cohort. "
            "Please save GWAS metadata (including column headings) before uploading files.\n"
        )
        sys.exit(1)

    column_mapping = extract_column_mapping(gwas_metadata)
    if not column_mapping:
        sys.stderr.write(
            "GWAS metadata exists but contains no column headings. "
            "Please complete the column headings in the GWAS metadata before uploading.\n"
        )
        sys.exit(1)

    print(f"Column mapping from GWAS metadata: {json.dumps(column_mapping, indent=2)}")

    # Validate file headers against the cohort's column mapping
    print("Validating file headers against GWAS metadata column mapping...")
    is_valid, missing = validate_file_headers(args.file, column_mapping)
    if not is_valid:
        sys.stderr.write("File headers do not match the cohort's GWAS metadata column mapping:\n")
        for field, expected in missing:
            sys.stderr.write(f"  {field}: expected column '{expected}' not found in file\n")
        sys.exit(1)
    print("File headers validated successfully.")

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    # STEP 1: Request presigned URL
    print("[Step 1/3] Requesting presigned URL...")
    request_data = {
        "cohort_id": cohort_id,
        "dataset": args.dataset,
        "phenotype": phenotype,
        "ancestry": ancestry,
        "filename": filename,
        "column_mapping": column_mapping,
        "metadata": metadata,
        "cases": cases,
        "controls": controls,
    }

    try:
        resp = requests.post(
            f"{API_BASE}/api/sgc/gwas-upload-url",
            headers=headers,
            json=request_data,
            timeout=30,
        )
        resp.raise_for_status()
        presigned_data = resp.json()
        presigned_url = presigned_data["presigned_url"]
        s3_key = presigned_data["s3_key"]
        print(f"  Presigned URL obtained. S3 key: {s3_key}")
    except requests.RequestException as e:
        sys.stderr.write(f"Failed to get presigned URL: {e}\n")
        if hasattr(e, "response") and e.response is not None:
            sys.stderr.write(f"Response: {e.response.text}\n")
        sys.exit(1)

    # STEP 2: Upload file to S3
    print(f"[Step 2/3] Uploading file to S3 ({file_size / (1024*1024):.2f} MB)...")
    # Note: Don't set Content-Type header - it must match what was specified when
    # generating the presigned URL (which didn't specify one)
    #
    # S3 uploads occasionally stall mid-transfer (~10% of the time from high-latency
    # connections). The presigned URL stays valid for the retry window, and a failed
    # PUT leaves no partial object in S3, so retrying the same URL is safe.
    UPLOAD_TIMEOUT = 120   # seconds; normal upload completes in <30s
    MAX_UPLOAD_ATTEMPTS = 3

    for attempt in range(1, MAX_UPLOAD_ATTEMPTS + 1):
        if attempt > 1:
            sys.stderr.write(f"  Retrying upload (attempt {attempt}/{MAX_UPLOAD_ATTEMPTS})...\n")
            time.sleep(5)
        try:
            with open(args.file, "rb") as fh:
                progress_reader = ProgressFileReader(fh, file_size, f"Uploading {filename}")
                try:
                    s3_resp = requests.put(
                        presigned_url,
                        data=progress_reader,
                        headers={"Content-Length": str(file_size)},
                        timeout=UPLOAD_TIMEOUT,
                    )
                    s3_resp.raise_for_status()
                finally:
                    progress_reader.close()
            print(f"  File uploaded to S3 (HTTP {s3_resp.status_code})")
            break  # success
        except requests.RequestException as e:
            if hasattr(e, "response") and e.response is not None and 400 <= e.response.status_code < 500:
                sys.stderr.write(f"\nS3 upload rejected (HTTP {e.response.status_code}): {e.response.text}\n")
                sys.exit(1)
            if attempt < MAX_UPLOAD_ATTEMPTS:
                sys.stderr.write(f"\n  Upload attempt {attempt} failed: {e}\n")
            else:
                sys.stderr.write(f"\nS3 upload failed after {MAX_UPLOAD_ATTEMPTS} attempts: {e}\n")
                if hasattr(e, "response") and e.response is not None:
                    sys.stderr.write(f"Response: {e.response.text}\n")
                sys.exit(1)

    # STEP 3: Confirm upload with API
    print("[Step 3/3] Confirming upload with API...")
    confirm_data = {
        "cohort_id": cohort_id,
        "dataset": args.dataset,
        "phenotype": phenotype,
        "ancestry": ancestry,
        "filename": filename,
        "file_size": file_size,
        "s3_key": s3_key,
        "column_mapping": column_mapping,
        "metadata": metadata,
        "cases": cases,
        "controls": controls,
    }

    try:
        confirm_resp = requests.post(
            f"{API_BASE}/api/sgc/confirm-gwas-upload",
            headers=headers,
            json=confirm_data,
            timeout=30,
        )
        confirm_resp.raise_for_status()
        result = confirm_resp.json()
        print(json.dumps(result, indent=2))
        file_id = result.get("file_id")
        if file_id and args.validate:
            start_validation(token, file_id)
    except requests.RequestException as e:
        sys.stderr.write(f"Confirm upload failed: {e}\n")
        if hasattr(e, "response") and e.response is not None:
            sys.stderr.write(f"Response: {e.response.text}\n")
        sys.exit(1)


def start_validation(token: str, file_id: str):
    """Kick off the batch QA validation job for the uploaded file, then poll until done."""
    print("\nSubmitting batch QA validation job...")
    try:
        r = requests.post(
            f"{API_BASE}/api/sgc/gwas-validate/{file_id}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        r.raise_for_status()
    except requests.RequestException as e:
        sys.stderr.write(f"Failed to submit validation job: {e}\n")
        sys.exit(1)

    result = r.json()
    print(f"Validation job submitted: {result.get('validation_job_id')}")
    poll_validation_progress(token, file_id)


def fetch_errors_url(token: str, file_id: str) -> Optional[str]:
    """Fetch a presigned download URL for the full validation error log."""
    try:
        r = requests.get(
            f"{API_BASE}/api/sgc/gwas-validate/{file_id}/errors",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        if r.ok:
            return r.json().get("errors_url")
    except requests.RequestException:
        pass
    return None


def poll_validation_progress(token: str, file_id: str, poll_interval: int = 10):
    """Poll the validation progress endpoint until the batch QA job completes or fails."""
    url = f"{API_BASE}/api/sgc/gwas-validate/{file_id}/progress"
    headers = {"Authorization": f"Bearer {token}"}

    print("Waiting for batch QA validation to start...")

    pbar = None
    last_pct = 0.0
    last_live_status = ""

    while True:
        try:
            r = requests.get(url, headers=headers, timeout=30)
        except requests.RequestException as e:
            sys.stderr.write(f"Error polling validation progress: {e}\n")
            time.sleep(poll_interval)
            continue

        if r.status_code == 404:
            time.sleep(poll_interval)
            continue

        if not r.ok:
            sys.stderr.write(f"Validation progress endpoint returned {r.status_code}: {r.text}\n")
            time.sleep(poll_interval)
            continue

        jobs = r.json().get("validation_jobs", [])
        if not jobs:
            time.sleep(poll_interval)
            continue

        latest = jobs[0]
        db_status = latest.get("status", "")       # SUBMITTED, RUNNING, COMPLETED, FAILED
        live = latest.get("live_progress") or {}   # populated from S3 while running

        live_status = live.get("status", "")
        pct = live.get("percent_complete", 0.0)
        total_rows = live.get("total_rows") or latest.get("total_rows") or 0
        rows_processed = live.get("rows_processed", 0)
        errors_found = live.get("errors_found") if live else (latest.get("errors_found") or 0)

        # Show status messages before the progress bar is available
        if pbar is None:
            if live_status == "downloading":
                dl_bytes = live.get("download_bytes", 0)
                dl_total = live.get("download_total", 0)
                if dl_total:
                    msg = f"  Downloading file to validator: {dl_bytes / (1024*1024):.0f} MB / {dl_total / (1024*1024):.0f} MB"
                else:
                    msg = "  Downloading file to validator..."
                print(msg)
            elif live_status == "counting" and last_live_status != "counting":
                print("  Counting rows...")
            last_live_status = live_status

        if pbar is None and total_rows and live_status == "running":
            pbar = tqdm(total=100, unit="%", desc="Validating",
                        bar_format="{l_bar}{bar}| {n:.1f}/{total}%  {postfix}")

        if pbar is not None:
            delta = pct - last_pct
            if delta > 0:
                pbar.update(delta)
                last_pct = pct
            pbar.set_postfix(errors=errors_found, rows=f"{rows_processed:,}/{total_rows:,}")

        if db_status in ("COMPLETED", "FAILED"):
            if pbar is not None:
                pbar.update(100.0 - last_pct)
                pbar.close()

            print(f"\nValidation {db_status.lower()}.")
            total = latest.get("total_rows")
            print(f"  Rows checked : {total:,}" if isinstance(total, int) else f"  Rows checked : {total}")
            print(f"  Errors found : {latest.get('errors_found', 'N/A')}")

            error_summary = latest.get("error_summary") or live.get("error_samples")
            if error_summary:
                print(f"\n  Sample errors (up to {len(error_summary)} shown):")
                for err in error_summary:
                    print(f"    Row {err.get('row')}: [{err.get('column')}] {err.get('error')} (value: {err.get('value')!r})")

            if latest.get("errors_found"):
                errors_url = fetch_errors_url(token, file_id)
                if errors_url:
                    print(f"\n  Full error log (TSV, expires in 1 hour):\n  {errors_url}")

            if db_status == "FAILED":
                sys.exit(1)
            return

        time.sleep(poll_interval)


if __name__ == "__main__":
    main()
