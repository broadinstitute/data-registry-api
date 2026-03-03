#!/usr/bin/env python3
"""
Upload GWAS file via streaming endpoint (/api/sgc/upload-gwas-stream).
- File must be tab-delimited (TSV format) - may be compressed (.gz) or uncompressed
- Filename extension does not matter; file content is validated to ensure tab-delimited format
- Logs into user service to obtain a JWT access token.
- Sends metadata in headers; file is streamed as multipart/form-data.
- Column mapping is pulled automatically from the cohort's saved GWAS metadata.
  Upload is rejected if no GWAS metadata has been saved for the cohort.

Authentication:
  By default, the script will prompt for username and password. To skip the
  prompt, create a file called .credentials in the same directory as this script
  (i.e. scripts/.credentials) with the following format:
    Line 1: username
    Line 2: password

Usage examples:
  # QA
  ./upload_gwas_stream.py \
    --env qa \
    --cohort-name "My Cohort" \
    --dataset test_curl \
    --phenotype acne \
    --ancestry EUR \
    --sex "Male only" \
    --codes-used "L20, L21" \
    --cases 1000 \
    --controls 10000 \
    --male-proportion-cases 1.0 \
    --male-proportion-controls 0.5 \
    --assoc-test-software "regenie v4.1" \
    --assoc-test-model "mixed model logistic" \
    /path/to/gwas_file.tsv.gz

  # PRD with explicit URLs (file can have any extension, will be validated as tab-delimited)
  ./upload_gwas_stream.py \
    --api-base-url https://api.kpndataregistry.org \
    --user-service-url https://users.kpndataregistry.org \
    --cohort-name ... --dataset ... --phenotype ... \
    --sex ... --codes-used ... --cases ... --controls ... \
    --male-proportion-cases ... --male-proportion-controls ... \
    --assoc-test-software ... --assoc-test-model ... \
    file.txt
"""

import argparse
import getpass
import gzip
import json
import os
import sys
from typing import Dict, Optional

import requests
from requests_toolbelt import MultipartEncoder, MultipartEncoderMonitor
from tqdm import tqdm

# Required col_* fields that must be present in GWAS metadata (col_variant_id is optional)
REQUIRED_COL_FIELDS = [
    'col_chromosome', 'col_position', 'col_effect_allele', 'col_non_effect_allele',
    'col_beta', 'col_se', 'col_pvalue', 'col_effect_allele_freq', 'col_imputation_quality',
]

DEFAULT_API_BY_ENV = {
    "qa": "https://api.kpndataregistry.org:8000",
    "prd": "https://api.kpndataregistry.org",
}
DEFAULT_USER_SERVICE_URL = "https://users.kpndataregistry.org"


def login(user_service_url: str, username: str, password: str) -> Optional[str]:
    """Authenticate and return JWT access token (or None)."""
    try:
        r = requests.post(
            f"{user_service_url}/api/auth/login/",
            json={"username": username, "password": password, "group": "sgc"},
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


def lookup_cohort_id(api_base: str, token: str, cohort_name: str) -> Optional[str]:
    """Look up cohort ID by name from the API."""
    try:
        resp = requests.get(
            f"{api_base}/api/sgc/cohorts",
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


def fetch_gwas_metadata(api_base: str, token: str, cohort_id: str) -> Optional[Dict]:
    """Fetch GWAS metadata for a cohort. Returns None if not saved yet."""
    try:
        resp = requests.get(
            f"{api_base}/api/sgc/cohorts/{cohort_id}/gwas-metadata",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        sys.stderr.write(f"Error fetching GWAS metadata: {e}\n")
        return None


def extract_column_mapping(gwas_metadata: Dict) -> Dict[str, str]:
    """Extract col_* fields from GWAS metadata as the column mapping.

    The metadata dict contains fields like col_chromosome, col_position, etc.
    whose values are the actual column header names expected in the GWAS file.
    """
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
    p = argparse.ArgumentParser(description="Upload GWAS via streaming endpoint")
    p.add_argument("file", help="Path to GWAS file (e.g., .tsv.gz)")

    # Target API selection
    p.add_argument("--env", choices=["qa", "prd"], help="Environment: qa or prd")
    p.add_argument("--api-base-url", help="Override API base URL")
    p.add_argument("--user-service-url", default=DEFAULT_USER_SERVICE_URL, help="User service URL")

    # Required GWAS metadata
    p.add_argument("--cohort-name", required=True, help="Name of the cohort")
    p.add_argument("--dataset", required=True)
    p.add_argument("--phenotype", required=True)

    # Dataset-level metadata
    p.add_argument("--ancestry", default="EUR")
    p.add_argument("--sex", required=True,
                   choices=["Not sex stratified", "Male only", "Female only"],
                   help="Sex stratification of this dataset")
    p.add_argument("--codes-used", required=True,
                   help="Diagnosis codes used for case definition (e.g. 'L20, L21')")
    p.add_argument("--cases", type=int, required=True, help="Number of cases")
    p.add_argument("--controls", type=int, required=True, help="Number of controls")
    p.add_argument("--male-proportion-cases", type=float, required=True,
                   help="Proportion of cases that are male (0-1)")
    p.add_argument("--male-proportion-controls", type=float, required=True,
                   help="Proportion of controls that are male (0-1)")
    p.add_argument("--assoc-test-software", required=True,
                   help="Association testing software and version (e.g. 'regenie v4.1')")
    p.add_argument("--assoc-test-model", required=True,
                   help="Association testing model and settings")

    args = p.parse_args()

    # Resolve endpoints
    if args.api_base_url:
        api_base = args.api_base_url.rstrip("/")
    elif args.env:
        api_base = DEFAULT_API_BY_ENV[args.env]
    else:
        p.error("Either --env or --api-base-url is required")
        return

    user_service_url = args.user_service_url.rstrip("/")

    if not os.path.isfile(args.file):
        sys.stderr.write(f"File not found: {args.file}\n")
        sys.exit(1)

    metadata = {
        "sex": args.sex,
        "codes_used": args.codes_used,
        "male_proportion_cases": args.male_proportion_cases,
        "male_proportion_controls": args.male_proportion_controls,
        "assoc_test_software_and_version": args.assoc_test_software,
        "assoc_test_model_and_settings": args.assoc_test_model,
    }

    # Credentials
    creds = load_credentials()
    if creds:
        username, password = creds
        print("Using credentials from .credentials file")
    else:
        username = input("Username: ").strip()
        password = getpass.getpass("Password: ")

    token = login(user_service_url, username, password)
    if not token:
        sys.stderr.write("Authentication failed\n")
        sys.exit(1)

    # Look up cohort ID by name
    cohort_id = lookup_cohort_id(api_base, token, args.cohort_name)
    if not cohort_id:
        sys.stderr.write(f"Cohort not found: {args.cohort_name}\n")
        sys.exit(1)
    print(f"Found cohort '{args.cohort_name}' -> {cohort_id}")

    # Fetch GWAS metadata and extract column mapping
    print("Fetching GWAS metadata for cohort...")
    gwas_metadata = fetch_gwas_metadata(api_base, token, cohort_id)
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

    filename = os.path.basename(args.file)

    headers = {
        "Authorization": f"Bearer {token}",
        "cohort_id": cohort_id,
        "dataset": args.dataset,
        "phenotype": args.phenotype,
        "ancestry": args.ancestry,
        "filename": filename,
        "column_mapping": json.dumps(column_mapping),
    }
    headers["cases"] = str(args.cases)
    headers["controls"] = str(args.controls)
    headers["metadata"] = json.dumps(metadata)

    url = f"{api_base}/api/sgc/upload-gwas-stream"

    with open(args.file, "rb") as fh:
        # Use MultipartEncoder for streaming upload with progress tracking
        encoder = MultipartEncoder(fields={"file": (filename, fh)})

        # Create progress bar
        pbar = tqdm(
            total=encoder.len,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            desc=f"Uploading {filename}",
        )

        def progress_callback(monitor):
            pbar.update(monitor.bytes_read - pbar.n)

        monitor = MultipartEncoderMonitor(encoder, progress_callback)
        headers["Content-Type"] = monitor.content_type

        try:
            resp = requests.post(url, headers=headers, data=monitor, timeout=600)
            resp.raise_for_status()
        except requests.RequestException as e:
            sys.stderr.write(f"\nUpload failed: {e}\n")
            if hasattr(e, "response") and e.response is not None:
                sys.stderr.write(f"Response: {e.response.text}\n")
            sys.exit(1)
        finally:
            pbar.close()

    print(json.dumps(resp.json(), indent=2))


if __name__ == "__main__":
    main()
