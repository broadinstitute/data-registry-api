#!/usr/bin/env python3
"""
Upload GWAS file via presigned URL (3-step process):
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
      "sex": "All",                        # or "Male" or "Female"
      "codes_used": "L20, L21",           # diagnosis codes for case definition
      "cases": 1000,                       # number of cases (integer)
      "controls": 10000,                   # number of controls (integer)
      "male_proportion_cases": 0.5,        # proportion of cases that are male (0-1)
      "male_proportion_controls": 0.5,     # proportion of controls that are male (0-1)
      "assoc_test_software_and_version": "regenie v4.1",
      "assoc_test_model_and_settings": "mixed model logistic"
    }

Usage examples:
  # QA
  ./upload_gwas_presigned.py \
    --env qa \
    --cohort-name "My Cohort" \
    --dataset test_presigned \
    --phenotype acne \
    --ancestry EUR \
    --metadata /path/to/metadata.json \
    /path/to/gwas_file.tsv.gz

  # PRD (file can have any extension, will be validated as tab-delimited)
  ./upload_gwas_presigned.py \
    --env prd \
    --cohort-name ... --dataset ... --phenotype ... \
    --metadata metadata.json \
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
from tqdm import tqdm

# Required col_* fields that must be present in GWAS metadata (col_variant_id is optional)
REQUIRED_COL_FIELDS = [
    'col_chromosome', 'col_position', 'col_effect_allele', 'col_non_effect_allele',
    'col_beta', 'col_se', 'col_pvalue', 'col_effect_allele_freq', 'col_imputation_quality',
]


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

DEFAULT_API_BY_ENV = {
    "qa": "https://api.kpndataregistry.org:8000",
    "prd": "https://api.kpndataregistry.org",
}
DEFAULT_UI_BY_ENV = {
    "qa": "https://kpndataregistry.org:8000",
    "prd": "https://kpndataregistry.org",
}
DEFAULT_USER_SERVICE_URL = "https://users.kpndataregistry.org"


def login(user_service_url: str, username: str, password: str, group: str = "sgc") -> Optional[str]:
    """Authenticate and return JWT access token (or None)."""
    try:
        r = requests.post(
            f"{user_service_url}/api/auth/login/",
            json={"username": username, "password": password, "group": group},
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


def fetch_valid_sex_values(api_base: str, token: str) -> list:
    """Fetch the list of valid sex stratification values from the API."""
    try:
        resp = requests.get(
            f"{api_base}/api/sgc/gwas-sex-values",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        sys.stderr.write(f"Error fetching sex values: {e}\n")
        return []


def fetch_valid_ancestries(api_base: str, token: str) -> list:
    """Fetch the list of valid ancestry codes from the API."""
    try:
        resp = requests.get(
            f"{api_base}/api/sgc/gwas-ancestries",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        sys.stderr.write(f"Error fetching ancestries: {e}\n")
        return []


def fetch_valid_phenotypes(api_base: str, token: str) -> set:
    """Fetch the set of valid phenotype codes from the API."""
    try:
        resp = requests.get(
            f"{api_base}/api/sgc/phenotypes",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        resp.raise_for_status()
        return {p["phenotype_code"] for p in resp.json()}
    except requests.RequestException as e:
        sys.stderr.write(f"Error fetching phenotypes: {e}\n")
        return set()


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


# Required metadata fields and their expected types
REQUIRED_METADATA_FIELDS = {
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
    p = argparse.ArgumentParser(description="Upload GWAS via presigned URL (3-step)")
    p.add_argument("file", help="Path to GWAS file (e.g., .tsv.gz)")

    # Target API selection
    p.add_argument("--env", choices=["qa", "prd"], help="Environment: qa or prd")
    p.add_argument("--api-base-url", help="Override API base URL")
    p.add_argument("--user-service-url", default=DEFAULT_USER_SERVICE_URL, help="User service URL")

    # Auth group
    p.add_argument("--group", default="sgc", help="Auth group (default: sgc)")

    # Required GWAS metadata
    p.add_argument("--cohort-name", required=True, help="Name of the cohort")
    p.add_argument("--dataset", required=True)
    p.add_argument("--phenotype", required=True)

    # Dataset-level metadata
    p.add_argument("--ancestry", required=True,
                   help="Ancestry code (e.g. EUR, AFR, Combined)")
    p.add_argument("--metadata", required=True,
                   help="Path to JSON file with dataset-level metadata")

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

    token = login(user_service_url, username, password, args.group)
    if not token:
        sys.stderr.write("Authentication failed\n")
        sys.exit(1)

    # Fetch valid sex values and validate metadata
    valid_sex_values = fetch_valid_sex_values(api_base, token)
    if not valid_sex_values:
        sys.stderr.write("Could not fetch valid sex values from API\n")
        sys.exit(1)

    errors = validate_metadata(metadata, valid_sex_values)
    if errors:
        sys.stderr.write(f"Metadata validation errors in {args.metadata}:\n")
        for err in errors:
            sys.stderr.write(f"  - {err}\n")
        sys.exit(1)

    cases = metadata.pop("cases")
    controls = metadata.pop("controls")

    # Validate ancestry against allowed values
    print("Validating ancestry...")
    valid_ancestries = fetch_valid_ancestries(api_base, token)
    if not valid_ancestries:
        sys.stderr.write("Could not fetch valid ancestries from API\n")
        sys.exit(1)
    if args.ancestry not in valid_ancestries:
        sys.stderr.write(
            f"Invalid ancestry: '{args.ancestry}'\n"
            f"Valid ancestries: {valid_ancestries}\n"
            f"If the ancestry you need is not listed, please contact the SGC so it can be added.\n"
        )
        sys.exit(1)
    print(f"Ancestry '{args.ancestry}' is valid.")

    # Validate phenotype against allowed values
    print("Validating phenotype...")
    valid_phenotypes = fetch_valid_phenotypes(api_base, token)
    if not valid_phenotypes:
        sys.stderr.write("Could not fetch valid phenotypes from API\n")
        sys.exit(1)
    if args.phenotype not in valid_phenotypes:
        ui_base = DEFAULT_UI_BY_ENV.get(args.env, "https://kpndataregistry.org")
        sys.stderr.write(
            f"Invalid phenotype: '{args.phenotype}'\n"
            f"Valid phenotypes: {sorted(valid_phenotypes)}\n"
            f"See {ui_base}/sgc/phenotypes for the full list of allowed phenotypes.\n"
        )
        sys.exit(1)
    print(f"Phenotype '{args.phenotype}' is valid.")

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

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    # STEP 1: Request presigned URL
    print("[Step 1/3] Requesting presigned URL...")
    request_data = {
        "cohort_id": cohort_id,
        "dataset": args.dataset,
        "phenotype": args.phenotype,
        "ancestry": args.ancestry,
        "filename": filename,
        "column_mapping": column_mapping,
        "metadata": metadata,
        "cases": cases,
        "controls": controls,
    }

    try:
        resp = requests.post(
            f"{api_base}/api/sgc/gwas-upload-url",
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

    try:
        with open(args.file, "rb") as fh:
            progress_reader = ProgressFileReader(fh, file_size, f"Uploading {filename}")
            s3_resp = requests.put(
                presigned_url,
                data=progress_reader,
                headers={"Content-Length": str(file_size)},
                timeout=600,
            )
            s3_resp.raise_for_status()
            progress_reader.close()
        print(f"  File uploaded to S3 (HTTP {s3_resp.status_code})")
    except requests.RequestException as e:
        sys.stderr.write(f"\nS3 upload failed: {e}\n")
        if hasattr(e, "response") and e.response is not None:
            sys.stderr.write(f"Response: {e.response.text}\n")
        sys.exit(1)

    # STEP 3: Confirm upload with API
    print("[Step 3/3] Confirming upload with API...")
    confirm_data = {
        "cohort_id": cohort_id,
        "dataset": args.dataset,
        "phenotype": args.phenotype,
        "ancestry": args.ancestry,
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
            f"{api_base}/api/sgc/confirm-gwas-upload",
            headers=headers,
            json=confirm_data,
            timeout=30,
        )
        confirm_resp.raise_for_status()
        result = confirm_resp.json()
        print(json.dumps(result, indent=2))
    except requests.RequestException as e:
        sys.stderr.write(f"Confirm upload failed: {e}\n")
        if hasattr(e, "response") and e.response is not None:
            sys.stderr.write(f"Response: {e.response.text}\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
