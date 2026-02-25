#!/usr/bin/env python3
"""
Upload GWAS file via presigned URL (3-step process):
  1. Request presigned URL from /api/sgc/gwas-upload-url
  2. Upload file directly to S3 using presigned URL
  3. Confirm upload via /api/sgc/confirm-gwas-upload

File must be tab-delimited (TSV format) - may be compressed (.gz) or uncompressed.
Filename extension does not matter; file content will be validated in step 3.
This method is better for large files as it uploads directly to S3.

Authentication:
  By default, the script will prompt for username and password. To skip the
  prompt, create a file called .credentials in the same directory as this script
  (i.e. scripts/.credentials) with the following format:
    Line 1: username
    Line 2: password

Usage examples:
  # QA
  ./scripts/upload_gwas_presigned.py \
    --env qa \
    --cohort-name "My Cohort" \
    --dataset test_presigned \
    --phenotype acne \
    --ancestry EUR \
    --column-mapping /tmp/col_map.json \
    --metadata /tmp/metadata.json \
    /path/to/gwas_file.tsv.gz

  # PRD (file can have any extension, will be validated as tab-delimited)
  ./scripts/upload_gwas_presigned.py \
    --env prd \
    --cohort-name ... --dataset ... --phenotype ... --column-mapping colmap.json file.txt
"""

import argparse
import getpass
import json
import os
import sys
from typing import Dict, Optional

import requests
from tqdm import tqdm


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


def load_json_file(path: str) -> Dict:
    with open(path, "r") as f:
        return json.load(f)


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

    # Optional metadata
    p.add_argument("--ancestry", default="EUR")
    p.add_argument("--column-mapping", required=True, help="Path to JSON for column mapping")
    p.add_argument("--cases", type=int, help="Number of cases (optional)")
    p.add_argument("--controls", type=int, help="Number of controls (optional)")
    p.add_argument("--metadata", help="Path to JSON for optional metadata")

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

    column_mapping = load_json_file(args.column_mapping)
    metadata = load_json_file(args.metadata) if args.metadata else None

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

    # Look up cohort ID by name
    cohort_id = lookup_cohort_id(api_base, token, args.cohort_name)
    if not cohort_id:
        sys.stderr.write(f"Cohort not found: {args.cohort_name}\n")
        sys.exit(1)
    print(f"Found cohort '{args.cohort_name}' -> {cohort_id}")

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
        "metadata": metadata or {},
    }
    if args.cases is not None:
        request_data["cases"] = args.cases
    if args.controls is not None:
        request_data["controls"] = args.controls

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
        "metadata": metadata or {},
    }
    if args.cases is not None:
        confirm_data["cases"] = args.cases
    if args.controls is not None:
        confirm_data["controls"] = args.controls

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
