#!/usr/bin/env python3
"""
Upload GWAS file via streaming endpoint (/api/sgc/upload-gwas-stream).
- File must be tab-delimited (TSV format) - may be compressed (.gz) or uncompressed
- Filename extension does not matter; file content is validated to ensure tab-delimited format
- Logs into user service to obtain a JWT access token.
- Sends metadata in headers; file is streamed as multipart/form-data.

Authentication:
  By default, the script will prompt for username and password. To skip the
  prompt, create a file called .credentials in the same directory as this script
  (i.e. scripts/.credentials) with the following format:
    Line 1: username
    Line 2: password

Usage examples:
  # QA
  ./scripts/upload_gwas_stream.py \
    --env qa \
    --cohort-name "My Cohort" \
    --dataset test_curl \
    --phenotype acne \
    --ancestry EUR \
    --column-mapping /tmp/col_map.json \
    --metadata /tmp/metadata.json \
    /path/to/gwas_file.tsv.gz

  # PRD with explicit URLs (file can have any extension, will be validated as tab-delimited)
  ./scripts/upload_gwas_stream.py \
    --api-base-url https://api.kpndataregistry.org \
    --user-service-url https://users.kpndataregistry.org \
    --cohort-name ... --dataset ... --phenotype ... --column-mapping colmap.json file.txt
"""

import argparse
import getpass
import json
import os
import sys
from typing import Dict, Optional

import requests
from requests_toolbelt import MultipartEncoder, MultipartEncoderMonitor
from tqdm import tqdm

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


def load_json_file(path: str) -> Dict:
    with open(path, "r") as f:
        return json.load(f)


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
    if args.cases is not None:
        headers["cases"] = str(args.cases)
    if args.controls is not None:
        headers["controls"] = str(args.controls)
    if metadata is not None:
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
