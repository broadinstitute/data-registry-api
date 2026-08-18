#!/usr/bin/env python3
"""
Download all SGC meta-analysis results into a local directory tree.

For every phenotype/ancestry/sex target with a SUCCEEDED meta-analysis, the
latest run's artifacts are downloaded:

    <out>/
      manifest.tsv                                   one row per downloaded run
      <PHENOTYPE>/
        <PHENOTYPE>_<SEX>_<ANCESTRY>_META.tsv.gz     meta-analysis summary stats
        <PHENOTYPE>_<SEX>_<ANCESTRY>_manhattan.png
        <PHENOTYPE>_<SEX>_<ANCESTRY>_qq.png
        <PHENOTYPE>_<SEX>_<ANCESTRY>_summary.json    cohort composition + QC counts

Re-running is safe: files that already exist (non-empty) are skipped, so an
interrupted download resumes where it left off. Use --force to re-download.

Authentication:
  Requires an account with the sgc-review-data permission. By default the
  script prompts for username and password. To skip the prompt, create a file
  called .credentials in the same directory as this script with:
    Line 1: username
    Line 2: password

Usage examples:
  # Everything from production into ./ma_results
  ./download_ma_results.py --env prd

  # One phenotype, custom output directory
  ./download_ma_results.py --env prd --phenotype ATOPIC_DERM --out /data/sgc_ma
"""

import argparse
import csv
import getpass
import os
import re
import sys
from typing import Optional

import requests

DEFAULT_API_BY_ENV = {
    "qa": "https://api.kpndataregistry.org:8000",
    "prd": "https://api.kpndataregistry.org",
}
DEFAULT_USER_SERVICE_URL = "https://users.kpndataregistry.org"
# Prod SGC accounts live in a separate user-service group.
DEFAULT_GROUP_BY_ENV = {"qa": "sgc", "prd": "sgc-prod"}

# (filename suffix, API route suffix, response kind)
# "presigned" endpoints return {"url": <S3 URL>}; "body" endpoints return the
# file content directly.
ARTIFACTS = [
    ("META.tsv.gz", "meta", "presigned"),
    ("manhattan.png", "manhattan", "presigned"),
    ("qq.png", "qq", "presigned"),
    ("summary.json", "summary", "body"),
]

MANIFEST_COLUMNS = ["phenotype", "sex", "ancestry", "run_id", "created_at",
                    "n_cohorts_used", "total_cases", "total_controls",
                    "meta_lambda_gc", "n_meta_variants", "n_genome_wide_sig"]


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
        sys.stderr.write(f"Login failed ({r.status_code}): {r.text}\n")
        return None
    except requests.RequestException as e:
        sys.stderr.write(f"Login error: {e}\n")
        return None


def load_credentials() -> Optional[tuple]:
    """Load credentials from .credentials next to this script, if present."""
    cred_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".credentials")
    if os.path.isfile(cred_path):
        with open(cred_path, "r") as f:
            lines = f.read().strip().splitlines()
        if len(lines) >= 2:
            return lines[0].strip(), lines[1].strip()
    return None


def _safe(token: str) -> str:
    """Make a metadata value safe for use in a file name."""
    return re.sub(r"[^A-Za-z0-9._-]", "-", str(token))


def base_name(run: dict) -> str:
    """PHENOTYPE_SEX_ANCESTRY -- the consortium's requested naming convention."""
    return f"{_safe(run['phenotype'])}_{_safe(run.get('sex', 'All'))}_{_safe(run['ancestry'])}"


def select_latest_runs(runs: list) -> list:
    """Latest SUCCEEDED run per (phenotype, ancestry, sex), ordered by target."""
    latest = {}
    for run in runs:
        if run.get("status") != "SUCCEEDED":
            continue
        key = (run["phenotype"], run["ancestry"], run.get("sex", "All"))
        current = latest.get(key)
        if current is None or (run.get("created_at") or "") > (current.get("created_at") or ""):
            latest[key] = run
    return [latest[k] for k in sorted(latest)]


def manifest_row(run: dict) -> dict:
    row = {"phenotype": run["phenotype"], "sex": run.get("sex", "All"),
           "ancestry": run["ancestry"], "run_id": run["id"],
           "created_at": run.get("created_at") or ""}
    for col in MANIFEST_COLUMNS[5:]:
        value = run.get(col)
        row[col] = value if value is not None else ""
    return row


def list_ma_results(api_base: str, token: str) -> list:
    r = requests.get(f"{api_base}/api/sgc/ma/results",
                     headers={"Authorization": f"Bearer {token}"}, timeout=60)
    r.raise_for_status()
    return r.json()


def _stream_to_file(resp: requests.Response, dest: str):
    """Write a (streaming) response to dest atomically via a .part temp file."""
    part = dest + ".part"
    with open(part, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1 << 20):
            f.write(chunk)
    os.replace(part, dest)


def download_artifact(api_base: str, token: str, run_id: str, route: str, kind: str, dest: str) -> str:
    """Download one artifact. Returns 'ok', 'missing', or raises."""
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(f"{api_base}/api/sgc/ma/runs/{run_id}/{route}", headers=headers, timeout=60)
    if r.status_code == 404:
        return "missing"
    r.raise_for_status()
    if kind == "presigned":
        url = r.json().get("url")
        if not url:
            return "missing"
        with requests.get(url, stream=True, timeout=300) as s3_resp:
            s3_resp.raise_for_status()
            _stream_to_file(s3_resp, dest)
    else:
        with open(dest + ".part", "wb") as f:
            f.write(r.content)
        os.replace(dest + ".part", dest)
    return "ok"


def main():
    ap = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[1],
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--env", choices=("qa", "prd"), default="prd")
    ap.add_argument("--api-base", default=None, help="Override the API base URL")
    ap.add_argument("--user-service-url", default=DEFAULT_USER_SERVICE_URL)
    ap.add_argument("--group", default=None,
                    help="User-service group (default: sgc for qa, sgc-prod for prd)")
    ap.add_argument("--out", default="ma_results", help="Output directory (default: ./ma_results)")
    ap.add_argument("--phenotype", default=None, help="Only download this phenotype")
    ap.add_argument("--force", action="store_true", help="Re-download files that already exist")
    args = ap.parse_args()

    api_base = args.api_base or DEFAULT_API_BY_ENV[args.env]

    creds = load_credentials()
    if creds:
        username, password = creds
    else:
        username = input("Username: ").strip()
        password = getpass.getpass("Password: ")
    token = login(args.user_service_url, username, password,
                  group=args.group or DEFAULT_GROUP_BY_ENV[args.env])
    if not token:
        sys.exit(1)

    runs = select_latest_runs(list_ma_results(api_base, token))
    if args.phenotype:
        runs = [r for r in runs if r["phenotype"] == args.phenotype]
    print(f"{len(runs)} meta-analysis target(s) to download from {api_base} into {args.out}/")

    os.makedirs(args.out, exist_ok=True)
    downloaded = skipped = missing = 0
    failures = []
    for i, run in enumerate(runs, 1):
        pheno_dir = os.path.join(args.out, _safe(run["phenotype"]))
        os.makedirs(pheno_dir, exist_ok=True)
        base = base_name(run)
        print(f"[{i}/{len(runs)}] {base}")
        for suffix, route, kind in ARTIFACTS:
            dest = os.path.join(pheno_dir, f"{base}_{suffix}")
            if not args.force and os.path.isfile(dest) and os.path.getsize(dest) > 0:
                skipped += 1
                continue
            try:
                status = download_artifact(api_base, token, run["id"], route, kind, dest)
            except (requests.RequestException, OSError) as e:
                failures.append((f"{base}_{suffix}", str(e)))
                print(f"    FAILED {suffix}: {e}", file=sys.stderr)
                continue
            if status == "missing":
                missing += 1
                print(f"    (no {suffix} for this run)")
            else:
                downloaded += 1

    manifest_path = os.path.join(args.out, "manifest.tsv")
    with open(manifest_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLUMNS, delimiter="\t")
        writer.writeheader()
        for run in runs:
            writer.writerow(manifest_row(run))

    print(f"\nDone: {downloaded} downloaded, {skipped} skipped (already present), "
          f"{missing} not available, {len(failures)} failed")
    print(f"Manifest: {manifest_path}")
    if failures:
        print("Failures (re-run to retry just these):", file=sys.stderr)
        for name, err in failures:
            print(f"  {name}: {err}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
