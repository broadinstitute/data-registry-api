#!/usr/bin/env python3
"""
Build a ZIP archive of SGC cohort files and upload to S3.

Intended to be run on a schedule (e.g. nightly cron). The resulting archive is
served to reviewers via the GET /sgc/download-all-files API endpoint.

GWAS summary-stat files are intentionally excluded — they are large and
reviewers fetch them individually through the SGC UI.

The script computes a checksum of the current file list and skips rebuilding
if nothing has changed since the last run.

Usage:
    python build_sgc_archive.py                # build and upload (skips if unchanged)
    python build_sgc_archive.py --dry-run      # list files without building
    python build_sgc_archive.py --force         # rebuild even if unchanged
"""

import argparse
import hashlib
import json
import logging
import os
import re
import sys
import tempfile
import time
import zipfile

import boto3
from botocore.exceptions import ClientError

# Add project root to path so we can import dataregistry modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api import query, s3

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

ARCHIVE_S3_KEY = 'sgc/exports/sgc-all-files.zip'
CHECKSUM_S3_KEY = 'sgc/exports/sgc-all-files.checksum'


def sanitize_name(name: str) -> str:
    """Replace characters that are problematic in file paths."""
    return re.sub(r'[^\w\-. ]', '_', name)


def extract_s3_key(file_path: str) -> str:
    """Extract the S3 key from a path that may be a full s3:// URI or a bare key."""
    if file_path.startswith('s3://'):
        return file_path.split('/', 3)[3]
    return file_path


def compute_file_list_checksum(cohort_files):
    """Compute a SHA-256 checksum of the sorted file list.

    This captures additions, deletions, and path changes. If any file
    record is added, removed, or has its path changed, the checksum
    will differ and the archive will be rebuilt.
    """
    paths = sorted(f['file_path'] for f in cohort_files)
    return hashlib.sha256(json.dumps(paths).encode()).hexdigest()


def get_stored_checksum(s3_client):
    """Retrieve the previously stored checksum from S3, or None if not found."""
    try:
        response = s3_client.get_object(Bucket=s3.BASE_BUCKET, Key=CHECKSUM_S3_KEY)
        return response['Body'].read().decode().strip()
    except ClientError:
        return None


def store_checksum(s3_client, checksum):
    """Store the current checksum to S3."""
    s3_client.put_object(Bucket=s3.BASE_BUCKET, Key=CHECKSUM_S3_KEY, Body=checksum.encode())


def build_archive(engine, s3_client, dry_run=False, force=False):
    cohort_files = query.get_all_sgc_cohort_files_with_cohort_names(engine)
    total_files = len(cohort_files)
    logger.info(f"Found {total_files} cohort files")

    if total_files == 0:
        logger.warning("No files found. Skipping archive build.")
        return

    current_checksum = compute_file_list_checksum(cohort_files)
    logger.info(f"Current file list checksum: {current_checksum[:12]}...")

    if not dry_run and not force:
        stored_checksum = get_stored_checksum(s3_client)
        if stored_checksum == current_checksum:
            logger.info("File list unchanged since last build. Skipping. Use --force to rebuild.")
            return
        if stored_checksum:
            logger.info(f"Previous checksum: {stored_checksum[:12]}... — file list changed, rebuilding.")
        else:
            logger.info("No previous checksum found. Building archive.")

    if dry_run:
        logger.info("=== DRY RUN - listing files ===")
        for f in cohort_files:
            cohort_name = sanitize_name(f['cohort_name'])
            zip_path = f"sgc-all-files/{cohort_name}/cohort-files/{f['file_type']}_{f['file_name']}"
            logger.info(f"  {zip_path}  <-  {f['file_path']}")
        return

    start_time = time.time()
    total_bytes = 0
    files_added = 0

    with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as tmp:
        tmp_path = tmp.name

    try:
        with zipfile.ZipFile(tmp_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            for f in cohort_files:
                cohort_name = sanitize_name(f['cohort_name'])
                s3_key = extract_s3_key(f['file_path'])
                zip_entry = f"sgc-all-files/{cohort_name}/cohort-files/{f['file_type']}_{f['file_name']}"

                try:
                    response = s3_client.get_object(Bucket=s3.BASE_BUCKET, Key=s3_key)
                    data = response['Body'].read()
                    zf.writestr(zip_entry, data)
                    total_bytes += len(data)
                    files_added += 1
                    logger.info(f"  Added cohort file ({files_added}/{total_files}): {zip_entry}")
                except Exception as e:
                    logger.error(f"  Failed to fetch {s3_key}: {e}")

        if files_added == 0:
            raise RuntimeError(
                f"All {total_files} files failed to fetch from s3://{s3.BASE_BUCKET}. "
                "Refusing to upload an empty archive or update the checksum. "
                "Verify DATA_REGISTRY_BUCKET matches the environment the DB records point to."
            )

        archive_size = os.path.getsize(tmp_path)
        logger.info(f"Archive built: {archive_size / (1024*1024):.1f} MB compressed, "
                     f"{total_bytes / (1024*1024):.1f} MB uncompressed, "
                     f"{files_added}/{total_files} files")

        logger.info(f"Uploading archive to s3://{s3.BASE_BUCKET}/{ARCHIVE_S3_KEY}")
        s3_client.upload_file(tmp_path, s3.BASE_BUCKET, ARCHIVE_S3_KEY)

        store_checksum(s3_client, current_checksum)
        logger.info("Checksum stored.")

        elapsed = time.time() - start_time
        logger.info(f"Done in {elapsed:.1f}s")

    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


def main():
    parser = argparse.ArgumentParser(description='Build SGC files archive and upload to S3')
    parser.add_argument('--dry-run', action='store_true', help='List files without building the archive')
    parser.add_argument('--force', action='store_true', help='Rebuild even if the file list has not changed')
    args = parser.parse_args()

    engine = DataRegistryReadWriteDB().get_engine()
    s3_client = boto3.client('s3', region_name=s3.S3_REGION)

    build_archive(engine, s3_client, dry_run=args.dry_run, force=args.force)


if __name__ == '__main__':
    main()
