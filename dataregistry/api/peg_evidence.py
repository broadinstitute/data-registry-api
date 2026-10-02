"""Summarise which evidence categories a PEG list carries, from its header row."""
import logging
from functools import lru_cache
from typing import Optional

import boto3
from pegasus.schema.constant import evidence_category

from dataregistry.api import s3

logger = logging.getLogger(__name__)

# Enough for any realistic header row; the rest of the file is never needed.
HEADER_READ_BYTES = 65536


def classify_evidence_columns(header: list) -> dict:
    """Group evidence columns by controlled category, as PEGASUS does: a column is
    evidence when its name, or the part before its first '_', is a category.
    Categories and their columns keep header order."""
    evidence = {}
    for column in header:
        category = column.split("_", 1)[0]
        if category in evidence_category:
            evidence.setdefault(category, []).append(column)
    return evidence


@lru_cache(maxsize=1024)
def _cached_list_evidence(file_id: str, file_path: str) -> dict:
    # Keyed by file id: a re-upload creates a new file row, so entries never go stale.
    key = file_path.replace(f"s3://{s3.BASE_BUCKET}/", "")
    client = boto3.client('s3', region_name=s3.S3_REGION)
    head = client.get_object(Bucket=s3.BASE_BUCKET, Key=key, Range=f"bytes=0-{HEADER_READ_BYTES - 1}")['Body'].read()
    if b"\n" not in head and len(head) == HEADER_READ_BYTES:
        head = client.get_object(Bucket=s3.BASE_BUCKET, Key=key)['Body'].read()
    header_line = head.split(b"\n", 1)[0].decode("utf-8-sig").rstrip("\r")
    return classify_evidence_columns(header_line.split("\t"))


def list_file_evidence(list_file: Optional[dict]) -> Optional[dict]:
    """Evidence summary for a study's peg_list file row: {} when there is no list
    file, None when it can't be read (failures aren't cached, so they retry)."""
    if not list_file:
        return {}
    try:
        return _cached_list_evidence(str(list_file['id']), list_file['file_path'])
    except Exception:
        logger.exception("Could not read PEG list header for file %s", list_file['id'])
        return None
