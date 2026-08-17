"""KPN dataset-info endpoints and mirrored-asset serving.

The registry serves dataset info (from kp_datasets, populated by
scripts/migrate_kp_datasets.py) and mirrored assets. The general kp4cd.org
content replacement (news, portal front, help book, EGL methods, etc.) was
retired -- other content types stay on their existing source.
"""
import boto3
import fastapi
from botocore.exceptions import ClientError
from fastapi.responses import RedirectResponse

from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api.kpn_cms_assets import ASSET_PREFIX, ASSETS_BUCKET
from dataregistry.api import kp_datasets_query as kq
from dataregistry.api.kp_datasets_envelope import node_envelope

router = fastapi.APIRouter()
engine = DataRegistryReadWriteDB().get_engine()
BUCKET = ASSETS_BUCKET

# Input-length clamps for the datasetinfo params (formerly also the column
# widths of the now-dropped general CMS snapshot table; the clamp behavior is
# kept so an oversized value degrades gracefully instead of raising under
# MySQL strict mode).
_ITEM_KEY_MAXLEN = 255
_PORTAL_MAXLEN = 64


@router.get('/kpn/rest/views/datasetinfo')
def kpn_datasetinfo(datasetid: str = None, portal: str = None):
    """kp_datasets is the system of record -- no snapshot, no proxy-on-miss."""
    if datasetid is not None:
        row = kq.get_by_dataset_id(engine, datasetid[:_ITEM_KEY_MAXLEN])
        return [node_envelope(row)] if row else []
    rows = kq.list_recent(engine, portal=portal[:_PORTAL_MAXLEN] if portal else None)
    return [node_envelope(r) for r in rows]


@router.get('/kpn/files/{path:path}')
def kpn_file(path: str):
    s3 = boto3.client('s3')
    key = ASSET_PREFIX + path
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
    except ClientError:
        raise fastapi.HTTPException(status_code=404, detail='asset not found')
    url = s3.generate_presigned_url('get_object', Params={'Bucket': BUCKET, 'Key': key}, ExpiresIn=3600)
    return RedirectResponse(url, status_code=307)
