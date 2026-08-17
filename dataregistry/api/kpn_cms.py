"""Drupal-compatible KPN CMS endpoints (kp4cd.org replacement).

Response contract: same array-of-flat-objects JSON the Drupal views returned,
same field names, HTML-in-strings untouched; only asset URLs are rewritten to
/api/kpn/files/ (import-time rewrite — proxy-persisted rows keep original URLs
until the next import run mirrors their assets). Content is populated by
scripts/import_kpn_cms.py; unknown requests fall back to a feature-flagged
proxy against the live CMS while it exists (KPN_CMS_PROXY_ON_MISS).
"""
import os
import re

import boto3
import fastapi
import requests
from botocore.exceptions import ClientError
from fastapi import Request
from fastapi.responses import RedirectResponse

from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api import kpn_cms_query as q
from dataregistry.api.kpn_cms_assets import ASSET_PREFIX, ASSETS_BUCKET, BROWSER_UA
from dataregistry.api.kpn_cms_ingest import rows_for
from dataregistry.api import kp_datasets_query as kq
from dataregistry.api.kp_datasets_envelope import node_envelope

router = fastapi.APIRouter()
engine = DataRegistryReadWriteDB().get_engine()
BUCKET = ASSETS_BUCKET

# Real Drupal view/egldata-kind machine names are [a-z0-9_]. A path segment
# containing anything else (e.g. a decoded '?' or '#' smuggled in via
# percent-encoding) must never reach the outbound proxy request -- it would
# let an anonymous caller inject/override query params or a fragment on the
# request we make to kp4cd.org. Reject up front; treat exactly like a
# proxy-disabled miss (record + return []), never proxy.
_NAME_RE = re.compile(r'^[A-Za-z0-9_]+$')

# Column widths in cms_content_item / cms_request_miss (see migration). User-
# controlled values are clamped to these before any DB call so an
# oversized-but-valid-charset value degrades gracefully (miss recorded, [])
# instead of raising a DataError under MySQL strict mode.
_VIEW_NAME_MAXLEN = 128
_PORTAL_MAXLEN = 64
_NID_MAXLEN = 32
_ITEM_KEY_MAXLEN = 255

# view -> the query param the portal filters it by (audit inventory).
# Views absent here (research_data, arbitrary ${query.page} names) key on the
# whole canonicalized query string.
FILTER_PARAM = {
    'news2vueportal': 'portal', 'kpdatasets': 'portal', 'newfeatures': 'portal',
    'newresources': 'portal', 'eglmethodsperportal': 'portal', 'portal_front': 'portal',
    'content_by_id': 'nid', 'eglmethod': 'from',
    'static_content': 'field_page', 'paperheadermenu': 'paper',
}


def _proxy_enabled():
    return os.getenv('KPN_CMS_PROXY_ON_MISS', 'true').lower() == 'true'


def _source_host():
    return os.getenv('KPN_CMS_SOURCE_HOST', 'https://kp4cd.org')


def _canonical_item_key(params):
    return q.canonical_key(params)[:_ITEM_KEY_MAXLEN]


def _lookup(view_name, params):
    filter_param = FILTER_PARAM.get(view_name)
    if filter_param is None and params:
        key = _canonical_item_key(params)
        return q.get_view_rows(engine, view_name, item_key=key), 'item_key', key
    if filter_param is None:
        return q.get_view_rows(engine, view_name), None, None
    value = params.get(filter_param)
    if filter_param == 'portal':
        value = value[:_PORTAL_MAXLEN] if value is not None else value
        return q.get_view_rows(engine, view_name, portal=value), 'portal', value
    if filter_param == 'nid':
        value = value[:_NID_MAXLEN] if value is not None else value
        return q.get_view_rows(engine, view_name, nid=value), 'nid', value
    value = value[:_ITEM_KEY_MAXLEN] if value is not None else value
    return q.get_view_rows(engine, view_name, item_key=value), 'item_key', value


def _proxy_and_persist(view_name, path, params, scope_col, scope_val):
    query_string = q.canonical_key(params)
    if not _proxy_enabled():
        q.record_miss(engine, view_name, query_string, False, None)
        return []
    try:
        resp = requests.get(f'{_source_host()}{path}', params=params,
                            headers={'User-Agent': BROWSER_UA}, timeout=30)
    except requests.RequestException:
        q.record_miss(engine, view_name, query_string, True, None)
        return []
    q.record_miss(engine, view_name, query_string, True, resp.status_code)
    if resp.status_code != 200:
        return []
    try:
        body = resp.json()
    except ValueError:
        return []
    if not isinstance(body, list):
        return []
    rows = rows_for(view_name, body,
                    portal=scope_val if scope_col == 'portal' else None,
                    nid=scope_val if scope_col == 'nid' else None,
                    item_key=scope_val if scope_col == 'item_key' else None)
    # Persist the ORIGINAL (unrewritten) Drupal payload here, not the
    # asset-rewritten form. Proxy-on-miss only runs while kp4cd.org is still
    # alive, so the raw kp4cd.org asset URLs still resolve directly in
    # browsers today. The import pipeline's asset phase (scripts/import_kpn_cms.py)
    # scans ALL cms_content_item rows for bare-host kp4cd.org URLs and
    # mirrors+rewrites them on its next run, so proxy-persisted rows get
    # healed the same way as normally-imported ones -- and mirroring to S3
    # must never happen synchronously inside a public request.
    q.replace_view_rows(engine, view_name, scope_col, scope_val, rows)
    kwargs = {}
    if scope_col:
        kwargs[scope_col] = scope_val   # scope_col names match get_view_rows kwargs
    return q.get_view_rows(engine, view_name, **kwargs)


@router.get('/kpn/rest/views/help_book_search')
def kpn_help_book_search(body: str = ''):
    return q.search_help_book(engine, body)


def _datasetinfo(params):
    """kp_datasets is the system of record for datasetinfo -- no snapshot
    lookup, no proxy-on-miss, no miss recording."""
    ds = params.get('datasetid')
    if ds is not None:
        row = kq.get_by_dataset_id(engine, ds[:_ITEM_KEY_MAXLEN])
        return [node_envelope(row)] if row else []
    portal = params.get('portal')
    rows = kq.list_recent(engine, portal=portal[:_PORTAL_MAXLEN] if portal else None)
    return [node_envelope(r) for r in rows]


@router.get('/kpn/rest/views/{view_name}')
def kpn_view(view_name: str, request: Request):
    view_name = view_name[:_VIEW_NAME_MAXLEN]
    params = dict(request.query_params)
    if not _NAME_RE.match(view_name):
        q.record_miss(engine, view_name, q.canonical_key(params), False, None)
        return []
    if view_name == 'datasetinfo':
        return _datasetinfo(params)
    rows, scope_col, scope_val = _lookup(view_name, params)
    if rows:
        return rows
    return _proxy_and_persist(view_name, f'/rest/views/{view_name}', params, scope_col, scope_val)


@router.get('/kpn/reset/views/portal_front')
def kpn_portal_front(request: Request):
    params = dict(request.query_params)
    rows, scope_col, scope_val = _lookup('portal_front', params)
    if rows:
        return rows
    return _proxy_and_persist('portal_front', '/reset/views/portal_front', params, scope_col, scope_val)


@router.get('/kpn/egldata/{kind}')
def kpn_egldata(kind: str, request: Request):
    params = dict(request.query_params)
    view_name = f'egldata_{kind}'[:_VIEW_NAME_MAXLEN]
    if not _NAME_RE.match(kind):
        q.record_miss(engine, view_name, q.canonical_key(params), False, None)
        return []
    key = _canonical_item_key(params)
    rows = q.get_view_rows(engine, view_name, item_key=key)
    if rows:
        return rows
    return _proxy_and_persist(view_name, f'/egldata/{kind}', params, 'item_key', key)


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


