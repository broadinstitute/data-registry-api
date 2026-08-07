"""Snapshot the kp4cd.org Drupal CMS into the data-registry cms_* tables.

Re-runnable while kp4cd.org is alive; per-view transactional replace means a
failed run never leaves a view half-imported. Run --dry-run first to review
the URL set. Serves the /api/kpn/* endpoints in dataregistry/api/kpn_cms.py;
see the README's "KPN CMS content" section for the operational story.
"""
import argparse
import json
import re
import sys

import boto3
import requests

from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api import kpn_cms_query as q
from dataregistry.api.kpn_cms_assets import BROWSER_UA, find_asset_urls, mirror_assets, rewrite_asset_urls
from dataregistry.api.kpn_cms_ingest import rows_for, strip_html, _field_value

PORTAL_VIEWS = ['news2vueportal', 'newfeatures', 'eglmethodsperportal', 'newresources', 'kpdatasets']
GLOBAL_VIEWS = ['a2f_community_kps', 'help_book']
_NODE_RE = re.compile(r'\\?/node\\?/(\d+)')


def get_portals(bioindex_host):
    r = requests.get(f'{bioindex_host}/api/portal/groups',
                     headers={'User-Agent': BROWSER_UA}, timeout=60)
    r.raise_for_status()
    return [g['name'] for g in r.json()['data']]


def fetch_view(source_host, path):
    r = requests.get(f'{source_host}{path}', headers={'User-Agent': BROWSER_UA}, timeout=120)
    if r.status_code == 404:
        return []
    r.raise_for_status()
    try:
        body = r.json()
    except ValueError as e:
        # A 200 with a non-JSON body (e.g. an anti-bot interstitial page) is
        # a fetch failure, not a parse-time crash -- surface it the same way
        # a RequestException would so every caller's existing except clause
        # (skip the view, keep prior rows) covers it without special-casing.
        raise requests.RequestException(f'invalid JSON from {path}: {e}') from e
    return body if isinstance(body, list) else []


def _harvest_nids(engine):
    from sqlalchemy import text
    with engine.connect() as con:
        payloads = [r[0] for r in con.execute(text(
            "SELECT payload FROM cms_content_item WHERE view_name != 'content_by_id'")).fetchall()]
    nids = set()
    for p in payloads:
        nids |= set(_NODE_RE.findall(p))
        try:
            for item in json.loads(f'[{p}]'):
                nid_val = _field_value(item.get('nid'))
                if nid_val:
                    nids.add(str(nid_val))
        except (ValueError, AttributeError):
            pass
    return nids


def run_import(engine, s3_client, bucket, source_host, bioindex_host,
               dry_run=False, skip_assets=False):
    report = {'views': {}, 'assets': {'mirrored': 0, 'absent': []},
              'egldata_live': False, 'skipped': []}
    portals = get_portals(bioindex_host)

    def _import(view, path, scope_col, scope_val, portal=None, item_key=None):
        try:
            drupal_rows = fetch_view(source_host, path)
        except requests.RequestException as e:
            report['skipped'].append(f'{view}: {e}')
            return
        if dry_run:
            report['views'][view] = report['views'].get(view, 0) + len(drupal_rows)
            return
        stored = rows_for(view, drupal_rows, portal=portal,
                          nid=(scope_val if scope_col == 'nid' else None),
                          item_key=item_key)
        q.replace_view_rows(engine, view, scope_col, scope_val, stored)
        report['views'][view] = report['views'].get(view, 0) + len(stored)

    for portal in portals:
        for view in PORTAL_VIEWS:
            _import(view, f'/rest/views/{view}?portal={portal}', 'portal', portal, portal=portal)
        _import('portal_front', f'/reset/views/portal_front?portal={portal}', 'portal', portal, portal=portal)
    for view in GLOBAL_VIEWS:
        _import(view, f'/rest/views/{view}', None, None)

    if not dry_run:
        # datasetinfo keyed fetches. Id source: per-portal datasetinfo?portal=
        # listing calls (kpdatasets is dead in production — 404 under every
        # parameterization, incl. what the live frontend sends; the datasetinfo
        # view called with portal= returns the dataset node listing including
        # field_dataset_id. Evidence: tests/kpn_cms/fixtures/MANIFEST.md).
        ds_ids = set()
        for portal in portals:
            try:
                listing = fetch_view(source_host, f'/rest/views/datasetinfo?portal={portal}')
            except requests.RequestException as e:
                report['skipped'].append(f'datasetinfo listing {portal}: {e}')
                continue
            for row in listing:
                ds_id = _field_value(row.get('field_dataset_id'))
                if ds_id:
                    ds_ids.add(ds_id)
        for ds in sorted(ds_ids):
            _import('datasetinfo', f'/rest/views/datasetinfo?datasetid={ds}', 'item_key', ds, item_key=ds)
        for nid in sorted(_harvest_nids(engine)):
            _import('content_by_id', f'/rest/views/content_by_id?nid={nid}', 'nid', nid)

    # egldata probe — audit found these 404 (superseded by servedata); record reality.
    # Runs after every view import has succeeded, so a probe failure must never
    # discard the accumulated report -- it can only mark the probe unavailable.
    try:
        probe = fetch_view(source_host, '/egldata/config?dataset=&trait=')
        report['egldata_live'] = bool(probe)
    except requests.RequestException as e:
        report['egldata_live'] = False
        report['skipped'].append(f'egldata probe: {e}')

    if not dry_run and not skip_assets:
        from sqlalchemy import text as _text
        with engine.connect() as con:
            payloads = con.execute(_text("SELECT id, payload FROM cms_content_item")).fetchall()
        urls = set()
        for _, p in payloads:
            urls |= find_asset_urls(p)
        report['assets'] = mirror_assets(urls, engine, s3_client, bucket)
        with engine.begin() as con:
            for row_id, p in payloads:
                new = rewrite_asset_urls(p)
                if new != p:
                    con.execute(_text("UPDATE cms_content_item SET payload = :p WHERE id = :i"),
                                {'p': new, 'i': row_id})
    return report


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--source-host', default='https://kp4cd.org')
    ap.add_argument('--bioindex-host', default='https://bioindex.hugeamp.org')
    ap.add_argument('--bucket', default=None, help='defaults to DATA_REGISTRY_BUCKET / dig-data-registry')
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--skip-assets', action='store_true')
    args = ap.parse_args()
    from dataregistry.api.s3 import BASE_BUCKET
    engine = DataRegistryReadWriteDB().get_engine()
    s3_client = boto3.client('s3')
    report = run_import(engine, s3_client, args.bucket or BASE_BUCKET,
                        args.source_host, args.bioindex_host,
                        dry_run=args.dry_run, skip_assets=args.skip_assets)
    json.dump(report, sys.stdout, indent=2)
    print()


if __name__ == '__main__':
    main()
