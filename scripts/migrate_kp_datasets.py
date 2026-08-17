"""One-off (re-runnable) migration of kp_dataset nodes from the kp4cd.org
Drupal DB into the registry's kp_datasets table.

Reads the Drupal RDS directly (SELECT only) via the 'kp4cd-220214' secret,
applies latest-wins dedup on field_dataset_id, upserts by drupal_nid, links
rows to existing registry datasets by exact name, deletes the superseded
cms_content_item datasetinfo snapshot, and mirrors/rewrites embedded kp4cd
asset URLs. Spec: docs/superpowers/specs/2026-08-14-kp-datasets-migration-design.md

    python -m scripts.migrate_kp_datasets --dry-run   # report only, no writes
    python -m scripts.migrate_kp_datasets
"""
import argparse
import json
import sys
from datetime import datetime, timezone

import boto3
import pymysql
from sqlalchemy import text

from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api import kp_datasets_query as kq
from dataregistry.api.kpn_cms_assets import (
    ASSETS_BUCKET, find_asset_urls, mirror_assets, rewrite_asset_urls)

DRUPAL_SECRET_ID = 'kp4cd-220214'

NODE_SQL = """
    SELECT n.nid, n.title, n.status, n.created, n.changed, u.mail AS author,
           d.field_dataset_id_value AS dataset_id,
           p.field_portals_value AS portals,
           b.body_value AS body
    FROM node_field_data n
    LEFT JOIN node__field_dataset_id d ON d.entity_id = n.nid AND d.deleted = 0
    LEFT JOIN node__field_portals p ON p.entity_id = n.nid AND p.deleted = 0
    LEFT JOIN node__body b ON b.entity_id = n.nid AND b.deleted = 0
    LEFT JOIN users_field_data u ON u.uid = n.uid
    WHERE n.type = 'kp_dataset'
"""


def fetch_drupal_nodes():
    sec = json.loads(boto3.client('secretsmanager', region_name='us-east-1')
                     .get_secret_value(SecretId=DRUPAL_SECRET_ID)['SecretString'])
    conn = pymysql.connect(host=sec['host'], port=int(sec['port']),
                           user=sec['username'], password=sec['password'],
                           database=sec['dbname'], connect_timeout=15,
                           read_timeout=300,
                           cursorclass=pymysql.cursors.DictCursor)
    try:
        with conn.cursor() as cur:
            cur.execute(NODE_SQL)
            return cur.fetchall()
    finally:
        conn.close()


def dedup_nodes(nodes):
    """Latest-changed node keeps a duplicated dataset_id; earlier twins are
    kept but demoted to dataset_id NULL with an explanatory note."""
    winners = {}
    for n in nodes:
        ds = n['dataset_id']
        if ds and (ds not in winners or n['changed'] > winners[ds]['changed']):
            winners[ds] = n
    out = []
    for n in nodes:
        n = dict(n)
        ds = n['dataset_id']
        if ds and winners[ds]['nid'] != n['nid']:
            n['migration_note'] = (f'duplicate dataset_id {ds}; '
                                   f'superseded by nid {winners[ds]["nid"]}')
            n['dataset_id'] = None
        else:
            n['migration_note'] = None
        out.append(n)
    return out


def node_to_row(node):
    def _dt(unix):
        return datetime.fromtimestamp(unix, tz=timezone.utc).replace(tzinfo=None)
    return {'dataset_id': node['dataset_id'],
            'title': node['title'],
            'body': node['body'] or '',
            'portals': node['portals'] or '',
            'published': int(node['status']),
            'drupal_nid': node['nid'],
            'drupal_author': node['author'],
            'migration_note': node['migration_note'],
            'created_at': _dt(node['created']),
            'updated_at': _dt(node['changed'])}


def run_migration(engine, s3_client, bucket, dry_run=False, skip_assets=False):
    rows = [node_to_row(n) for n in dedup_nodes(fetch_drupal_nodes())]
    report = {'nodes': len(rows),
              'published': sum(r['published'] for r in rows),
              'no_dataset_id': sum(1 for r in rows if r['dataset_id'] is None),
              'demoted_duplicates': sum(1 for r in rows if r['migration_note']),
              'registry_links': 0, 'cms_rows_deleted': 0,
              'assets': {'mirrored': 0, 'absent': [], 'errors': []}}
    if dry_run:
        return report
    for r in rows:
        kq.upsert_kp_dataset(engine, r)
    report['registry_links'] = kq.backfill_registry_links(engine)
    report['cms_rows_deleted'] = kq.delete_cms_datasetinfo_rows(engine)
    if not skip_assets:
        with engine.connect() as con:
            bodies = con.execute(text("SELECT id, body FROM kp_datasets")).fetchall()
        urls = set()
        for _, body in bodies:
            urls |= find_asset_urls(body)
        report['assets'] = mirror_assets(urls, engine, s3_client, bucket)
        with engine.begin() as con:
            for row_id, body in bodies:
                new = rewrite_asset_urls(body)
                if new != body:
                    con.execute(text("UPDATE kp_datasets SET body = :b WHERE id = :i"),
                                {'b': new, 'i': row_id})
    return report


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--bucket', default=None,
                    help='defaults to KPN_CMS_ASSETS_BUCKET / dig-kpn-cms-assets')
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--skip-assets', action='store_true')
    args = ap.parse_args()
    engine = DataRegistryReadWriteDB().get_engine()
    report = run_migration(engine, boto3.client('s3'), args.bucket or ASSETS_BUCKET,
                           dry_run=args.dry_run, skip_assets=args.skip_assets)
    json.dump(report, sys.stdout, indent=2)
    print()


if __name__ == '__main__':
    main()
