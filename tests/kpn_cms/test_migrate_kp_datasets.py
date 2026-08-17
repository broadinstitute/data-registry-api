from datetime import datetime

import boto3
import responses
from moto import mock_aws
from sqlalchemy import text

from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api import kp_datasets_query as kq
from dataregistry.api import kpn_cms_query as q
from scripts import migrate_kp_datasets as mig

engine = DataRegistryReadWriteDB().get_engine()


def _clean():
    with engine.connect() as con:
        con.execute(text("TRUNCATE TABLE kp_datasets"))
        con.execute(text("TRUNCATE TABLE cms_content_item"))
        con.execute(text("TRUNCATE TABLE cms_asset"))
        con.commit()


def _node(nid, dataset_id, changed=1700000000, status=1, body='<p>b</p>',
          portals='md, a2f'):
    return {'nid': nid, 'title': f't{nid}', 'status': status,
            'created': 1600000000, 'changed': changed,
            'author': 'mariacos@broadinstitute.org',
            'dataset_id': dataset_id, 'portals': portals, 'body': body}


def test_dedup_latest_changed_wins():
    nodes = [_node(1, 'DUP_X', changed=100), _node(2, 'DUP_X', changed=200),
             _node(3, 'SOLO_Y', changed=50), _node(4, None, changed=60)]
    out = {n['nid']: n for n in mig.dedup_nodes(nodes)}
    assert len(out) == 4
    assert out[2]['dataset_id'] == 'DUP_X' and out[2]['migration_note'] is None
    assert out[1]['dataset_id'] is None
    assert out[1]['migration_note'] == 'duplicate dataset_id DUP_X; superseded by nid 2'
    assert out[3]['dataset_id'] == 'SOLO_Y' and out[3]['migration_note'] is None
    assert out[4]['dataset_id'] is None and out[4]['migration_note'] is None


def test_dedup_ties_break_on_higher_nid():
    """On equal changed timestamp, higher nid wins (deterministic tiebreak)."""
    # Test both orderings to verify deterministic result regardless of input order
    nodes_asc = [_node(1, 'TIE_A', changed=500), _node(3, 'TIE_A', changed=500),
                 _node(2, 'TIE_A', changed=500)]
    nodes_desc = [_node(3, 'TIE_A', changed=500), _node(2, 'TIE_A', changed=500),
                  _node(1, 'TIE_A', changed=500)]

    # Both orderings should produce the same result: nid 3 wins
    for nodes in [nodes_asc, nodes_desc]:
        out = {n['nid']: n for n in mig.dedup_nodes(nodes)}
        assert len(out) == 3
        # nid 3 (highest) keeps the dataset_id with no note
        assert out[3]['dataset_id'] == 'TIE_A' and out[3]['migration_note'] is None
        # nid 2 (middle) is demoted
        assert out[2]['dataset_id'] is None
        assert out[2]['migration_note'] == 'duplicate dataset_id TIE_A; superseded by nid 3'
        # nid 1 (lowest) is also demoted
        assert out[1]['dataset_id'] is None
        assert out[1]['migration_note'] == 'duplicate dataset_id TIE_A; superseded by nid 3'


def test_node_to_row_converts_types_and_defaults():
    node = dict(_node(9, 'A_B'), migration_note=None, body=None, portals=None)
    row = mig.node_to_row(node)
    assert row['body'] == '' and row['portals'] == ''
    assert row['published'] == 1 and row['drupal_nid'] == 9
    assert row['created_at'] == datetime(2020, 9, 13, 12, 26, 40)
    assert row['updated_at'] == datetime(2023, 11, 14, 22, 13, 20)


@mock_aws
def test_run_migration_upserts_links_and_cleans_cms(monkeypatch):
    _clean()
    monkeypatch.setattr(mig, 'fetch_drupal_nodes',
                        lambda: [_node(1, 'MIGTEST_A'), _node(2, 'MIGTEST_B', status=0)])
    q.replace_view_rows(engine, 'datasetinfo', 'item_key', 'old',
                        [{'view_name': 'datasetinfo', 'portal': None, 'nid': None,
                          'item_key': 'old', 'payload': '{}', 'search_text': None,
                          'sort_order': 0}])
    s3 = boto3.client('s3', region_name='us-east-1'); s3.create_bucket(Bucket='kpn-cms-test')
    report = mig.run_migration(engine, s3, 'kpn-cms-test', skip_assets=True)
    assert report['nodes'] == 2 and report['published'] == 1
    assert report['cms_rows_deleted'] == 1
    assert kq.get_by_dataset_id(engine, 'MIGTEST_A')['title'] == 't1'
    # idempotent: run again, same row count
    mig.run_migration(engine, s3, 'kpn-cms-test', skip_assets=True)
    with engine.connect() as con:
        assert con.execute(text("SELECT COUNT(*) FROM kp_datasets")).scalar() == 2


@mock_aws
def test_run_migration_dry_run_writes_nothing(monkeypatch):
    _clean()
    monkeypatch.setattr(mig, 'fetch_drupal_nodes', lambda: [_node(1, 'DRYTEST_A')])
    s3 = boto3.client('s3', region_name='us-east-1'); s3.create_bucket(Bucket='kpn-cms-test')
    report = mig.run_migration(engine, s3, 'kpn-cms-test', dry_run=True)
    assert report['nodes'] == 1
    with engine.connect() as con:
        assert con.execute(text("SELECT COUNT(*) FROM kp_datasets")).scalar() == 0


@mock_aws
@responses.activate
def test_run_migration_mirrors_and_rewrites_body_assets(monkeypatch):
    _clean()
    body = '<img src="https://kp4cd.org/sites/default/files/pic.png">'
    monkeypatch.setattr(mig, 'fetch_drupal_nodes',
                        lambda: [_node(1, 'ASSETTEST_A', body=body)])
    responses.get('https://kp4cd.org/sites/default/files/pic.png', body=b'png',
                  content_type='image/png')
    s3 = boto3.client('s3', region_name='us-east-1'); s3.create_bucket(Bucket='kpn-cms-test')
    report = mig.run_migration(engine, s3, 'kpn-cms-test')
    assert report['assets']['mirrored'] == 1
    stored = kq.get_by_dataset_id(engine, 'ASSETTEST_A')['body']
    assert '/api/kpn/files/pic.png' in stored and 'kp4cd.org' not in stored
    s3.head_object(Bucket='kpn-cms-test', Key='kpn-cms-assets/pic.png')
