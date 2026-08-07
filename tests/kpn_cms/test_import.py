import json
import pathlib

import boto3
import responses
from moto import mock_aws
from sqlalchemy import text

from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api import kpn_cms_query as q
from scripts.import_kpn_cms import run_import, strip_html, rows_for

engine = DataRegistryReadWriteDB().get_engine()
FIX = pathlib.Path(__file__).parent / 'fixtures'
SRC = 'https://kp4cd-fixture.test'
BIO = 'https://bioindex-fixture.test'


def _clean():
    with engine.connect() as con:
        for t in ('cms_content_item', 'cms_asset', 'cms_request_miss'):
            con.execute(text(f"TRUNCATE TABLE {t}"))
        con.commit()


def _fixture(name):
    return json.loads((FIX / name).read_text())


def _register(portals=('md',)):
    responses.get(f'{BIO}/api/portal/groups',
                  json={'data': [{'name': p} for p in portals]})
    news = _fixture('news2vueportal_md.json')
    responses.get(f'{SRC}/rest/views/news2vueportal', json=news)
    responses.get(f'{SRC}/reset/views/portal_front', json=_fixture('portal_front_md.json'))
    responses.get(f'{SRC}/rest/views/newfeatures', json=_fixture('newfeatures_md.json'))
    responses.get(f'{SRC}/rest/views/eglmethodsperportal', json=_fixture('eglmethodsperportal_md.json'))
    # Production reality (Task 2 evidence): newresources returns [] for every
    # portal; kpdatasets 404s under every parameterization. No fixtures exist.
    responses.get(f'{SRC}/rest/views/newresources', json=[])
    responses.get(f'{SRC}/rest/views/kpdatasets', status=404)
    responses.get(f'{SRC}/rest/views/a2f_community_kps', json=_fixture('a2f_community_kps.json'))
    responses.get(f'{SRC}/rest/views/help_book', json=_fixture('help_book.json'))
    responses.get(f'{SRC}/rest/views/datasetinfo', json=_fixture('datasetinfo_sample.json'))
    responses.get(f'{SRC}/rest/views/content_by_id', json=_fixture('content_by_id_sample.json'))
    responses.get(f'{SRC}/egldata/dataset', status=404)
    responses.get(f'{SRC}/egldata/config', status=404)


def test_strip_html():
    assert strip_html('<p>Hello <b>world</b> &amp; more</p>') == 'Hello world & more'


def test_rows_for_builds_stored_rows_with_order_and_search_text():
    rows = rows_for('help_book', [{'title': 'A', 'body': '<p>text one</p>'},
                                  {'title': 'B', 'body': 'two'}])
    assert rows[0]['sort_order'] == 0 and rows[1]['sort_order'] == 1
    assert rows[0]['view_name'] == 'help_book'
    assert 'text one' in rows[0]['search_text']
    assert json.loads(rows[0]['payload'])['title'] == 'A'


@mock_aws
@responses.activate
def test_run_import_fills_all_views_and_harvests_nids():
    _clean(); _register()
    s3 = boto3.client('s3', region_name='us-east-1'); s3.create_bucket(Bucket='kpn-cms-test')
    report = run_import(engine, s3, 'kpn-cms-test', SRC, BIO, skip_assets=True)
    assert report['views']['news2vueportal'] == len(_fixture('news2vueportal_md.json'))
    assert q.get_view_rows(engine, 'help_book')
    assert q.get_view_rows(engine, 'a2f_community_kps')
    # every nid appearing in imported rows got a content_by_id fetch
    news_nids = {r['nid'] for r in _fixture('news2vueportal_md.json') if r.get('nid')}
    for nid in news_nids:
        assert q.get_view_rows(engine, 'content_by_id', nid=nid)
    # datasetinfo keyed rows harvested via the portal-listing call.
    # field_dataset_id arrives in Drupal's full-entity field format
    # ([{"value": "..."}]) rather than a flat scalar -- unwrap it the same
    # way the import pipeline does before using it as a lookup key.
    raw_ds_id = _fixture('datasetinfo_sample.json')[0].get('field_dataset_id')
    ds_id = raw_ds_id[0]['value'] if isinstance(raw_ds_id, list) and raw_ds_id else raw_ds_id
    if ds_id:
        assert q.get_view_rows(engine, 'datasetinfo', item_key=ds_id)
    # dead/empty production views import as zero rows without failing the run
    assert report['views'].get('kpdatasets', 0) == 0
    assert report['views'].get('newresources', 0) == 0
    assert report['egldata_live'] is False


@mock_aws
@responses.activate
def test_run_import_idempotent():
    _clean(); _register()
    s3 = boto3.client('s3', region_name='us-east-1'); s3.create_bucket(Bucket='kpn-cms-test')
    run_import(engine, s3, 'kpn-cms-test', SRC, BIO, skip_assets=True)
    with engine.connect() as con:
        n1 = con.execute(text("SELECT COUNT(*) FROM cms_content_item")).fetchone()[0]
    _register()
    run_import(engine, s3, 'kpn-cms-test', SRC, BIO, skip_assets=True)
    with engine.connect() as con:
        n2 = con.execute(text("SELECT COUNT(*) FROM cms_content_item")).fetchone()[0]
    assert n1 == n2


@mock_aws
@responses.activate
def test_failed_view_leaves_prior_snapshot_intact():
    _clean(); _register()
    s3 = boto3.client('s3', region_name='us-east-1'); s3.create_bucket(Bucket='kpn-cms-test')
    run_import(engine, s3, 'kpn-cms-test', SRC, BIO, skip_assets=True)
    before = q.get_view_rows(engine, 'help_book')
    responses.reset()
    _register()
    responses.replace(responses.GET, f'{SRC}/rest/views/help_book', status=500)
    report = run_import(engine, s3, 'kpn-cms-test', SRC, BIO, skip_assets=True)
    assert q.get_view_rows(engine, 'help_book') == before      # old rows survive
    assert any('help_book' in s for s in report['skipped'])
