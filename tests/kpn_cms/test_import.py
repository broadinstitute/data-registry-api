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
    # datasetinfo is no longer imported -- kp_datasets is its system of record
    assert q.get_view_rows(engine, 'datasetinfo') == []
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


@mock_aws
@responses.activate
def test_view_with_non_json_body_lands_in_skipped_leaves_snapshot_others_import():
    # A 200 response with a non-JSON body (e.g. an anti-bot interstitial page --
    # exactly the failure class the BROWSER_UA workaround exists for) must be
    # treated as a fetch failure like any other: the view is skipped, its prior
    # rows survive untouched, and every other view still imports normally.
    _clean(); _register()
    s3 = boto3.client('s3', region_name='us-east-1'); s3.create_bucket(Bucket='kpn-cms-test')
    run_import(engine, s3, 'kpn-cms-test', SRC, BIO, skip_assets=True)
    before = q.get_view_rows(engine, 'help_book')
    responses.reset()
    _register()
    responses.replace(responses.GET, f'{SRC}/rest/views/help_book', body='<html>bot check</html>')
    report = run_import(engine, s3, 'kpn-cms-test', SRC, BIO, skip_assets=True)
    assert q.get_view_rows(engine, 'help_book') == before      # old rows survive
    assert any('help_book' in s for s in report['skipped'])
    # every other view still imported despite help_book's failure
    assert report['views']['news2vueportal'] == len(_fixture('news2vueportal_md.json'))
    assert q.get_view_rows(engine, 'a2f_community_kps')


@mock_aws
@responses.activate
def test_run_import_heals_proxy_persisted_asset_urls():
    # Rows written by the API's proxy-on-miss path (dataregistry/api/kpn_cms.py
    # _proxy_and_persist) persist UNREWRITTEN kp4cd.org asset URLs on purpose
    # (mirroring to S3 must not happen synchronously inside a public request).
    # The import's asset phase must heal them on its next run: it queries ALL
    # cms_content_item rows (not just ones this run fetched), so a
    # proxy-persisted row -- for a view this run never even touches -- still
    # gets its asset mirrored to S3 and its payload rewritten.
    _clean(); _register()
    s3 = boto3.client('s3', region_name='us-east-1'); s3.create_bucket(Bucket='kpn-cms-test')
    asset_url = 'https://kp4cd.org/sites/default/files/images/proxied.png'
    responses.get(asset_url, body=b'PNGDATA', content_type='image/png')
    proxied_row = {'view_name': 'eglmethod', 'portal': None, 'nid': None,
                   'item_key': 'proxied-item',
                   'payload': json.dumps({'title': 'proxied',
                                          'body': f'<img src="{asset_url}"/>'}),
                   'search_text': None, 'sort_order': 0}
    q.replace_view_rows(engine, 'eglmethod', 'item_key', 'proxied-item', [proxied_row])
    run_import(engine, s3, 'kpn-cms-test', SRC, BIO)   # assets enabled (default skip_assets=False)
    healed = q.get_view_rows(engine, 'eglmethod', item_key='proxied-item')
    assert '/api/kpn/files/images/proxied.png' in healed[0]['body']
    assert asset_url not in healed[0]['body']
    objs = s3.list_objects_v2(Bucket='kpn-cms-test', Prefix='kpn-cms-assets/images/proxied.png')
    assert objs.get('KeyCount', 0) == 1
    body = s3.get_object(Bucket='kpn-cms-test', Key='kpn-cms-assets/images/proxied.png')['Body'].read()
    assert body == b'PNGDATA'


@mock_aws
@responses.activate
def test_egldata_probe_failure_does_not_abort_run():
    # The probe runs after every view import has already succeeded; a failure
    # there must only mark the probe unavailable, never discard the report.
    _clean(); _register()
    s3 = boto3.client('s3', region_name='us-east-1'); s3.create_bucket(Bucket='kpn-cms-test')
    responses.replace(responses.GET, f'{SRC}/egldata/config', body='<html>not json</html>')
    report = run_import(engine, s3, 'kpn-cms-test', SRC, BIO, skip_assets=True)
    assert report['egldata_live'] is False
    assert any('egldata' in s for s in report['skipped'])
    # the rest of the run completed and is present in the returned report
    assert report['views']['news2vueportal'] == len(_fixture('news2vueportal_md.json'))
    assert q.get_view_rows(engine, 'help_book')
