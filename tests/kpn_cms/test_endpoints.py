import json
import pathlib

import boto3
import pytest
import responses
from fastapi.testclient import TestClient
from moto import mock_aws
from sqlalchemy import text

from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api import kpn_cms
from dataregistry.api import kpn_cms_query as q
from dataregistry.server import app
from scripts.import_kpn_cms import rows_for

client = TestClient(app)
engine = DataRegistryReadWriteDB().get_engine()
FIX = pathlib.Path(__file__).parent / 'fixtures'


def _clean():
    with engine.connect() as con:
        for t in ('cms_content_item', 'cms_asset', 'cms_request_miss'):
            con.execute(text(f"TRUNCATE TABLE {t}"))
        con.commit()


def _load(view, fixture, scope_col=None, scope_val=None, **kw):
    rows = rows_for(view, json.loads((FIX / fixture).read_text()), **kw)
    q.replace_view_rows(engine, view, scope_col, scope_val, rows)
    return [json.loads(r['payload']) for r in rows]


# --- fidelity: one test per captured live view ---

def test_news_fidelity(monkeypatch):
    monkeypatch.setenv('KPN_CMS_PROXY_ON_MISS', 'false')
    _clean()
    expect = _load('news2vueportal', 'news2vueportal_md.json', 'portal', 'md', portal='md')
    got = client.get('/api/kpn/rest/views/news2vueportal?portal=md')
    assert got.status_code == 200
    assert got.json() == expect          # field-for-field


def test_portal_front_reset_spelling(monkeypatch):
    monkeypatch.setenv('KPN_CMS_PROXY_ON_MISS', 'false')
    _clean()
    expect = _load('portal_front', 'portal_front_md.json', 'portal', 'md', portal='md')
    assert client.get('/api/kpn/reset/views/portal_front?portal=md').json() == expect


def test_unparameterized_views(monkeypatch):
    monkeypatch.setenv('KPN_CMS_PROXY_ON_MISS', 'false')
    _clean()
    expect = _load('a2f_community_kps', 'a2f_community_kps.json')
    assert client.get('/api/kpn/rest/views/a2f_community_kps').json() == expect


def test_content_by_id(monkeypatch):
    monkeypatch.setenv('KPN_CMS_PROXY_ON_MISS', 'false')
    _clean()
    # '1770' is the nid the fixture was captured with (MANIFEST.md); the real
    # Drupal response body does not echo the nid, so it must be supplied as scope.
    rows = _load('content_by_id', 'content_by_id_sample.json', 'nid', '1770', nid='1770')
    assert client.get('/api/kpn/rest/views/content_by_id?nid=1770').json() == rows


def test_help_book_search(monkeypatch):
    monkeypatch.setenv('KPN_CMS_PROXY_ON_MISS', 'false')
    _clean()
    rows = _load('help_book', 'help_book.json')
    # pick the first (row, word) with a distinctive alphabetic word — not every
    # row's title has one (row 0's title is just "Data" in the real fixture)
    row, word = next((r, w) for r in rows
                     for w in r.get('title', '').split() if len(w) > 5 and w.isalpha())
    got = client.get(f'/api/kpn/rest/views/help_book_search?body={word}').json()
    assert row in got


@pytest.mark.parametrize('view,fixture', [
    ('newfeatures', 'newfeatures_md.json'),
    ('eglmethodsperportal', 'eglmethodsperportal_md.json'),
])
def test_portal_view_fidelity(monkeypatch, view, fixture):
    monkeypatch.setenv('KPN_CMS_PROXY_ON_MISS', 'false')
    _clean()
    expect = _load(view, fixture, 'portal', 'md', portal='md')
    assert client.get(f'/api/kpn/rest/views/{view}?portal=md').json() == expect


# Keyed-view fidelity. The keys are the exact values the Task 2 fixtures were
# captured with (see tests/kpn_cms/fixtures/MANIFEST.md); fixtures are committed,
# so hardcoding them is stable. kpdatasets/newresources have no fixtures --
# dead/empty in production (404 / [] everywhere, same MANIFEST evidence).
@pytest.mark.parametrize('view,fixture,param,key', [
    ('static_content', 'static_content_apis.json', 'field_page', 'apis'),
    ('paperheadermenu', 'paperheadermenu_sample.json', 'paper', 'apol1_portal'),
    ('eglmethod', 'eglmethod_sample.json', 'from', 'cardiogram'),
])
def test_keyed_view_fidelity(monkeypatch, view, fixture, param, key):
    monkeypatch.setenv('KPN_CMS_PROXY_ON_MISS', 'false')
    _clean()
    expect = _load(view, fixture, 'item_key', key, item_key=key)
    assert client.get(f'/api/kpn/rest/views/{view}?{param}={key}').json() == expect


# --- miss paths ---

def test_miss_with_proxy_off_returns_empty_and_logs(monkeypatch):
    monkeypatch.setenv('KPN_CMS_PROXY_ON_MISS', 'false')
    _clean()
    got = client.get('/api/kpn/rest/views/someview?portal=zz')
    assert got.status_code == 200 and got.json() == []
    misses = q.get_misses(engine)
    assert misses[0]['view_name'] == 'someview' and not misses[0]['proxied']


@responses.activate
def test_miss_with_proxy_on_forwards_and_persists(monkeypatch):
    monkeypatch.setenv('KPN_CMS_PROXY_ON_MISS', 'true')
    monkeypatch.setenv('KPN_CMS_SOURCE_HOST', 'https://kp4cd-fixture.test')
    _clean()
    payload = [{'title': 'proxied row'}]
    responses.get('https://kp4cd-fixture.test/rest/views/someview', json=payload)
    assert client.get('/api/kpn/rest/views/someview?portal=zz').json() == payload
    # second call is served from the DB, not the proxy (responses would ConnectionError on a 2nd unregistered call)
    responses.reset()
    assert client.get('/api/kpn/rest/views/someview?portal=zz').json() == payload
    assert q.get_misses(engine)[0]['proxied']


@responses.activate
def test_proxy_persist_keeps_original_asset_urls_for_import_to_heal(monkeypatch):
    # Proxy-on-miss must persist the UNREWRITTEN Drupal payload: rewriting to
    # /api/kpn/files/ here without ever mirroring to S3 would permanently break
    # the asset (mirroring must not happen synchronously inside a public
    # request). The offline import's asset phase heals these rows on its next
    # run by scanning ALL stored rows for bare-host kp4cd.org URLs.
    monkeypatch.setenv('KPN_CMS_PROXY_ON_MISS', 'true')
    monkeypatch.setenv('KPN_CMS_SOURCE_HOST', 'https://kp4cd-fixture.test')
    _clean()
    asset_url = 'https://kp4cd.org/sites/default/files/images/example_egl_method.png'
    payload = [{'title': 'EGL method - example',
               'body': f'<p><img src="{asset_url}" style="width:800px;" /></p>'}]
    responses.get('https://kp4cd-fixture.test/rest/views/eglmethod', json=payload)
    got = client.get('/api/kpn/rest/views/eglmethod?from=examplemethod').json()
    assert got == payload
    assert asset_url in got[0]['body']
    assert '/api/kpn/files/' not in got[0]['body']
    stored = q.get_view_rows(engine, 'eglmethod', item_key='examplemethod')
    assert stored == payload
    assert asset_url in stored[0]['body']
    assert '/api/kpn/files/' not in stored[0]['body']


# --- security: view_name / egldata-kind charset + length hardening ---

@responses.activate
def test_invalid_view_name_charset_blocks_proxy(monkeypatch):
    monkeypatch.setenv('KPN_CMS_PROXY_ON_MISS', 'true')
    monkeypatch.setenv('KPN_CMS_SOURCE_HOST', 'https://kp4cd-fixture.test')
    _clean()
    # %3F/%3D decode to '?'/'='; nothing is registered with `responses`, so any
    # outbound call would raise ConnectionError -- the clean [] plus proxied=False
    # below together prove no outbound request was ever attempted.
    got = client.get('/api/kpn/rest/views/x%3Fevil%3D1')
    assert got.status_code == 200
    assert got.json() == []
    misses = q.get_misses(engine)
    assert misses and misses[0]['view_name'] == 'x?evil=1' and not misses[0]['proxied']


def test_long_view_name_is_clamped_before_miss_record(monkeypatch):
    monkeypatch.setenv('KPN_CMS_PROXY_ON_MISS', 'false')
    _clean()
    long_name = 'a' * 250
    got = client.get(f'/api/kpn/rest/views/{long_name}')
    assert got.status_code == 200
    assert got.json() == []
    misses = q.get_misses(engine)
    assert misses and len(misses[0]['view_name']) <= 128


@responses.activate
def test_egldata_invalid_kind_charset_blocks_proxy(monkeypatch):
    monkeypatch.setenv('KPN_CMS_PROXY_ON_MISS', 'true')
    monkeypatch.setenv('KPN_CMS_SOURCE_HOST', 'https://kp4cd-fixture.test')
    _clean()
    got = client.get('/api/kpn/egldata/bogus%3Fx')
    assert got.status_code == 200
    assert got.json() == []
    misses = q.get_misses(engine)
    assert misses and not misses[0]['proxied']


# --- egldata ---

def test_egldata_empty_when_no_data(monkeypatch):
    monkeypatch.setenv('KPN_CMS_PROXY_ON_MISS', 'false')
    _clean()
    assert client.get('/api/kpn/egldata/config?dataset=x&trait=y').json() == []


# --- files ---

@mock_aws
def test_files_redirects_to_presigned_s3(monkeypatch):
    monkeypatch.setenv('KPN_CMS_PROXY_ON_MISS', 'false')
    s3 = boto3.client('s3', region_name='us-east-1')
    bucket = kpn_cms.BUCKET
    s3.create_bucket(Bucket=bucket)
    s3.put_object(Bucket=bucket, Key='kpn-cms-assets/images/news.svg', Body=b'<svg/>')
    got = client.get('/api/kpn/files/images/news.svg', follow_redirects=False)
    assert got.status_code == 307
    assert 'kpn-cms-assets/images/news.svg' in got.headers['location']


@mock_aws
def test_files_404_when_missing(monkeypatch):
    s3 = boto3.client('s3', region_name='us-east-1')
    s3.create_bucket(Bucket=kpn_cms.BUCKET)
    assert client.get('/api/kpn/files/nope.png', follow_redirects=False).status_code == 404


# --- misses endpoint requires auth ---

def test_misses_route_absent():
    # The dedicated /api/kpn/misses endpoint was removed — gap review queries
    # the cms_request_miss table directly. Whatever handler catches this path
    # now, it must not expose the miss log.
    resp = client.get('/api/kpn/misses')
    assert 'hit_count' not in resp.text


# --- datasetinfo: served from kp_datasets, not the cms_content_item snapshot ---

from datetime import datetime

from dataregistry.api import kp_datasets_query as kq


def _kp_row(nid, dataset_id, published=1, portals='md, a2f',
            updated=datetime(2024, 6, 1)):
    return {'dataset_id': dataset_id, 'title': f'title {nid}',
            'body': '<h3>Experiment summary</h3><p>s</p>', 'portals': portals,
            'published': published, 'drupal_nid': nid,
            'drupal_author': 'a@broadinstitute.org', 'migration_note': None,
            'created_at': datetime(2024, 1, 1), 'updated_at': updated}


def _clean_kp():
    with engine.connect() as con:
        con.execute(text("TRUNCATE TABLE kp_datasets"))
        con.commit()


def test_datasetinfo_keyed_hit_returns_envelope():
    _clean(); _clean_kp()
    kq.upsert_kp_dataset(engine, _kp_row(1704, 'Satoshi2024_TGtoHDL_EU'))
    rows = client.get('/api/kpn/rest/views/datasetinfo',
                      params={'datasetid': 'Satoshi2024_TGtoHDL_EU'}).json()
    assert len(rows) == 1
    assert rows[0]['field_dataset_id'] == [{'value': 'Satoshi2024_TGtoHDL_EU'}]
    assert rows[0]['nid'] == [{'value': 1704}]
    assert rows[0]['body'][0]['format'] == 'full_html'


@responses.activate
def test_datasetinfo_miss_returns_empty_without_proxy_or_miss_record():
    # responses.activate with nothing registered: any outbound HTTP would blow
    # up, so a clean [] proves the proxy path was never entered.
    _clean(); _clean_kp()
    rows = client.get('/api/kpn/rest/views/datasetinfo',
                      params={'datasetid': 'Nope2020_X'}).json()
    assert rows == []
    with engine.connect() as con:
        n = con.execute(text("SELECT COUNT(*) FROM cms_request_miss")).fetchone()[0]
    assert n == 0


def test_datasetinfo_unpublished_not_served():
    _clean(); _clean_kp()
    kq.upsert_kp_dataset(engine, _kp_row(1, 'Hidden2020_X', published=0))
    assert client.get('/api/kpn/rest/views/datasetinfo',
                      params={'datasetid': 'Hidden2020_X'}).json() == []
    listing = client.get('/api/kpn/rest/views/datasetinfo').json()
    assert 'Hidden2020_X' not in [r['field_dataset_id'][0]['value'] for r in listing if r['field_dataset_id']]


def test_datasetinfo_listing_caps_at_10_newest_first_with_portal_filter():
    _clean(); _clean_kp()
    for i in range(12):
        kq.upsert_kp_dataset(engine, _kp_row(300 + i, f'DS{i}_X',
                                             portals='md, a2f' if i % 2 else 'cvd',
                                             updated=datetime(2024, 1, 1 + i)))
    listing = client.get('/api/kpn/rest/views/datasetinfo').json()
    assert len(listing) == 10
    assert listing[0]['field_dataset_id'] == [{'value': 'DS11_X'}]
    md = client.get('/api/kpn/rest/views/datasetinfo', params={'portal': 'md'}).json()
    assert {r['field_dataset_id'][0]['value'] for r in md} == {f'DS{i}_X' for i in (1, 3, 5, 7, 9, 11)}
