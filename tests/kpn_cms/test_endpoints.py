import pathlib
from datetime import datetime

import boto3
import responses
from fastapi.testclient import TestClient
from moto import mock_aws
from sqlalchemy import text

from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api import kpn_cms
from dataregistry.api import kp_datasets_query as kq
from dataregistry.server import app

client = TestClient(app)
engine = DataRegistryReadWriteDB().get_engine()
FIX = pathlib.Path(__file__).parent / 'fixtures'


def _clean():
    with engine.connect() as con:
        con.execute(text("TRUNCATE TABLE cms_asset"))
        con.commit()


# --- files ---

@mock_aws
def test_files_redirects_to_presigned_s3():
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


# --- datasetinfo: served from kp_datasets, the system of record ---

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
def test_datasetinfo_miss_returns_empty_without_proxy():
    # responses.activate with nothing registered: any outbound HTTP would blow
    # up, so a clean [] proves the proxy path was never entered (there is none
    # -- kp_datasets is the system of record, no proxy-on-miss fallback).
    _clean(); _clean_kp()
    rows = client.get('/api/kpn/rest/views/datasetinfo',
                      params={'datasetid': 'Nope2020_X'}).json()
    assert rows == []


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
