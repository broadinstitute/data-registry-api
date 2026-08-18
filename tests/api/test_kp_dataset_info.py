import uuid

import responses

from dataregistry.api.jwt_utils import get_encoded_jwt_data
from dataregistry.api.model import User
from tests.conftest import client

AUTH = {'Authorization': f"Bearer {get_encoded_jwt_data(User(user_name='test', roles=['admin'], id=1))}"}


@responses.activate
def test_kp_portals_lists_bioindex_groups():
    from dataregistry.api import portals
    portals.get_portals.cache_clear()
    responses.get('https://bioindex.hugeamp.org/api/portal/groups',
                  json={'data': [{'name': 'md'}, {'name': 'a2f'}, {'name': 'cvd'}]})
    resp = client.get('/api/kp-portals', headers=AUTH)
    assert resp.status_code == 200
    assert resp.json() == ['a2f', 'cvd', 'md']


def test_kp_portals_requires_auth():
    from dataregistry.api import portals
    portals.get_portals.cache_clear()
    assert client.get('/api/kp-portals').status_code == 401


def _mk_dataset():
    study = client.post('/api/studies',
                        json={'name': f'KPI Study {uuid.uuid4().hex[:8]}', 'institution': 'Broad'},
                        headers=AUTH).json()
    ds = client.post('/api/datasets', json={
        'name': f'KPITEST_{uuid.uuid4().hex[:8]}', 'data_source_type': 'file',
        'data_type': 'gwas', 'genome_build': 'grch38', 'ancestry': 'EU',
        'data_submitter': 's', 'data_submitter_email': 's@x.org',
        'data_contributor': 'c', 'data_contributor_email': 'c@x.org',
        'sex': 'mixed', 'global_sample_size': 100, 'status': 'open',
        'description': 'the description', 'study_id': study['id'].replace('-', ''),
        'pub_id': None, 'publication': 'Big GWAS Paper 2026',
        'publicly_available': True}, headers=AUTH).json()
    return ds


def _mock_bioindex():
    from dataregistry.api import portals, phenotypes
    portals.get_portals.cache_clear()
    phenotypes.get_phenotypes.cache_clear()
    responses.get('https://bioindex.hugeamp.org/api/portal/groups',
                  json={'data': [{'name': 'md'}, {'name': 'a2f'}]})
    responses.get('https://bioindex.hugeamp.org/api/portal/phenotypes',
                  json={'data': [{'name': 'T2D', 'description': 'Type 2 diabetes',
                                  'dichotomous': True}]})


@responses.activate
def test_get_info_returns_null_before_save_then_roundtrips():
    _mock_bioindex()
    ds = _mk_dataset()
    assert client.get(f"/api/kp-dataset-info/{ds['id']}", headers=AUTH).json() is None
    saved = client.post('/api/kp-dataset-info', json={
        'dataset_id': ds['id'], 'title': 'My GWAS: European ancestry',
        'portals': ['md', 'a2f'], 'experiment_summary': 'A GWAS of things.'},
        headers=AUTH)
    assert saved.status_code == 200
    body = saved.json()['body']
    assert '<h3>Publication</h3><p>Big GWAS Paper 2026</p>' in body
    assert '<h3>Experiment summary</h3><p>A GWAS of things.</p>' in body
    got = client.get(f"/api/kp-dataset-info/{ds['id']}", headers=AUTH).json()
    assert got['title'] == 'My GWAS: European ancestry'
    assert got['portals'] == ['md', 'a2f']
    assert got['dataset_id'] == ds['name']
    assert got['experiment_summary'] == 'A GWAS of things.'


@responses.activate
def test_post_validates_title_and_portals():
    _mock_bioindex()
    ds = _mk_dataset()
    base = {'dataset_id': ds['id'], 'title': 'T', 'portals': ['md'], 'experiment_summary': 's'}
    assert client.post('/api/kp-dataset-info', json={**base, 'title': '  '},
                       headers=AUTH).status_code == 422
    assert client.post('/api/kp-dataset-info', json={**base, 'portals': []},
                       headers=AUTH).status_code == 422
    assert client.post('/api/kp-dataset-info', json={**base, 'portals': ['md', 'mskkp']},
                       headers=AUTH).status_code == 422
    assert client.post('/api/kp-dataset-info', json={**base, 'title': 'x' * 501},
                       headers=AUTH).status_code == 422


@responses.activate
def test_saved_info_is_served_by_datasetinfo_endpoint():
    _mock_bioindex()
    ds = _mk_dataset()
    client.post('/api/kp-dataset-info', json={
        'dataset_id': ds['id'], 'title': 'Served Title', 'portals': ['md'],
        'experiment_summary': 's'}, headers=AUTH)
    rows = client.get('/api/kpn/rest/views/datasetinfo',
                      params={'datasetid': ds['name']}).json()
    assert rows and rows[0]['title'] == [{'value': 'Served Title'}]
    assert rows[0]['field_portals'] == [{'value': 'md'}]


@responses.activate
def test_post_conflicts_when_name_collides_with_other_linked_row():
    _mock_bioindex()
    ds1 = _mk_dataset()
    ds2 = _mk_dataset()
    base = {'title': 'T', 'portals': ['md'], 'experiment_summary': 's'}
    client.post('/api/kp-dataset-info', json={**base, 'dataset_id': ds1['id']}, headers=AUTH)
    # rename ds2 to ds1's name via the DB is contrived; instead simulate the
    # collision by saving info for ds2 after inserting a kp row that already
    # holds ds2's name but is linked to a DIFFERENT registry id
    from dataregistry.api.db import DataRegistryReadWriteDB
    from dataregistry.api import kp_datasets_query as kq
    engine2 = DataRegistryReadWriteDB().get_engine()
    kq.upsert_portal_info(engine2, uuid.uuid4().hex.encode(), ds2['name'], 'other', 'md', '<p>b</p>')
    resp = client.post('/api/kp-dataset-info', json={**base, 'dataset_id': ds2['id']}, headers=AUTH)
    assert resp.status_code == 409
