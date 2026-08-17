import json
import pathlib
from datetime import datetime

from dataregistry.api.kp_datasets_envelope import node_envelope

FIX = pathlib.Path(__file__).parent / 'fixtures'


def _row(**over):
    row = {'id': 7, 'drupal_nid': 1721, 'dataset_id': 'Small2025_AorticStenosis',
           'title': 'Calcific Aortic Stenosis 2025 GWAS', 'body': '<h3>Publication</h3><p>x</p>',
           'portals': 'md, t2d, a2f', 'published': 1,
           'created_at': datetime(2025, 10, 10, 13, 46, 5),
           'updated_at': datetime(2025, 10, 16, 21, 36, 58)}
    row.update(over)
    return row


def test_envelope_matches_live_fixture_shape():
    fixture = json.loads((FIX / 'datasetinfo_sample.json').read_text())[0]
    env = node_envelope(_row())
    assert set(env) == set(fixture)
    for field, items in fixture.items():
        assert isinstance(env[field], list)
        if items and env[field]:
            assert set(env[field][0]) == set(items[0]), field


def test_envelope_carries_authored_values():
    env = node_envelope(_row())
    assert env['nid'] == [{'value': 1721}]
    assert env['title'] == [{'value': 'Calcific Aortic Stenosis 2025 GWAS'}]
    assert env['field_dataset_id'] == [{'value': 'Small2025_AorticStenosis'}]
    assert env['field_portals'] == [{'value': 'md, t2d, a2f'}]
    assert env['status'] == [{'value': True}]
    assert env['body'][0]['value'] == '<h3>Publication</h3><p>x</p>'
    assert env['body'][0]['format'] == 'full_html'
    assert env['created'][0] == {'value': '2025-10-10T13:46:05+00:00',
                                 'format': 'Y-m-d\\TH:i:sP'}
    assert env['changed'][0]['value'] == '2025-10-16T21:36:58+00:00'


def test_envelope_is_deterministic():
    assert node_envelope(_row()) == node_envelope(_row())


def test_synthetic_nid_for_post_drupal_rows():
    env = node_envelope(_row(drupal_nid=None, id=42))
    assert env['nid'] == [{'value': 100042}]
    assert env['vid'] == [{'value': 100042}]


def test_missing_dataset_id_yields_empty_field():
    assert node_envelope(_row(dataset_id=None))['field_dataset_id'] == []
