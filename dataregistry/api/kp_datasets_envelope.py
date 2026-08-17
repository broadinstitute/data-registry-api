"""Synthesize the Drupal node envelope the portal frontends expect from a
kp_datasets row (captured live shape: tests/kpn_cms/fixtures/datasetinfo_sample.json).

Authored fields carry real values. Drupal bookkeeping fields the frontends
never read (uuid, vid, revision_*, uid, ...) get deterministic synthetic
values so responses are stable across requests and deploys.
"""
import uuid

_DATE_FMT = 'Y-m-d\\TH:i:sP'
_TYPE_UUID = '5dfdf16a-041b-40a0-8deb-37d1255a4bc6'   # kp_dataset node type uuid on kp4cd.org
_UID = 22                                             # historical content-author uid on kp4cd.org
_USER_UUID = '1b3bbbf7-364b-4ff6-86e8-6bf26e3d6cf9'
_NID_OFFSET = 100000                                  # synthetic nids for post-Drupal rows


def _ts(dt):
    return {'value': dt.strftime('%Y-%m-%dT%H:%M:%S+00:00'), 'format': _DATE_FMT}


def node_envelope(row):
    nid = row['drupal_nid'] if row['drupal_nid'] is not None else _NID_OFFSET + row['id']
    user = {'target_id': _UID, 'target_type': 'user', 'target_uuid': _USER_UUID,
            'url': f'/user/{_UID}'}
    return {
        'nid': [{'value': nid}],
        'uuid': [{'value': str(uuid.uuid5(uuid.NAMESPACE_URL, f'kpn-dataset-{nid}'))}],
        'vid': [{'value': nid}],
        'langcode': [{'value': 'en'}],
        'type': [{'target_id': 'kp_dataset', 'target_type': 'node_type',
                  'target_uuid': _TYPE_UUID}],
        'revision_timestamp': [_ts(row['updated_at'])],
        'revision_uid': [dict(user)],
        'revision_log': [],
        'status': [{'value': bool(row['published'])}],
        'uid': [dict(user)],
        'title': [{'value': row['title']}],
        'created': [_ts(row['created_at'])],
        'changed': [_ts(row['updated_at'])],
        'promote': [{'value': True}],
        'sticky': [{'value': False}],
        'default_langcode': [{'value': True}],
        'revision_translation_affected': [{'value': True}],
        'path': [{'alias': None, 'pid': None, 'langcode': 'en'}],
        'body': [{'value': row['body'], 'format': 'full_html',
                  'processed': row['body'], 'summary': ''}],
        'field_dataset_id': ([{'value': row['dataset_id']}]
                             if row['dataset_id'] is not None else []),
        'field_portals': [{'value': row['portals']}],
    }
