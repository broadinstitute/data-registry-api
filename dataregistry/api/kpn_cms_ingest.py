"""Drupal-row -> storage-row conversion helpers shared by the request path and the import CLI.

Used by both dataregistry/api/kpn_cms.py (proxy-on-miss, runs inside the API
process) and scripts/import_kpn_cms.py (offline import CLI). Lives under
dataregistry/ rather than scripts/ so the API router never imports from
scripts/ -- the Docker image only ships dataregistry/ (see Dockerfile), so an
import from scripts/ there would 500 on every proxy-persist.
"""
import html
import json
import re

_TAG_RE = re.compile(r'<[^>]+>')


def strip_html(s):
    if not s:
        return ''
    return re.sub(r'\s+', ' ', html.unescape(_TAG_RE.sub(' ', s))).strip()


def _field_value(val):
    """Unwrap Drupal's full-entity field format (e.g. datasetinfo's
    field_dataset_id: [{'value': 'x'}]) down to the bare scalar. Views REST
    export rows (news2vueportal, etc.) already hand back flat scalars and
    pass through unchanged. Evidence: tests/kpn_cms/fixtures/datasetinfo_sample.json.
    """
    if isinstance(val, list) and val and isinstance(val[0], dict) and 'value' in val[0]:
        return val[0]['value']
    return val


def rows_for(view_name, drupal_rows, portal=None, nid=None, item_key=None):
    rows = []
    for i, item in enumerate(drupal_rows):
        search = strip_html(' '.join(str(_field_value(item.get(f)) or '') for f in ('title', 'body')))
        row_nid = nid if nid is not None else _field_value(item.get('nid'))
        rows.append({'view_name': view_name, 'portal': portal,
                     'nid': str(row_nid) if row_nid is not None else None,
                     'item_key': item_key, 'payload': json.dumps(item),
                     'search_text': search or None, 'sort_order': i})
    return rows
