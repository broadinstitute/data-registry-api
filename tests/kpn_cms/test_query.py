import json

from sqlalchemy import text

from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api import kpn_cms_query as q

db = DataRegistryReadWriteDB()
engine = db.get_engine()


def _clean():
    with engine.connect() as con:
        for t in ('cms_content_item', 'cms_asset', 'cms_request_miss'):
            con.execute(text(f"TRUNCATE TABLE {t}"))
        con.commit()


def _row(view, payload, portal=None, nid=None, item_key=None, sort_order=0, search_text=None):
    return {'view_name': view, 'portal': portal, 'nid': nid, 'item_key': item_key,
            'payload': json.dumps(payload), 'search_text': search_text, 'sort_order': sort_order}


def test_canonical_key_sorts_and_keeps_empty():
    assert q.canonical_key({'b': '2', 'a': '1'}) == 'a=1&b=2'
    assert q.canonical_key({'trait': '', 'dataset': 'x'}) == 'dataset=x&trait='


def test_replace_and_get_ordered():
    _clean()
    rows = [_row('news2vueportal', {'title': 'second'}, portal='md', sort_order=1),
            _row('news2vueportal', {'title': 'first'}, portal='md', sort_order=0)]
    assert q.replace_view_rows(engine, 'news2vueportal', 'portal', 'md', rows) == 2
    got = q.get_view_rows(engine, 'news2vueportal', portal='md')
    assert [r['title'] for r in got] == ['first', 'second']


def test_replace_is_scoped_to_filter_value():
    _clean()
    q.replace_view_rows(engine, 'news2vueportal', 'portal', 'md', [_row('news2vueportal', {'t': 1}, portal='md')])
    q.replace_view_rows(engine, 'news2vueportal', 'portal', 'cvd', [_row('news2vueportal', {'t': 2}, portal='cvd')])
    q.replace_view_rows(engine, 'news2vueportal', 'portal', 'md', [_row('news2vueportal', {'t': 3}, portal='md')])
    assert q.get_view_rows(engine, 'news2vueportal', portal='cvd') == [{'t': 2}]
    assert q.get_view_rows(engine, 'news2vueportal', portal='md') == [{'t': 3}]


def test_get_by_nid_and_item_key():
    _clean()
    q.replace_view_rows(engine, 'content_by_id', 'nid', '1770', [_row('content_by_id', {'nid': '1770'}, nid='1770')])
    q.replace_view_rows(engine, 'datasetinfo', 'item_key', 'GWAS_x', [_row('datasetinfo', {'d': 1}, item_key='GWAS_x')])
    assert q.get_view_rows(engine, 'content_by_id', nid='1770') == [{'nid': '1770'}]
    assert q.get_view_rows(engine, 'datasetinfo', item_key='GWAS_x') == [{'d': 1}]


def test_unscoped_replace_replaces_whole_view():
    _clean()
    q.replace_view_rows(engine, 'help_book', None, None, [_row('help_book', {'v': 1})])
    q.replace_view_rows(engine, 'help_book', None, None, [_row('help_book', {'v': 2})])
    assert q.get_view_rows(engine, 'help_book') == [{'v': 2}]


def test_search_help_book_fulltext_and_like():
    _clean()
    q.replace_view_rows(engine, 'help_book', None, None, [
        _row('help_book', {'title': 'GWAS tutorial'}, search_text='GWAS tutorial walkthrough guide'),
        _row('help_book', {'title': 'other'}, search_text='unrelated content entirely')])
    got = q.search_help_book(engine, 'tutorial')
    assert [r['title'] for r in got] == ['GWAS tutorial']


def test_record_miss_dedupes_and_counts():
    _clean()
    q.record_miss(engine, 'oddview', 'portal=md', False, None)
    q.record_miss(engine, 'oddview', 'portal=md', True, 200)
    misses = q.get_misses(engine)
    assert len(misses) == 1
    assert misses[0]['hit_count'] == 2
    assert misses[0]['proxied'] in (1, True)


def test_upsert_asset_idempotent():
    _clean()
    q.upsert_asset(engine, 'https://kp4cd.org/sites/default/files/a.png', 'kpn-cms-assets/a.png', 'image/png', 10, 'mirrored')
    q.upsert_asset(engine, 'https://kp4cd.org/sites/default/files/a.png', 'kpn-cms-assets/a.png', 'image/png', 12, 'mirrored')
    with engine.connect() as con:
        n, size = con.execute(text("SELECT COUNT(*), MAX(size) FROM cms_asset")).fetchone()
    assert (n, size) == (1, 12)
