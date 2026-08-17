import uuid
from datetime import datetime

import pytest
from sqlalchemy import text

from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api import kp_datasets_query as kq
from dataregistry.api import kpn_cms_query as q

engine = DataRegistryReadWriteDB().get_engine()


def _clean():
    with engine.begin() as con:
        con.execute(text("SET FOREIGN_KEY_CHECKS = 0"))
        con.execute(text("TRUNCATE TABLE kp_datasets"))
        con.execute(text("TRUNCATE TABLE cms_content_item"))
        con.execute(text("DELETE FROM datasets WHERE name LIKE 'KPTEST_%'"))
        con.execute(text("SET FOREIGN_KEY_CHECKS = 1"))


def _row(nid, dataset_id=None, title='T', body='<p>b</p>', portals='md, a2f',
         published=1, note=None, updated=datetime(2024, 6, 1)):
    return {'dataset_id': dataset_id, 'title': title, 'body': body,
            'portals': portals, 'published': published, 'drupal_nid': nid,
            'drupal_author': 'a@broadinstitute.org', 'migration_note': note,
            'created_at': datetime(2024, 1, 1), 'updated_at': updated}


def _mk_registry_dataset(name):
    ds_id = uuid.uuid4().hex.encode()
    study_id = uuid.uuid4().hex.encode()
    with engine.begin() as con:
        # Create study first to satisfy foreign key constraint
        con.execute(text("""
            INSERT INTO studies (id, name, institution, created_at)
            VALUES (:id, :study_name, :inst, NOW())
        """), {'id': study_id, 'study_name': f'study_{study_id.decode()[:8]}', 'inst': 'test'})
        con.execute(text("""
            INSERT INTO datasets (id, name, data_source_type, data_type, genome_build,
                ancestry, sex, global_sample_size, status, data_submitter,
                data_submitter_email, data_contributor, data_contributor_email,
                study_id, description, created_at, publicly_available, user_id)
            VALUES (:id, :name, 'file', 'gwas', 'grch38', 'EU', 'mixed', 100, 'open',
                's', 's@x.org', 'c', 'c@x.org', :sid, 'desc', NOW(), 1, 1)
        """), {'id': ds_id, 'name': name, 'sid': study_id})
    return ds_id


def test_upsert_is_idempotent_by_drupal_nid():
    _clean()
    kq.upsert_kp_dataset(engine, _row(1703, dataset_id='KPTEST_A', title='old'))
    kq.upsert_kp_dataset(engine, _row(1703, dataset_id='KPTEST_A', title='new'))
    with engine.connect() as con:
        n = con.execute(text("SELECT COUNT(*) FROM kp_datasets")).scalar()
    assert n == 1
    assert kq.get_by_dataset_id(engine, 'KPTEST_A')['title'] == 'new'


def test_get_by_dataset_id_respects_published_flag():
    _clean()
    kq.upsert_kp_dataset(engine, _row(1, dataset_id='KPTEST_UNPUB', published=0))
    assert kq.get_by_dataset_id(engine, 'KPTEST_UNPUB') is None
    assert kq.get_by_dataset_id(engine, 'KPTEST_UNPUB', published_only=False)['drupal_nid'] == 1


def test_list_recent_orders_caps_and_excludes_unpublished():
    _clean()
    for i in range(12):
        kq.upsert_kp_dataset(engine, _row(100 + i, dataset_id=f'KPTEST_{i}',
                                          updated=datetime(2024, 1, 1 + i)))
    kq.upsert_kp_dataset(engine, _row(200, dataset_id='KPTEST_HIDDEN',
                                      published=0, updated=datetime(2025, 1, 1)))
    got = kq.list_recent(engine)
    assert len(got) == 10
    assert got[0]['dataset_id'] == 'KPTEST_11'          # newest first
    assert all(r['published'] for r in got)


def test_list_recent_portal_filter_is_token_exact():
    _clean()
    kq.upsert_kp_dataset(engine, _row(1, dataset_id='KPTEST_MD', portals='md, a2f'))
    kq.upsert_kp_dataset(engine, _row(2, dataset_id='KPTEST_MDEP', portals='mdep, a2f'))
    got = kq.list_recent(engine, portal='md')
    assert [r['dataset_id'] for r in got] == ['KPTEST_MD']   # 'mdep' must not match


def test_backfill_links_exact_name_matches_only():
    _clean()
    exact_id = _mk_registry_dataset('KPTEST_Exact')
    _mk_registry_dataset('KPTEST_lower')
    kq.upsert_kp_dataset(engine, _row(1, dataset_id='KPTEST_Exact'))
    kq.upsert_kp_dataset(engine, _row(2, dataset_id='KPTEST_LOWER'))  # case differs
    kq.upsert_kp_dataset(engine, _row(3, dataset_id='KPTEST_Missing'))
    assert kq.backfill_registry_links(engine) == 1
    assert kq.get_by_dataset_id(engine, 'KPTEST_Exact')['registry_dataset_id'] == exact_id
    assert kq.get_by_dataset_id(engine, 'KPTEST_LOWER')['registry_dataset_id'] is None
    assert kq.get_by_dataset_id(engine, 'KPTEST_Missing')['registry_dataset_id'] is None


def test_delete_cms_datasetinfo_rows_leaves_other_views():
    _clean()
    q.replace_view_rows(engine, 'datasetinfo', 'item_key', 'X',
                        [{'view_name': 'datasetinfo', 'portal': None, 'nid': None,
                          'item_key': 'X', 'payload': '{}', 'search_text': None,
                          'sort_order': 0}])
    q.replace_view_rows(engine, 'help_book', None, None,
                        [{'view_name': 'help_book', 'portal': None, 'nid': None,
                          'item_key': None, 'payload': '{}', 'search_text': None,
                          'sort_order': 0}])
    assert kq.delete_cms_datasetinfo_rows(engine) == 1
    assert q.get_view_rows(engine, 'datasetinfo') == []
    assert q.get_view_rows(engine, 'help_book') == [{}]
