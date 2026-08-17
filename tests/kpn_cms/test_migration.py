from sqlalchemy import text

from dataregistry.api.db import DataRegistryReadWriteDB

db = DataRegistryReadWriteDB()


def _columns(table):
    with db.get_engine().connect() as con:
        rows = con.execute(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = DATABASE() AND table_name = :t"), {'t': table}).fetchall()
    return {r[0] for r in rows}


def test_cms_content_item_table():
    assert {'id', 'view_name', 'portal', 'nid', 'item_key', 'payload',
            'search_text', 'sort_order', 'imported_at'} <= _columns('cms_content_item')


def test_cms_asset_table():
    assert {'id', 'remote_url', 's3_key', 'content_type', 'size', 'status',
            'imported_at'} <= _columns('cms_asset')


def test_cms_request_miss_table():
    assert {'id', 'view_name', 'query_string', 'proxied', 'response_status',
            'hit_count', 'first_seen', 'last_seen'} <= _columns('cms_request_miss')


def test_kp_datasets_table():
    assert {'id', 'dataset_id', 'title', 'body', 'portals', 'published',
            'registry_dataset_id', 'drupal_nid', 'drupal_author',
            'migration_note', 'created_at', 'updated_at'} <= _columns('kp_datasets')
