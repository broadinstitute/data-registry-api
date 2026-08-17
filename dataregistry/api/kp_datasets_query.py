"""Store operations for kp_datasets — the system of record for portal-facing
KP dataset info after the kp4cd.org migration.

Spec: docs/superpowers/specs/2026-08-14-kp-datasets-migration-design.md
"""
from sqlalchemy import text


def upsert_kp_dataset(engine, row) -> None:
    """Insert or update by drupal_nid (the migration's idempotency key)."""
    with engine.begin() as con:
        con.execute(text("""
            INSERT INTO kp_datasets
                (dataset_id, title, body, portals, published, drupal_nid,
                 drupal_author, migration_note, created_at, updated_at)
            VALUES (:dataset_id, :title, :body, :portals, :published,
                    :drupal_nid, :drupal_author, :migration_note,
                    :created_at, :updated_at)
            ON DUPLICATE KEY UPDATE
                dataset_id = VALUES(dataset_id), title = VALUES(title),
                body = VALUES(body), portals = VALUES(portals),
                published = VALUES(published),
                drupal_author = VALUES(drupal_author),
                migration_note = VALUES(migration_note),
                created_at = VALUES(created_at), updated_at = VALUES(updated_at)
        """), row)


def get_by_dataset_id(engine, dataset_id, published_only=True):
    sql = "SELECT * FROM kp_datasets WHERE dataset_id = :d"
    if published_only:
        sql += " AND published = 1"
    with engine.connect() as con:
        r = con.execute(text(sql), {'d': dataset_id}).mappings().fetchone()
    return dict(r) if r else None


def list_recent(engine, portal=None, limit=10):
    """Published rows, newest updated first; optional exact portal-code filter.

    Portal filtering happens in Python: portals is a comma-separated string
    ('md, t2d, a2f') and a SQL LIKE would let 'md' match 'mdep'. The table
    tops out around a thousand rows, so fetching published rows is cheap.
    """
    with engine.connect() as con:
        rows = [dict(r) for r in con.execute(text(
            "SELECT * FROM kp_datasets WHERE published = 1 "
            "ORDER BY updated_at DESC, id DESC")).mappings().fetchall()]
    if portal is not None:
        rows = [r for r in rows
                if portal in [p.strip() for p in r['portals'].split(',')]]
    return rows[:limit]


def backfill_registry_links(engine) -> int:
    """Link kp_datasets rows to datasets rows sharing the exact name.

    BINARY comparison: MySQL's default collation is case-insensitive, but the
    spec requires exact matches only.
    """
    with engine.begin() as con:
        res = con.execute(text("""
            UPDATE kp_datasets k
            JOIN datasets d ON BINARY d.name = BINARY k.dataset_id
            SET k.registry_dataset_id = d.id
        """))
    return res.rowcount


def delete_cms_datasetinfo_rows(engine) -> int:
    """Drop the superseded Drupal-snapshot rows; serving now uses kp_datasets."""
    with engine.begin() as con:
        res = con.execute(text(
            "DELETE FROM cms_content_item WHERE view_name = 'datasetinfo'"))
    return res.rowcount
