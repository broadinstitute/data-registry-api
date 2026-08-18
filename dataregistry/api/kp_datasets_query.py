"""Store operations for kp_datasets — the system of record for portal-facing
KP dataset info after the kp4cd.org migration.

Spec: docs/superpowers/specs/2026-08-14-kp-datasets-migration-design.md
"""
from datetime import datetime
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

    FIND_IN_SET over the space-stripped portal list is token-exact ('md'
    never matches 'mdep'), and the LIMIT keeps mediumtext bodies for only
    the returned rows instead of every published row.
    """
    with engine.connect() as con:
        rows = con.execute(text(
            "SELECT * FROM kp_datasets WHERE published = 1 "
            "AND (:p IS NULL OR FIND_IN_SET(:p, REPLACE(portals, ' ', ''))) "
            "ORDER BY updated_at DESC, id DESC LIMIT :n"
        ), {'p': portal, 'n': limit}).mappings().fetchall()
    return [dict(r) for r in rows]


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


def get_by_registry_dataset_id(engine, registry_dataset_id):
    with engine.connect() as con:
        r = con.execute(text(
            "SELECT * FROM kp_datasets WHERE registry_dataset_id = :r"),
            {'r': registry_dataset_id}).mappings().fetchone()
    return dict(r) if r else None


def upsert_portal_info(engine, registry_dataset_id, dataset_id, title, portals, body):
    """Create or update the portal-display row for a registry dataset.

    One KP row per registry dataset, keyed on registry_dataset_id. A
    migrated row that already holds this dataset_id but has no registry
    link yet is adopted (linked + updated) rather than colliding with the
    dataset_id unique key. Timestamps are naive UTC -- node_envelope's
    _ts() depends on that.
    """
    now = datetime.utcnow().replace(microsecond=0)
    with engine.begin() as con:
        row = con.execute(text(
            "SELECT id FROM kp_datasets WHERE registry_dataset_id = :r"),
            {'r': registry_dataset_id}).fetchone()
        if row is None:
            row = con.execute(text(
                "SELECT id FROM kp_datasets WHERE BINARY dataset_id = :d "
                "AND registry_dataset_id IS NULL"), {'d': dataset_id}).fetchone()
        if row:
            con.execute(text("""
                UPDATE kp_datasets SET dataset_id = :d, title = :t, body = :b,
                    portals = :p, published = 1, registry_dataset_id = :r,
                    updated_at = :now
                WHERE id = :i
            """), {'d': dataset_id, 't': title, 'b': body, 'p': portals,
                   'r': registry_dataset_id, 'now': now, 'i': row[0]})
            return row[0]
        res = con.execute(text("""
            INSERT INTO kp_datasets
                (dataset_id, title, body, portals, published,
                 registry_dataset_id, created_at, updated_at)
            VALUES (:d, :t, :b, :p, 1, :r, :now, :now)
        """), {'d': dataset_id, 't': title, 'b': body, 'p': portals,
               'r': registry_dataset_id, 'now': now})
        return res.lastrowid
