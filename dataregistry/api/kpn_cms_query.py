"""SQL access for the KPN CMS content store (cms_content_item / cms_asset / cms_request_miss)."""
import json

from sqlalchemy import text

SCOPE_COLS = {'portal', 'nid', 'item_key'}


def canonical_key(params: dict) -> str:
    return '&'.join(f'{k}={params[k]}' for k in sorted(params))


def replace_view_rows(engine, view_name, scope_col, scope_val, rows) -> int:
    if scope_col is not None and scope_col not in SCOPE_COLS:
        raise ValueError(f'bad scope_col {scope_col}')
    with engine.begin() as con:
        if scope_col is None:
            con.execute(text("DELETE FROM cms_content_item WHERE view_name = :v"), {'v': view_name})
        else:
            con.execute(text(f"DELETE FROM cms_content_item WHERE view_name = :v AND {scope_col} = :s"),
                        {'v': view_name, 's': scope_val})
        for r in rows:
            con.execute(text("""
                INSERT INTO cms_content_item
                    (view_name, portal, nid, item_key, payload, search_text, sort_order)
                VALUES (:view_name, :portal, :nid, :item_key, :payload, :search_text, :sort_order)
            """), r)
    return len(rows)


def get_view_rows(engine, view_name, portal=None, nid=None, item_key=None):
    sql = "SELECT payload FROM cms_content_item WHERE view_name = :v"
    args = {'v': view_name}
    for col, val in (('portal', portal), ('nid', nid), ('item_key', item_key)):
        if val is not None:
            sql += f" AND {col} = :{col}"
            args[col] = val
    sql += " ORDER BY sort_order"
    with engine.connect() as con:
        return [json.loads(r[0]) for r in con.execute(text(sql), args).fetchall()]


def search_help_book(engine, body_text: str):
    with engine.connect() as con:
        rows = con.execute(text("""
            SELECT payload FROM cms_content_item
            WHERE view_name = 'help_book'
              AND MATCH(search_text) AGAINST (:q IN NATURAL LANGUAGE MODE)
        """), {'q': body_text}).fetchall()
        if not rows:
            rows = con.execute(text("""
                SELECT payload FROM cms_content_item
                WHERE view_name = 'help_book' AND search_text LIKE :like
                ORDER BY sort_order
            """), {'like': f'%{body_text}%'}).fetchall()
    return [json.loads(r[0]) for r in rows]


def record_miss(engine, view_name, query_string, proxied, response_status) -> None:
    with engine.begin() as con:
        con.execute(text("""
            INSERT INTO cms_request_miss (view_name, query_string, proxied, response_status)
            VALUES (:v, :q, :p, :s)
            ON DUPLICATE KEY UPDATE hit_count = hit_count + 1,
                proxied = VALUES(proxied), response_status = VALUES(response_status)
        """), {'v': view_name, 'q': query_string[:600], 'p': proxied, 's': response_status})


def get_misses(engine):
    with engine.connect() as con:
        rows = con.execute(text(
            "SELECT * FROM cms_request_miss ORDER BY hit_count DESC")).mappings().fetchall()
    return [dict(r) for r in rows]


def upsert_asset(engine, remote_url, s3_key, content_type, size, status) -> None:
    with engine.begin() as con:
        con.execute(text("""
            INSERT INTO cms_asset (remote_url, s3_key, content_type, size, status)
            VALUES (:u, :k, :c, :z, :st)
            ON DUPLICATE KEY UPDATE s3_key = VALUES(s3_key), content_type = VALUES(content_type),
                size = VALUES(size), status = VALUES(status)
        """), {'u': remote_url, 'k': s3_key, 'c': content_type, 'z': size, 'st': status})
