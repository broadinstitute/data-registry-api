"""SGC munge-result DB helpers, kept apart from ``query.py`` on purpose.

Worker images (sgc_munge) import this module without the API's heavy dependencies
(fastapi, pegasus, the pydantic models). ``query.py`` re-exports everything here.
"""
import json
import uuid
from typing import Optional

from sqlalchemy import text


_MUNGE_RESULT_COLS = ("batch_job_id", "n_in", "n_out", "n_with_rsid", "n_swapped", "n_dropped",
                      "has_eaf", "has_info", "maf_min", "info_min", "ref_version", "engine",
                      "standardised_s3_key", "munge_json_s3_key", "ref_mismatch_s3_key",
                      "error_message")
MUNGE_UPDATABLE = _MUNGE_RESULT_COLS

_MUNGE_SELECT_COLS = (
    "mu.id, mu.file_id, mu.status, mu.batch_job_id, mu.n_in, mu.n_out, mu.n_with_rsid, "
    "mu.n_swapped, mu.n_dropped, mu.has_eaf, mu.has_info, mu.maf_min, mu.info_min, "
    "mu.ref_version, mu.engine, mu.standardised_s3_key, mu.munge_json_s3_key, "
    "mu.ref_mismatch_s3_key, mu.error_message, mu.created_at, mu.updated_at"
)


def _format_sgc_munge_row(d: dict) -> dict:
    """Format a raw sgc_gwas_munge_results row: binary ids -> hex str, n_dropped JSON -> dict."""
    for key in ('id', 'file_id'):
        if isinstance(d.get(key), (bytes, bytearray)):
            d[key] = d[key].decode('ascii')
    v = d.get('n_dropped')
    if isinstance(v, (str, bytes, bytearray)):
        d['n_dropped'] = json.loads(v)
    return d


def insert_sgc_munge_result_pending(engine, file_id_hex: str) -> str:
    """Create a PENDING munge-result row; if one already exists for this file, reset it to
    PENDING and clear all result fields. Returns the row's hex id (the new id is only used
    for a fresh insert; an existing row keeps its id).
    """
    munge_id = str(uuid.uuid4()).replace('-', '')
    resets = ",\n                ".join(f"{c}=NULL" for c in _MUNGE_RESULT_COLS)
    with engine.connect() as conn:
        conn.execute(text(f"""
            INSERT INTO sgc_gwas_munge_results (id, file_id, status)
            VALUES (:id, :file_id, 'PENDING')
            ON DUPLICATE KEY UPDATE
                status='PENDING',
                {resets}
        """), {'id': munge_id, 'file_id': file_id_hex})
        conn.commit()
    return munge_id


def update_sgc_munge_result(engine, file_id_hex: str, /, *, status: str,
                            clear_error: bool = False, **fields) -> None:
    """Set status plus every non-None field (dynamic SET); ``clear_error=True`` also sets
    ``error_message = NULL`` (a None field is skipped, not written). Raises ValueError for
    an unknown field or when no row exists for the file."""
    unknown = set(fields) - set(MUNGE_UPDATABLE)
    if unknown:
        raise ValueError(f"Unknown sgc_gwas_munge_results field(s): {sorted(unknown)}")
    if clear_error and fields.get('error_message') is not None:
        raise ValueError("clear_error=True contradicts a non-None error_message")
    params = {'status': status, 'file_id': file_id_hex}
    sets = ["status = :status"]
    if clear_error:
        sets.append("error_message = NULL")
    for col in MUNGE_UPDATABLE:
        val = fields.get(col)
        if val is None:
            continue
        if col == 'n_dropped' and not isinstance(val, (str, bytes, bytearray)):
            val = json.dumps(val)
        elif col in ('has_eaf', 'has_info'):
            val = int(bool(val))
        sets.append(f"{col} = :{col}")
        params[col] = val
    with engine.connect() as conn:
        rs = conn.execute(text(f"UPDATE sgc_gwas_munge_results SET {', '.join(sets)} "
                               "WHERE file_id = :file_id"), params)
        if rs.rowcount == 0:
            raise ValueError(f"No sgc_gwas_munge_results row for file_id={file_id_hex}")
        conn.commit()


def get_sgc_munge_result(engine, file_id_hex: str) -> Optional[dict]:
    """The munge-result row for a file (hex id), or None."""
    with engine.connect() as conn:
        row = conn.execute(text(f"SELECT {_MUNGE_SELECT_COLS} FROM sgc_gwas_munge_results mu "
                                "WHERE mu.file_id = :file_id"), {'file_id': file_id_hex}).fetchone()
    return _format_sgc_munge_row(dict(row._mapping)) if row else None


def list_sgc_munge_results(engine) -> list:
    """All munge-result rows joined to their file (dataset, phenotype, ancestry, cohort_id)."""
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT {_MUNGE_SELECT_COLS}, f.dataset, f.phenotype, f.ancestry,
                   CAST(f.cohort_id AS CHAR) AS cohort_id
            FROM sgc_gwas_munge_results mu
            JOIN sgc_gwas_files f ON f.id = mu.file_id
            ORDER BY mu.updated_at DESC
        """)).fetchall()
    return [_format_sgc_munge_row(dict(r._mapping)) for r in rows]
