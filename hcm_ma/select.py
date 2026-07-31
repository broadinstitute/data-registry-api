"""Select HCM GWAS files for a free-form meta-analysis and adapt HCM's canonical
column_mapping to the shared reader's col_* keys."""
import json
from sqlalchemy import text, bindparam

from sgc_ma.select import normalize_build

# HCM canonical column_mapping key -> sgc_ma.reader col_* key.
_COLMAP = {
    "chromosome": "col_chromosome",
    "position": "col_position",
    "effect_allele": "col_effect_allele",
    "non_effect_allele": "col_non_effect_allele",
    "beta": "col_beta",
    "standard_error": "col_se",
    "p_value": "col_pvalue",
    "effect_allele_frequency": "col_effect_allele_freq",
    "imputation_quality": "col_imputation_quality",
    "sample_size": "col_variant_n",
}


def hcm_colmap_to_ma(cm: dict) -> dict:
    """Translate an HCM column_mapping into the reader's col_* keys. Only keys
    present in cm are emitted; header values pass through unchanged."""
    return {_COLMAP[k]: v for k, v in (cm or {}).items() if k in _COLMAP}


def _coerce_map(raw) -> dict:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, (bytes, bytearray)):
        return json.loads(raw.decode())
    return json.loads(raw)


_LIST_SQL = """
    SELECT CAST(id AS CHAR) AS file_id, cohort_name, sarc, ancestry, sex,
           genome_build, cases, controls, file_name
    FROM hcm_gwas_files
    ORDER BY cohort_name, ancestry, sarc, sex
"""


def list_eligible_files(engine) -> list[dict]:
    """All HCM GWAS files with a normalized build and an `eligible` flag
    (GRCh38-effective). GRCh37/unknown rows are returned with eligible=False so
    the UI can show them disabled."""
    with engine.connect() as c:
        rows = [dict(r._mapping) for r in c.execute(text(_LIST_SQL))]
    out = []
    for r in rows:
        nb = normalize_build(r["genome_build"])
        out.append({**r, "genome_build": nb, "eligible": nb == "GRCh38"})
    return out


_BY_IDS_SQL = """
    SELECT CAST(id AS CHAR) AS file_id, cohort_name AS cohort, file_name AS dataset,
           s3_path, column_mapping, cases, controls
    FROM hcm_gwas_files
    WHERE id IN :file_ids
"""


def select_files_by_ids(engine, file_ids) -> list[dict]:
    """Worker input rows for an explicit set of file_ids (authoritative — no
    re-filtering). column_mapping is decoded to a dict; s3_path is a
    bucket-relative key."""
    ids = list(file_ids or [])
    if not ids:
        return []
    q = text(_BY_IDS_SQL).bindparams(bindparam("file_ids", expanding=True))
    with engine.connect() as c:
        rows = [dict(r._mapping) for r in c.execute(q, {"file_ids": ids})]
    for r in rows:
        r["column_mapping"] = _coerce_map(r["column_mapping"])
    return rows
