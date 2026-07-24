"""Select the per-cohort GWAS to meta-analyze for a (phenotype, ancestry)."""
import json
from typing import Optional

from sqlalchemy import text


def normalize_build(raw: Optional[str]) -> Optional[str]:
    """Collapse free-text build labels. Anything mentioning 38 (incl. 'liftover
    to GRCh38') is GRCh38-effective; else 37; else None."""
    if not raw:
        return None
    s = str(raw).lower()
    if "38" in s:
        return "GRCh38"
    if "37" in s or "hg19" in s:
        return "GRCh37"
    return None


def include_file(row: dict) -> bool:
    """v1 selection predicate: sex=All, GRCh38-effective, not a pre-existing MA."""
    if str(row.get("sex", "")).lower() != "all":
        return False
    if normalize_build(row.get("genome_build")) != "GRCh38":
        return False
    if str(row.get("dataset", "")).startswith("meta_analysis_"):
        return False
    return True


_SQL = """
    SELECT CAST(f.id AS CHAR) AS file_id, f.dataset, f.s3_path, f.column_mapping,
           f.cases, f.controls,
           CAST(f.cohort_id AS CHAR) AS cohort_id, sc.name AS cohort,
           JSON_UNQUOTE(JSON_EXTRACT(f.metadata, '$.sex')) AS sex,
           JSON_UNQUOTE(JSON_EXTRACT(gc.metadata, '$.genome_build')) AS genome_build
    FROM sgc_gwas_files f
    JOIN sgc_gwas_plot_results p ON p.file_id = f.id AND p.status = 'SUCCEEDED'
    LEFT JOIN sgc_gwas_cohorts gc ON gc.cohort_id = f.cohort_id
    LEFT JOIN sgc_cohorts sc ON sc.id = f.cohort_id
    WHERE f.phenotype = :phenotype AND f.ancestry = :ancestry
    ORDER BY f.dataset
"""


def _coerce_map(raw) -> dict:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, (bytes, bytearray)):
        return json.loads(raw.decode())
    return json.loads(raw)


def select_cohorts(engine, phenotype: str, ancestry: str) -> list[dict]:
    with engine.connect() as c:
        rows = [dict(r._mapping) for r in c.execute(
            text(_SQL), {"phenotype": phenotype, "ancestry": ancestry})]
    out = []
    for r in rows:
        if not include_file(r):
            continue
        r["column_mapping"] = _coerce_map(r["column_mapping"])
        r["genome_build"] = normalize_build(r["genome_build"])
        out.append(r)
    return out
