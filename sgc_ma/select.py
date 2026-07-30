"""Select the per-cohort GWAS to meta-analyze for a (phenotype, ancestry)."""
import json
from typing import Optional

from sqlalchemy import text, bindparam


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


def classify_liftover_status(normalized_build: Optional[str],
                             latest_job_status: Optional[str]) -> str:
    """Map (normalized build, most-recent sgc_liftover_jobs.status) to the
    at-a-glance gwas-summary column label. Job status wins over build so a
    re-run resolves correctly (a FAILED-then-SUCCEEDED file reads as lifted).
    The six return strings are the frontend contract -- keep them byte-for-byte
    in sync with utils/sgcLiftover.js LIFTOVER_STATUS_OPTIONS."""
    if latest_job_status in ("PENDING", "RUNNING"):
        return "In progress"
    if latest_job_status == "SUCCEEDED":
        return "Lifted to GRCh38"
    if latest_job_status == "FAILED":
        return "Failed"
    # No liftover job on record.
    if normalized_build == "GRCh38":
        return "GRCh38 (native)"
    if normalized_build == "GRCh37":
        return "Needs liftover"
    return "Unknown build"


def include_file(row: dict) -> bool:
    """MA eligibility, sex-agnostic: GRCh38-effective and not a pre-existing MA product.
    Sex/ancestry targeting is applied by resolve_target()."""
    if normalize_build(row.get("genome_build")) != "GRCh38":
        return False
    if str(row.get("dataset", "")).startswith("meta_analysis_"):
        return False
    return True


def not_ignored(row: dict) -> bool:
    """False iff this GWAS has an active MA ignore-list entry for its
    (cohort, phenotype, ancestry)."""
    return row.get("ignore_reason") is None


def _pick_one_per_cohort(rows, prefer_combined):
    """One row per cohort. When prefer_combined, use the cohort's ancestry='Combined' row
    if present; otherwise its sole remaining row. Cohorts with >1 eligible row and no
    Combined file are ambiguous -> skipped, reported in warnings."""
    by_cohort = {}
    for r in rows:
        by_cohort.setdefault(r["cohort_id"], []).append(r)
    selected, warnings = [], []
    for cohort_id, crows in by_cohort.items():
        if prefer_combined:
            combined = [r for r in crows if str(r.get("ancestry")) == "Combined"]
            if combined:
                selected.append(combined[0]); continue
        if len(crows) == 1:
            selected.append(crows[0])
        else:
            warnings.append({"cohort_id": cohort_id, "n_candidates": len(crows)})
    return selected, warnings


def resolve_target(rows, target_ancestry, target_sex):
    """Select per-cohort input files for one of the nine MA targets.
    - Combined family (target_ancestry == 'Combined'): sex == target_sex across all
      ancestries, one per cohort preferring the Combined-tagged file (the combined-fallback
      and the sex-stratified 'any ancestry' rules both fall out of this).
    - Ancestry-stratified: ancestry == target_ancestry AND sex == 'All'.
    Returns (selected_rows, warnings)."""
    if target_ancestry == "Combined":
        cand = [r for r in rows if str(r.get("sex")) == target_sex]
        return _pick_one_per_cohort(cand, prefer_combined=True)
    cand = [r for r in rows
            if str(r.get("ancestry")) == target_ancestry and str(r.get("sex")) == "All"]
    return _pick_one_per_cohort(cand, prefer_combined=False)


_SQL = """
    SELECT CAST(f.id AS CHAR) AS file_id, f.dataset, f.s3_path, f.column_mapping,
           f.cases, f.controls,
           CAST(f.cohort_id AS CHAR) AS cohort_id, sc.name AS cohort,
           f.ancestry AS ancestry,
           mi.reason AS ignore_reason,
           JSON_UNQUOTE(JSON_EXTRACT(f.metadata, '$.sex')) AS sex,
           COALESCE(JSON_UNQUOTE(JSON_EXTRACT(f.metadata, '$.genome_build')), JSON_UNQUOTE(JSON_EXTRACT(gc.metadata, '$.genome_build'))) AS genome_build
    FROM sgc_gwas_files f
    JOIN sgc_gwas_plot_results p ON p.file_id = f.id AND p.status = 'SUCCEEDED'
    LEFT JOIN sgc_gwas_cohorts gc ON gc.cohort_id = f.cohort_id
    LEFT JOIN sgc_cohorts sc ON sc.id = f.cohort_id
    LEFT JOIN sgc_ma_ignore mi ON mi.file_id = f.id
    WHERE f.phenotype = :phenotype
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


def select_cohorts(engine, phenotype: str, target_ancestry: str, target_sex: str = "All") -> list[dict]:
    """Resolved MA inputs for one target; files on the ignore-list are dropped."""
    with engine.connect() as c:
        rows = [dict(r._mapping) for r in c.execute(text(_SQL), {"phenotype": phenotype})]
    eligible = [r for r in rows if include_file(r) and r.get("ignore_reason") is None]
    selected, _ = resolve_target(eligible, target_ancestry, target_sex)
    out = []
    for r in selected:
        r["column_mapping"] = _coerce_map(r["column_mapping"])
        r["genome_build"] = normalize_build(r["genome_build"])
        out.append(r)
    return out


_SQL_BY_IDS = """
    SELECT CAST(f.id AS CHAR) AS file_id, f.dataset, f.s3_path, f.column_mapping,
           f.cases, f.controls,
           CAST(f.cohort_id AS CHAR) AS cohort_id, sc.name AS cohort,
           f.ancestry AS ancestry,
           JSON_UNQUOTE(JSON_EXTRACT(f.metadata, '$.sex')) AS sex,
           COALESCE(JSON_UNQUOTE(JSON_EXTRACT(f.metadata, '$.genome_build')), JSON_UNQUOTE(JSON_EXTRACT(gc.metadata, '$.genome_build'))) AS genome_build
    FROM sgc_gwas_files f
    JOIN sgc_gwas_plot_results p ON p.file_id = f.id AND p.status = 'SUCCEEDED'
    LEFT JOIN sgc_gwas_cohorts gc ON gc.cohort_id = f.cohort_id
    LEFT JOIN sgc_cohorts sc ON sc.id = f.cohort_id
    WHERE f.id IN :file_ids
    ORDER BY f.dataset
"""


def list_ma_candidates(engine, phenotype: str, target_ancestry: str, target_sex: str = "All") -> list[dict]:
    """The resolved per-cohort files that WOULD be included for a target, each flagged
    `ignored` (surfaced, not dropped) for the launch UI."""
    with engine.connect() as c:
        rows = [dict(r._mapping) for r in c.execute(text(_SQL), {"phenotype": phenotype})]
    eligible = [r for r in rows if include_file(r)]
    selected, _ = resolve_target(eligible, target_ancestry, target_sex)
    return [{
        "file_id": r["file_id"], "cohort": r["cohort"], "dataset": r["dataset"],
        "ancestry": r.get("ancestry"), "sex": r.get("sex"),
        "cases": r["cases"], "controls": r["controls"],
        "genome_build": normalize_build(r["genome_build"]),
        "ignored": r.get("ignore_reason") is not None,
    } for r in selected]


def select_cohorts_by_file_ids(engine, file_ids) -> list[dict]:
    """Per-cohort MA input rows for an explicit set of file_ids (the recorded
    selection). No include_file/ignore filtering -- the selection is authoritative."""
    ids = list(file_ids or [])
    if not ids:
        return []
    q = text(_SQL_BY_IDS).bindparams(bindparam("file_ids", expanding=True))
    with engine.connect() as c:
        rows = [dict(r._mapping) for r in c.execute(q, {"file_ids": ids})]
    for r in rows:
        r["column_mapping"] = _coerce_map(r["column_mapping"])
        r["genome_build"] = normalize_build(r["genome_build"])
    return rows


_IGNORED_SQL = """
    SELECT CAST(f.id AS CHAR) AS file_id, sc.name AS cohort, f.dataset, f.ancestry, mi.reason AS reason
    FROM sgc_ma_ignore mi
    JOIN sgc_gwas_files f ON f.id = mi.file_id
    LEFT JOIN sgc_cohorts sc ON sc.id = f.cohort_id
    WHERE f.phenotype = :phenotype
    ORDER BY sc.name
"""


def ignored_cohorts(engine, phenotype: str) -> list[dict]:
    """Ignore-list files for a phenotype (for the run summary)."""
    with engine.connect() as c:
        return [dict(r._mapping) for r in c.execute(text(_IGNORED_SQL), {"phenotype": phenotype})]
