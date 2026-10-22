"""Select the per-cohort GWAS to meta-analyze for a (phenotype, ancestry)."""
import json
import os
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
    """MA eligibility, sex-agnostic: GRCh38-effective.
    Sex/ancestry targeting is applied by resolve_target(). Dataset naming is
    deliberately not consulted: portal MA products are never written to
    sgc_gwas_files (they live under their own run-id S3 prefix and
    sgc_gwas_ma_results), and uploads may legitimately be named anything --
    Rotterdam's contributed GWAS are called meta_analysis_*."""
    return normalize_build(row.get("genome_build")) == "GRCh38"


def not_ignored(row: dict) -> bool:
    """False iff this row's file has an active MA ignore-list entry (the
    ignore is file-based, surfaced here as `ignore_reason` from the
    sgc_ma_ignore join)."""
    return row.get("ignore_reason") is None


def is_munged(row: dict) -> bool:
    """True iff the file has a SUCCEEDED munge result (surfaced as
    `munge_s3_key` from the sgc_gwas_munge_results join). METAL consumes the
    standardised output, so un-munged files are not MA candidates for it."""
    return bool(row.get("munge_s3_key"))


METAL_JOB_DEFINITION = "sgc-gwas-metal-job"
_TRUTHY = ("1", "true", "yes")


def munge_required() -> bool:
    """Whether MA candidates must be munged. SGC_MA_REQUIRE_MUNGE, when set, decides
    ("1"/"true"/"yes" = on); otherwise on iff MA jobs go to the METAL worker, i.e. the
    same SGC_MA_JOB_DEFINITION switch submit_ma_batch reads (default sgc-gwas-ma-job:
    the old worker reads raw files, so prod selection is unchanged until the cut-over)."""
    override = os.getenv("SGC_MA_REQUIRE_MUNGE")
    if override is not None:
        return override.strip().lower() in _TRUTHY
    return os.getenv("SGC_MA_JOB_DEFINITION", "sgc-gwas-ma-job") == METAL_JOB_DEFINITION


def _eligible(rows) -> list[dict]:
    """include_file + ignore-list, plus is_munged when munge_required()."""
    need_munge = munge_required()
    return [r for r in rows if include_file(r) and r.get("ignore_reason") is None
            and (not need_munge or is_munged(r))]


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


def matches_target(row: dict, target_ancestry: str, target_sex: str) -> bool:
    """True iff the row is in the target's bucket, before per-cohort dedup.
    The Combined family pools every ancestry at the target sex; ancestry-
    stratified targets are always sex='All'."""
    if target_ancestry == "Combined":
        return str(row.get("sex")) == target_sex
    return (str(row.get("ancestry")) == target_ancestry
            and str(row.get("sex")) == "All")


def resolve_target(rows, target_ancestry, target_sex):
    """Select per-cohort input files for one of the nine MA targets.
    - Combined family (target_ancestry == 'Combined'): sex == target_sex across all
      ancestries, one per cohort preferring the Combined-tagged file (the combined-fallback
      and the sex-stratified 'any ancestry' rules both fall out of this).
    - Ancestry-stratified: ancestry == target_ancestry AND sex == 'All'.
    Returns (selected_rows, warnings)."""
    cand = [r for r in rows if matches_target(r, target_ancestry, target_sex)]
    return _pick_one_per_cohort(cand, prefer_combined=(target_ancestry == "Combined"))


_SQL = """
    SELECT CAST(f.id AS CHAR) AS file_id, f.dataset, f.s3_path, f.column_mapping,
           f.cases, f.controls,
           CAST(f.cohort_id AS CHAR) AS cohort_id, sc.name AS cohort,
           f.ancestry AS ancestry,
           mu.standardised_s3_key AS munge_s3_key, mu.n_in AS munge_n_in, mu.n_out AS munge_n_out,
           mu.n_with_rsid AS munge_n_with_rsid, mu.ref_version AS munge_ref_version,
           mu.has_eaf AS munge_has_eaf, mu.maf_min AS munge_maf_min, mu.info_min AS munge_info_min,
           mi.reason AS ignore_reason,
           JSON_UNQUOTE(JSON_EXTRACT(f.metadata, '$.sex')) AS sex,
           COALESCE(JSON_UNQUOTE(JSON_EXTRACT(f.metadata, '$.genome_build')), JSON_UNQUOTE(JSON_EXTRACT(gc.metadata, '$.genome_build'))) AS genome_build
    FROM sgc_gwas_files f
    JOIN sgc_gwas_plot_results p ON p.file_id = f.id AND p.status = 'SUCCEEDED'
    LEFT JOIN sgc_gwas_cohorts gc ON gc.cohort_id = f.cohort_id
    LEFT JOIN sgc_cohorts sc ON sc.id = f.cohort_id
    LEFT JOIN sgc_ma_ignore mi ON mi.file_id = f.id
    LEFT JOIN sgc_gwas_munge_results mu ON mu.file_id = f.id AND mu.status = 'SUCCEEDED'
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
    """Resolved MA inputs for one target; files on the ignore-list are dropped, and so are
    un-munged files when munge_required()."""
    with engine.connect() as c:
        rows = [dict(r._mapping) for r in c.execute(text(_SQL), {"phenotype": phenotype})]
    selected, _ = resolve_target(_eligible(rows), target_ancestry, target_sex)
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
           mu.standardised_s3_key AS munge_s3_key, mu.n_in AS munge_n_in, mu.n_out AS munge_n_out,
           mu.n_with_rsid AS munge_n_with_rsid, mu.ref_version AS munge_ref_version,
           mu.has_eaf AS munge_has_eaf, mu.maf_min AS munge_maf_min, mu.info_min AS munge_info_min,
           JSON_UNQUOTE(JSON_EXTRACT(f.metadata, '$.sex')) AS sex,
           COALESCE(JSON_UNQUOTE(JSON_EXTRACT(f.metadata, '$.genome_build')), JSON_UNQUOTE(JSON_EXTRACT(gc.metadata, '$.genome_build'))) AS genome_build
    FROM sgc_gwas_files f
    JOIN sgc_gwas_plot_results p ON p.file_id = f.id AND p.status = 'SUCCEEDED'
    LEFT JOIN sgc_gwas_cohorts gc ON gc.cohort_id = f.cohort_id
    LEFT JOIN sgc_cohorts sc ON sc.id = f.cohort_id
    LEFT JOIN sgc_gwas_munge_results mu ON mu.file_id = f.id AND mu.status = 'SUCCEEDED'
    WHERE f.id IN :file_ids
    ORDER BY f.dataset
"""


def list_ma_candidates(engine, phenotype: str, target_ancestry: str, target_sex: str = "All") -> list[dict]:
    """The resolved per-cohort files that WOULD be included for a target for the manual-launch
    preview UI. Ignore-listed files are dropped before resolution, same as select_cohorts, so
    preview == auto."""
    with engine.connect() as c:
        rows = [dict(r._mapping) for r in c.execute(text(_SQL), {"phenotype": phenotype})]
    selected, _ = resolve_target(_eligible(rows), target_ancestry, target_sex)
    return [{
        "file_id": r["file_id"], "cohort": r["cohort"], "dataset": r["dataset"],
        "ancestry": r.get("ancestry"), "sex": r.get("sex"),
        "cases": r["cases"], "controls": r["controls"],
        "genome_build": normalize_build(r["genome_build"]),
        "munged": is_munged(r),
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
    SELECT CAST(f.id AS CHAR) AS file_id, sc.name AS cohort, f.dataset, f.ancestry, mi.reason AS reason,
           JSON_UNQUOTE(JSON_EXTRACT(f.metadata, '$.sex')) AS sex,
           COALESCE(JSON_UNQUOTE(JSON_EXTRACT(f.metadata, '$.genome_build')), JSON_UNQUOTE(JSON_EXTRACT(gc.metadata, '$.genome_build'))) AS genome_build
    FROM sgc_ma_ignore mi
    JOIN sgc_gwas_files f ON f.id = mi.file_id
    LEFT JOIN sgc_gwas_cohorts gc ON gc.cohort_id = f.cohort_id
    LEFT JOIN sgc_cohorts sc ON sc.id = f.cohort_id
    WHERE f.phenotype = :phenotype
    ORDER BY sc.name
"""


def ignored_cohorts(engine, phenotype: str, target_ancestry: str, target_sex: str = "All") -> list[dict]:
    """Ignore-list files for a phenotype's run bucket (for the run summary).
    Filtered the same way selection is -- include_file() (build) and
    matches_target() (ancestry/sex) -- so a file that was never a candidate for
    this run's bucket (wrong sex, wrong ancestry, GRCh37) is never reported as
    "Ignored" for it. genome_build is only needed for that filter and is not
    returned."""
    with engine.connect() as c:
        rows = [dict(r._mapping) for r in c.execute(text(_IGNORED_SQL), {"phenotype": phenotype})]
    return [{"file_id": r["file_id"], "cohort": r["cohort"], "dataset": r["dataset"],
            "ancestry": r["ancestry"], "sex": r["sex"], "reason": r["reason"]}
            for r in rows
            if include_file(r) and matches_target(r, target_ancestry, target_sex)]
