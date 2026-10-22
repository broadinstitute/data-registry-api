"""SGC meta-analysis (MA) result DB helpers, kept apart from ``query.py`` on purpose.

Worker images (sgc_metal) import this module without the API's heavy dependencies
(fastapi, pegasus, the pydantic models). ``query.py`` re-exports everything here.
"""
import json
import uuid
from typing import Optional

from sqlalchemy import text


_MA_SELECT_COLS = """
    SELECT
        id, phenotype, ancestry, sex, status,
        meta_lambda_gc, n_meta_variants, n_genome_wide_sig, n_cohorts, n_cohorts_used,
        total_cases, total_controls,
        manhattan_s3_key, qq_s3_key, meta_s3_key,
        summary_json_s3_key, summary_tsv_s3_key, top_loci_s3_key,
        batch_job_id, error_message,
        label, run_type, dataset_file_ids, maf_min, info_min, submitted_by,
        created_at, updated_at
"""

MA_LIST_SQL = _MA_SELECT_COLS + " FROM sgc_gwas_ma_results ORDER BY created_at DESC"


def _format_sgc_ma_row(d: dict) -> dict:
    """Format a raw DB row for sgc_gwas_ma_results: binary id -> hex str, dataset_file_ids JSON -> list."""
    if isinstance(d.get('id'), (bytes, bytearray)):
        d['id'] = d['id'].decode('ascii')
    v = d.get('dataset_file_ids')
    if isinstance(v, (str, bytes, bytearray)):
        d['dataset_file_ids'] = json.loads(v)
    return d


def insert_sgc_ma_run(engine, phenotype: str, ancestry: str, *, sex: str = "All",
                      run_type: str = "auto",
                      label=None, dataset_file_ids=None, maf_min=None, info_min=None,
                      submitted_by=None) -> str:
    """Create a fresh PENDING MA run row (never an upsert). Returns the hex run_id."""
    run_id = str(uuid.uuid4()).replace('-', '')
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO sgc_gwas_ma_results
                (id, phenotype, ancestry, sex, status, run_type, label,
                 dataset_file_ids, maf_min, info_min, submitted_by)
            VALUES (:id, :phenotype, :ancestry, :sex, 'PENDING', :run_type, :label,
                 :dataset_file_ids, :maf_min, :info_min, :submitted_by)
        """), {
            'id': run_id, 'phenotype': phenotype, 'ancestry': ancestry, 'sex': sex,
            'run_type': run_type, 'label': label,
            'dataset_file_ids': json.dumps(list(dataset_file_ids)) if dataset_file_ids is not None else None,
            'maf_min': maf_min, 'info_min': info_min, 'submitted_by': submitted_by,
        })
        conn.commit()
    return run_id


def get_sgc_ma_run(engine, run_id: str) -> Optional[dict]:
    """A single MA run by run_id (hex), or None."""
    with engine.connect() as conn:
        rs = conn.execute(text(_MA_SELECT_COLS + " FROM sgc_gwas_ma_results WHERE id = :id"),
                          {'id': run_id})
        row = rs.fetchone()
    return _format_sgc_ma_row(dict(row._mapping)) if row else None


def update_sgc_ma_result(
    engine,
    run_id: str,
    *,
    status: str,
    batch_job_id: Optional[str] = None,
    meta_lambda_gc: Optional[float] = None,
    n_meta_variants: Optional[int] = None,
    n_genome_wide_sig: Optional[int] = None,
    n_cohorts: Optional[int] = None,
    n_cohorts_used: Optional[int] = None,
    total_cases: Optional[int] = None,
    total_controls: Optional[int] = None,
    manhattan_s3_key: Optional[str] = None,
    qq_s3_key: Optional[str] = None,
    meta_s3_key: Optional[str] = None,
    summary_json_s3_key: Optional[str] = None,
    summary_tsv_s3_key: Optional[str] = None,
    top_loci_s3_key: Optional[str] = None,
    error_message: Optional[str] = None,
) -> None:
    """Partial update: only non-None fields are written; others left alone via COALESCE.

    Optional fields default to None, in which case the existing DB value is preserved
    (COALESCE). Raises ValueError if no row matches run_id.
    """
    with engine.connect() as conn:
        result = conn.execute(text("""
            UPDATE sgc_gwas_ma_results
            SET status = :status,
                batch_job_id = COALESCE(:batch_job_id, batch_job_id),
                meta_lambda_gc = COALESCE(:meta_lambda_gc, meta_lambda_gc),
                n_meta_variants = COALESCE(:n_meta_variants, n_meta_variants),
                n_genome_wide_sig = COALESCE(:n_genome_wide_sig, n_genome_wide_sig),
                n_cohorts = COALESCE(:n_cohorts, n_cohorts),
                n_cohorts_used = COALESCE(:n_cohorts_used, n_cohorts_used),
                total_cases = COALESCE(:total_cases, total_cases),
                total_controls = COALESCE(:total_controls, total_controls),
                manhattan_s3_key = COALESCE(:manhattan_s3_key, manhattan_s3_key),
                qq_s3_key = COALESCE(:qq_s3_key, qq_s3_key),
                meta_s3_key = COALESCE(:meta_s3_key, meta_s3_key),
                summary_json_s3_key = COALESCE(:summary_json_s3_key, summary_json_s3_key),
                summary_tsv_s3_key = COALESCE(:summary_tsv_s3_key, summary_tsv_s3_key),
                top_loci_s3_key = COALESCE(:top_loci_s3_key, top_loci_s3_key),
                error_message = COALESCE(:error_message, error_message)
            WHERE id = :run_id
        """), {
            'status': status,
            'batch_job_id': batch_job_id,
            'meta_lambda_gc': meta_lambda_gc,
            'n_meta_variants': n_meta_variants,
            'n_genome_wide_sig': n_genome_wide_sig,
            'n_cohorts': n_cohorts,
            'n_cohorts_used': n_cohorts_used,
            'total_cases': total_cases,
            'total_controls': total_controls,
            'manhattan_s3_key': manhattan_s3_key,
            'qq_s3_key': qq_s3_key,
            'meta_s3_key': meta_s3_key,
            'summary_json_s3_key': summary_json_s3_key,
            'summary_tsv_s3_key': summary_tsv_s3_key,
            'top_loci_s3_key': top_loci_s3_key,
            'error_message': error_message,
            'run_id': run_id,
        })
        if result.rowcount == 0:
            raise ValueError(
                f"No sgc_gwas_ma_results row found for run_id={run_id}"
            )
        conn.commit()


def get_sgc_ma_results(engine) -> list[dict]:
    """All meta-analysis runs, ordered by created_at desc (most recently created first)."""
    with engine.connect() as conn:
        rs = conn.execute(text(MA_LIST_SQL))
        return [_format_sgc_ma_row(dict(r._mapping)) for r in rs]


def delete_sgc_ma_run(engine, run_id: str) -> bool:
    """Delete one MA run row. Returns False if no row matched.

    S3 artifacts are the endpoint's problem: read the row for its keys before
    calling this, because afterwards there is nothing left to read them from.
    """
    with engine.begin() as conn:
        return conn.execute(text("DELETE FROM sgc_gwas_ma_results WHERE id = :id"),
                            {'id': run_id}).rowcount > 0
