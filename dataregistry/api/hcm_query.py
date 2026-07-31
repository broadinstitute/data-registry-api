import json
import uuid
from typing import Optional

from sqlalchemy import text

from dataregistry.api.hcm_model import HCMGWASFile, HCMGWASValidationJob


def _format_uuid(raw) -> str:
    """Convert a binary(32) field (stored as ASCII hex bytes) to a dash-formatted UUID."""
    hex_str = raw.decode('ascii') if isinstance(raw, (bytes, bytearray)) else raw
    return f"{hex_str[:8]}-{hex_str[8:12]}-{hex_str[12:16]}-{hex_str[16:20]}-{hex_str[20:]}"


def _parse_hcm_gwas_row(row) -> dict:
    """Parse a raw DB row into a clean dict, handling JSON fields."""
    d = dict(row)
    for key in ('id',):
        if d.get(key) is not None:
            d[key] = _format_uuid(d[key])
    if isinstance(d.get('column_mapping'), str):
        d['column_mapping'] = json.loads(d['column_mapping'])
    if isinstance(d.get('metadata'), str):
        d['metadata'] = json.loads(d['metadata'])
    return d


def insert_hcm_gwas_file(engine, gwas_file: HCMGWASFile) -> str:
    """Insert a new HCM GWAS file record. Returns the file ID as a hex string."""
    with engine.connect() as conn:
        file_id = str(uuid.uuid4()).replace('-', '')

        conn.execute(text("""
            INSERT INTO hcm_gwas_files
            (id, cohort_name, sarc, ancestry, sex, genome_build, software, analyst,
             file_name, file_size, s3_path, uploaded_by, column_mapping, cases, controls, metadata)
            VALUES
            (:id, :cohort_name, :sarc, :ancestry, :sex, :genome_build, :software, :analyst,
             :file_name, :file_size, :s3_path, :uploaded_by, :column_mapping, :cases, :controls, :metadata)
        """), {
            'id': file_id,
            'cohort_name': gwas_file.cohort_name,
            'sarc': gwas_file.sarc,
            'ancestry': gwas_file.ancestry,
            'sex': gwas_file.sex,
            'genome_build': gwas_file.genome_build,
            'software': gwas_file.software,
            'analyst': gwas_file.analyst,
            'file_name': gwas_file.file_name,
            'file_size': gwas_file.file_size,
            's3_path': gwas_file.s3_path,
            'uploaded_by': gwas_file.uploaded_by,
            'column_mapping': json.dumps(gwas_file.column_mapping),
            'cases': gwas_file.cases,
            'controls': gwas_file.controls,
            'metadata': json.dumps(gwas_file.metadata) if gwas_file.metadata else None,
        })
        conn.commit()
        return file_id


def get_hcm_gwas_file_by_id(engine, file_id: str) -> Optional[dict]:
    """Get a single HCM GWAS file by ID."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT id, cohort_name, sarc, ancestry, sex, genome_build, software, analyst,
                   file_name, file_size, s3_path, uploaded_at, uploaded_by,
                   column_mapping, cases, controls, metadata
            FROM hcm_gwas_files
            WHERE id = :file_id
        """), {'file_id': file_id.replace('-', '')}).mappings().first()

        return _parse_hcm_gwas_row(result) if result else None


def get_hcm_gwas_file_by_s3_path(engine, s3_path: str) -> Optional[dict]:
    """Get an HCM GWAS file by its S3 path. Returns the file dict or None."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT id, cohort_name, sarc, ancestry, sex, genome_build, software, analyst,
                   file_name, file_size, s3_path, uploaded_at, uploaded_by
            FROM hcm_gwas_files
            WHERE s3_path = :s3_path
        """), {'s3_path': s3_path}).mappings().first()

        return _parse_hcm_gwas_row(result) if result else None


def get_all_hcm_gwas_files(engine) -> list:
    """Get all HCM GWAS files ordered by cohort, ancestry, upload date."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT id, cohort_name, sarc, ancestry, sex, genome_build, software, analyst,
                   file_name, file_size, s3_path, uploaded_at, uploaded_by,
                   column_mapping, cases, controls, metadata
            FROM hcm_gwas_files
            ORDER BY cohort_name ASC, ancestry ASC, uploaded_at DESC
        """)).mappings().all()

        return [_parse_hcm_gwas_row(row) for row in result]


def get_hcm_gwas_files_by_cohort(engine, cohort_name: str) -> list:
    """Get all HCM GWAS files for a specific cohort."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT id, cohort_name, sarc, ancestry, sex, genome_build, software, analyst,
                   file_name, file_size, s3_path, uploaded_at, uploaded_by,
                   column_mapping, cases, controls, metadata
            FROM hcm_gwas_files
            WHERE cohort_name = :cohort_name
            ORDER BY uploaded_at DESC
        """), {'cohort_name': cohort_name}).mappings().all()

        return [_parse_hcm_gwas_row(row) for row in result]


def get_hcm_gwas_files_by_uploader(engine, uploaded_by: str) -> list:
    """Get all HCM GWAS files uploaded by a specific user."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT id, cohort_name, sarc, ancestry, sex, genome_build, software, analyst,
                   file_name, file_size, s3_path, uploaded_at, uploaded_by,
                   column_mapping, cases, controls, metadata
            FROM hcm_gwas_files
            WHERE uploaded_by = :uploaded_by
            ORDER BY uploaded_at DESC
        """), {'uploaded_by': uploaded_by}).mappings().all()

        return [_parse_hcm_gwas_row(row) for row in result]


def delete_hcm_gwas_file(engine, file_id: str) -> bool:
    """Delete an HCM GWAS file by file_id. Returns True if deleted, False if not found."""
    with engine.connect() as conn:
        result = conn.execute(text("DELETE FROM hcm_gwas_files WHERE id = :file_id"),
                              {'file_id': file_id.replace('-', '')})
        conn.commit()
        return result.rowcount > 0


# ---------------------------------------------------------------------------
# Validation job queries
# ---------------------------------------------------------------------------

def _parse_validation_job_row(row) -> dict:
    d = dict(row)
    for key in ('id', 'file_id'):
        if d.get(key) is not None:
            d[key] = _format_uuid(d[key])
    if isinstance(d.get('error_summary'), str):
        d['error_summary'] = json.loads(d['error_summary'])
    return d


def insert_hcm_gwas_validation_job(engine, job: HCMGWASValidationJob) -> str:
    """Insert a new validation job record. Returns the job ID as a hex string."""
    with engine.connect() as conn:
        job_id = str(uuid.uuid4()).replace('-', '')
        conn.execute(text("""
            INSERT INTO hcm_gwas_validation_jobs
            (id, file_id, batch_job_id, status, progress_s3_key, submitted_by)
            VALUES (:id, :file_id, :batch_job_id, :status, :progress_s3_key, :submitted_by)
        """), {
            'id': job_id,
            'file_id': str(job.file_id).replace('-', ''),
            'batch_job_id': job.batch_job_id,
            'status': job.status,
            'progress_s3_key': job.progress_s3_key,
            'submitted_by': job.submitted_by,
        })
        conn.commit()
        return job_id


def update_hcm_gwas_validation_job_status(
    engine, job_id: str, status: str,
    total_rows: int = None, errors_found: int = None,
    error_summary: list = None, batch_job_id: str = None
):
    """Update validation job status and optional result fields."""
    with engine.connect() as conn:
        params = {'id': job_id, 'status': status}
        set_clauses = ['status = :status']

        if total_rows is not None:
            params['total_rows'] = total_rows
            set_clauses.append('total_rows = :total_rows')
        if errors_found is not None:
            params['errors_found'] = errors_found
            set_clauses.append('errors_found = :errors_found')
        if error_summary is not None:
            params['error_summary'] = json.dumps(error_summary)
            set_clauses.append('error_summary = :error_summary')
        if batch_job_id is not None:
            params['batch_job_id'] = batch_job_id
            set_clauses.append('batch_job_id = :batch_job_id')
        if status in ('COMPLETED', 'FAILED'):
            set_clauses.append('completed_at = NOW()')

        conn.execute(text(f"""
            UPDATE hcm_gwas_validation_jobs
            SET {', '.join(set_clauses)}
            WHERE id = :id
        """), params)
        conn.commit()


def get_hcm_gwas_validation_jobs_by_file_id(engine, file_id: str) -> list:
    """Get all validation jobs for a given GWAS file, most recent first."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT id, file_id, batch_job_id, status, total_rows, errors_found,
                   error_summary, progress_s3_key, submitted_at, completed_at, submitted_by
            FROM hcm_gwas_validation_jobs
            WHERE file_id = :file_id
            ORDER BY submitted_at DESC
        """), {'file_id': str(file_id).replace('-', '')}).mappings().all()
        return [_parse_validation_job_row(row) for row in result]


# ---------------------------------------------------------------------------
# MA run/result queries
# ---------------------------------------------------------------------------

_HCM_MA_COLS = """SELECT id, label, status, dataset_file_ids,
        maf_min, info_min, meta_lambda_gc, n_meta_variants, n_genome_wide_sig,
        n_cohorts, n_cohorts_used, total_cases, total_controls,
        manhattan_s3_key, qq_s3_key, meta_s3_key, summary_json_s3_key,
        summary_tsv_s3_key, top_loci_s3_key, batch_job_id, error_message,
        submitted_by, created_at, updated_at"""


def _format_hcm_ma_row(d: dict) -> dict:
    # id is stored as the raw ASCII bytes of the dashless hex string in a
    # binary(32) column (same convention as hcm_gwas_files / hcm_gwas_validation_jobs
    # above) -- NOT via UNHEX/HEX, which would require a 64-hex-char (32 byte) value.
    if d.get('id') is not None:
        raw = d['id']
        d['id'] = raw.decode('ascii') if isinstance(raw, (bytes, bytearray)) else raw
    if isinstance(d.get("dataset_file_ids"), str):
        d["dataset_file_ids"] = json.loads(d["dataset_file_ids"])
    return d


def insert_hcm_ma_run(engine, *, label=None, dataset_file_ids=None,
                      maf_min=None, info_min=None, submitted_by=None) -> str:
    """Create a fresh PENDING HCM MA run row. Returns the dashless hex id."""
    run_id = str(uuid.uuid4()).replace('-', '')
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO hcm_gwas_ma_results
                (id, label, status, dataset_file_ids, maf_min, info_min, submitted_by)
            VALUES (:id, :label, 'PENDING', :dfi, :maf, :info, :by)
        """), {'id': run_id, 'label': label,
               'dfi': json.dumps(list(dataset_file_ids)) if dataset_file_ids is not None else None,
               'maf': maf_min, 'info': info_min, 'by': submitted_by})
        conn.commit()
    return run_id


def get_hcm_ma_run(engine, run_id: str):
    with engine.connect() as conn:
        row = conn.execute(
            text(_HCM_MA_COLS + " FROM hcm_gwas_ma_results WHERE id = :id"),
            {'id': run_id.replace('-', '')}).mappings().first()
    return _format_hcm_ma_row(dict(row)) if row else None


def update_hcm_ma_result(engine, run_id: str, *, status: str, batch_job_id=None,
                         meta_lambda_gc=None, n_meta_variants=None,
                         n_genome_wide_sig=None, n_cohorts=None, n_cohorts_used=None,
                         total_cases=None, total_controls=None, manhattan_s3_key=None,
                         qq_s3_key=None, meta_s3_key=None, summary_json_s3_key=None,
                         summary_tsv_s3_key=None, top_loci_s3_key=None,
                         error_message=None) -> None:
    """Partial update: only non-None fields written (COALESCE). Raises ValueError
    if no row matches run_id."""
    with engine.connect() as conn:
        res = conn.execute(text("""
            UPDATE hcm_gwas_ma_results SET status = :status,
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
        """), {'status': status, 'batch_job_id': batch_job_id,
               'meta_lambda_gc': meta_lambda_gc, 'n_meta_variants': n_meta_variants,
               'n_genome_wide_sig': n_genome_wide_sig, 'n_cohorts': n_cohorts,
               'n_cohorts_used': n_cohorts_used, 'total_cases': total_cases,
               'total_controls': total_controls, 'manhattan_s3_key': manhattan_s3_key,
               'qq_s3_key': qq_s3_key, 'meta_s3_key': meta_s3_key,
               'summary_json_s3_key': summary_json_s3_key,
               'summary_tsv_s3_key': summary_tsv_s3_key,
               'top_loci_s3_key': top_loci_s3_key, 'error_message': error_message,
               'run_id': run_id.replace('-', '')})
        if res.rowcount == 0:
            raise ValueError(f"No hcm_gwas_ma_results row found for run_id={run_id}")
        conn.commit()


def get_hcm_ma_results(engine) -> list:
    with engine.connect() as conn:
        rows = conn.execute(text(
            _HCM_MA_COLS + " FROM hcm_gwas_ma_results ORDER BY created_at DESC")).mappings().all()
    return [_format_hcm_ma_row(dict(r)) for r in rows]


# ---------------------------------------------------------------------------
# Liftover job queries
# ---------------------------------------------------------------------------

def _format_hcm_liftover_row(d: dict) -> dict:
    # id/file_id are stored as the raw ASCII bytes of the dashless hex string in
    # binary(32) columns (same convention as hcm_gwas_files / hcm_gwas_ma_results
    # above) -- NOT via UNHEX/HEX.
    for key in ('id', 'file_id'):
        raw = d.get(key)
        if raw is not None:
            d[key] = raw.decode('ascii') if isinstance(raw, (bytes, bytearray)) else raw
    if isinstance(d.get('summary'), (str, bytes, bytearray)):
        d['summary'] = json.loads(d['summary'])
    return d


def insert_hcm_liftover_pending(engine, file_id: str, source_build: str, target_build: str,
                                original_s3_path: str, unmapped_s3_path: str,
                                submitted_by: str) -> str:
    """Create a PENDING hcm_liftover_jobs row; return its dashless hex id."""
    lid = str(uuid.uuid4()).replace('-', '')
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO hcm_liftover_jobs
                (id, file_id, source_genome_build, target_genome_build, status,
                 submitted_by, original_s3_path, unmapped_s3_path)
            VALUES (:id, :file_id, :src, :tgt, 'PENDING', :by, :orig, :unmapped)
        """), {'id': lid, 'file_id': str(file_id).replace('-', ''),
               'src': source_build, 'tgt': target_build,
               'by': submitted_by, 'orig': original_s3_path, 'unmapped': unmapped_s3_path})
        conn.commit()
    return lid


def update_hcm_liftover_job(engine, liftover_id: str, *, status: str,
                            batch_job_id=None, log=None, summary=None,
                            completed: bool = False) -> None:
    """Partial update of a hcm_liftover_jobs row. None fields are left untouched
    (COALESCE); completed=True stamps completed_at=NOW()."""
    completed_clause = ", completed_at = NOW()" if completed else ""
    with engine.connect() as conn:
        conn.execute(text(f"""
            UPDATE hcm_liftover_jobs
            SET status = :status,
                batch_job_id = COALESCE(:batch_job_id, batch_job_id),
                log = COALESCE(:log, log),
                summary = COALESCE(:summary, summary)
                {completed_clause}
            WHERE id = :id
        """), {'id': liftover_id.replace('-', ''), 'status': status,
               'batch_job_id': batch_job_id, 'log': log,
               'summary': json.dumps(summary) if summary is not None else None})
        conn.commit()


def get_hcm_liftover_jobs(engine) -> list[dict]:
    """All HCM liftover jobs, most recent first."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, file_id, source_genome_build, target_genome_build, batch_job_id,
                   status, submitted_at, completed_at, submitted_by,
                   original_s3_path, unmapped_s3_path, summary, log
            FROM hcm_liftover_jobs ORDER BY submitted_at DESC
        """)).mappings().all()
    return [_format_hcm_liftover_row(dict(r)) for r in rows]


def get_hcm_liftover_job_for_file(engine, file_id: str) -> dict | None:
    """The most recent liftover job for one HCM GWAS file (summary decoded), or None."""
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT id, file_id, source_genome_build, target_genome_build, batch_job_id,
                   status, submitted_at, completed_at, submitted_by,
                   original_s3_path, unmapped_s3_path, summary, log
            FROM hcm_liftover_jobs
            WHERE file_id = :file_id
            ORDER BY submitted_at DESC
            LIMIT 1
        """), {"file_id": str(file_id).replace('-', '')}).mappings().first()
    if row is None:
        return None
    return _format_hcm_liftover_row(dict(row))


def set_hcm_gwas_file_build(engine, file_id: str, genome_build: str) -> None:
    """Set the genome build column on a hcm_gwas_files row (plain column, not JSON)."""
    with engine.connect() as conn:
        conn.execute(text("""
            UPDATE hcm_gwas_files
            SET genome_build = :build
            WHERE id = :id
        """), {'id': str(file_id).replace('-', ''), 'build': genome_build})
        conn.commit()
