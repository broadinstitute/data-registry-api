import json
import uuid
from typing import Optional

from sqlalchemy import text

from dataregistry.api.hcm_model import HCMGWASFile


def _parse_hcm_gwas_row(row) -> dict:
    """Parse a raw DB row into a clean dict, handling JSON fields."""
    d = dict(row)
    for key in ('id',):
        if isinstance(d.get(key), (bytes, bytearray)):
            d[key] = d[key].hex()
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
