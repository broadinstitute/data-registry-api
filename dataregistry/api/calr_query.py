import uuid
from typing import Optional

from sqlalchemy import text

from dataregistry.api.calr_model import CALRFile, CalRSession, CALRSubmission


# =============================================================================
# Submissions and files
# =============================================================================

def insert_calr_submission(engine, submission: CALRSubmission) -> str:
    """Insert a new CALR submission record. Returns the submission ID."""
    with engine.connect() as conn:
        sub_id = str(submission.id).replace('-', '') if submission.id else str(uuid.uuid4()).replace('-', '')

        conn.execute(text("""
            INSERT INTO calr_submissions (id, name, description, public, uploaded_by)
            VALUES (:id, :name, :description, :public, :uploaded_by)
        """), {
            'id': sub_id,
            'name': submission.name,
            'description': submission.description,
            'public': 1 if submission.public else 0,
            'uploaded_by': submission.uploaded_by,
        })
        conn.commit()
        return sub_id


def insert_calr_file(engine, calr_file: CALRFile) -> str:
    """Insert a new CALR file record. Returns the file ID."""
    with engine.connect() as conn:
        file_id = str(calr_file.id).replace('-', '') if calr_file.id else str(uuid.uuid4()).replace('-', '')

        conn.execute(text("""
            INSERT INTO calr_files (id, submission_id, file_type, file_name, file_size, s3_path)
            VALUES (:id, :submission_id, :file_type, :file_name, :file_size, :s3_path)
        """), {
            'id': file_id,
            'submission_id': calr_file.submission_id,
            'file_type': calr_file.file_type,
            'file_name': calr_file.file_name,
            'file_size': calr_file.file_size,
            's3_path': calr_file.s3_path,
        })
        conn.commit()
        return file_id


def get_calr_submissions_by_user(engine, uploaded_by: str):
    """Get all CALR submissions for a user, with their associated files."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT s.id AS submission_id, s.name, s.description, s.public,
                   s.uploaded_by, s.uploaded_at,
                   f.id AS file_id, f.file_type, f.file_name, f.file_size
            FROM calr_submissions s
            LEFT JOIN calr_files f ON f.submission_id = s.id
            WHERE s.uploaded_by = :uploaded_by
            ORDER BY s.uploaded_at DESC, f.file_type
        """), {'uploaded_by': uploaded_by}).mappings().all()

        return _group_calr_submissions(rows)


def get_public_calr_submissions(engine):
    """Get all public CALR submissions with their associated files."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT s.id AS submission_id, s.name, s.description, s.public,
                   s.uploaded_by, s.uploaded_at,
                   f.id AS file_id, f.file_type, f.file_name, f.file_size
            FROM calr_submissions s
            LEFT JOIN calr_files f ON f.submission_id = s.id
            WHERE s.public = 1
            ORDER BY s.uploaded_at DESC, f.file_type
        """)).mappings().all()

        return _group_calr_submissions(rows)


def _group_calr_submissions(rows):
    """Group flat submission+file rows into nested submission dicts."""
    submissions = {}
    for row in rows:
        row = dict(row)
        sub_id = row['submission_id']
        if sub_id not in submissions:
            submissions[sub_id] = {
                'id': sub_id,
                'name': row['name'],
                'description': row['description'],
                'public': bool(row['public']),
                'uploaded_by': row['uploaded_by'],
                'uploaded_at': row['uploaded_at'],
                'files': [],
            }
        if row['file_id']:
            submissions[sub_id]['files'].append({
                'id': row['file_id'],
                'file_type': row['file_type'],
                'file_name': row['file_name'],
                'file_size': row['file_size'],
            })
    return list(submissions.values())


def get_calr_file_by_id(engine, file_id: str):
    """Get a CALR file joined with its submission (for public flag / ownership check)."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT f.id, f.submission_id, f.file_type, f.file_name, f.file_size,
                   f.s3_path, f.uploaded_at,
                   s.public, s.uploaded_by
            FROM calr_files f
            JOIN calr_submissions s ON s.id = f.submission_id
            WHERE f.id = :file_id
        """), {'file_id': file_id}).mappings().first()

        return dict(result) if result else None


def get_calr_files_by_submission(engine, submission_id: str):
    """Get all files for a submission with full metadata."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT f.id, f.file_type, f.file_name, f.file_size, f.s3_path,
                   s.uploaded_by
            FROM calr_files f
            JOIN calr_submissions s ON s.id = f.submission_id
            WHERE f.submission_id = :submission_id
        """), {'submission_id': submission_id}).mappings().all()
        return [dict(row) for row in result]


def set_calr_submission_public(engine, submission_id: str, public: bool) -> bool:
    """Set the public flag on a submission. Returns True if the record was found and updated."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            UPDATE calr_submissions SET public = :public WHERE id = :id
        """), {'public': 1 if public else 0, 'id': submission_id})
        conn.commit()
        return result.rowcount > 0


def delete_calr_submission(engine, submission_id: str) -> bool:
    """Delete a CALR submission and its files. Returns True if deleted."""
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM calr_files WHERE submission_id = :submission_id"),
                     {'submission_id': submission_id})
        result = conn.execute(text("DELETE FROM calr_submissions WHERE id = :submission_id"),
                              {'submission_id': submission_id})
        conn.commit()
        return result.rowcount > 0

