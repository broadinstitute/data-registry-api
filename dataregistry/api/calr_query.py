import json
import uuid
from typing import Optional

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from dataregistry.api.calr_ids import generate_calr_id
from dataregistry.api.calr_model import CALRFile, CalRSession, CALRSubmission

# Random ids collide rarely enough that a couple of retries is plenty; more
# attempts would only mask a real problem (a corrupt alphabet, a stuck RNG).
CALR_ID_ATTEMPTS = 3

# MySQL error number for a duplicate-key violation, and the name of the unique key
# on calr_submissions.calr_id (see the add_calr_submission_calr_id migration).
MYSQL_DUPLICATE_ENTRY = 1062
CALR_ID_UNIQUE_KEY = 'calr_submissions_calr_id_uniq'


def _is_calr_id_collision(exc: IntegrityError) -> bool:
    """True if `exc` is a duplicate-key error on the calr_id unique key.

    Any other integrity error (a clashing submission id, a NOT NULL violation) is
    not fixed by trying another calr_id. MySQL 8 reports the key as
    "<table>.<key>", so the key name is matched as a substring of the message.
    """
    args = getattr(exc.orig, 'args', ())
    if len(args) < 2:
        return False
    errno, message = args[0], args[1]
    return errno == MYSQL_DUPLICATE_ENTRY and CALR_ID_UNIQUE_KEY in str(message)


# =============================================================================
# Submissions and files
# =============================================================================

def insert_calr_submission(engine, submission: CALRSubmission) -> tuple:
    """Insert a new CALR submission record. Returns (submission_id, calr_id).

    Every submission gets a public-facing calr_id at creation, draft or not, so
    the id is stable and visible from the moment the experiment exists.
    """
    with engine.connect() as conn:
        sub_id = str(submission.id).replace('-', '') if submission.id else str(uuid.uuid4()).replace('-', '')

        for attempt in range(CALR_ID_ATTEMPTS):
            calr_id = generate_calr_id()
            try:
                conn.execute(text("""
                    INSERT INTO calr_submissions (id, calr_id, name, description, public, uploaded_by)
                    VALUES (:id, :calr_id, :name, :description, :public, :uploaded_by)
                """), {
                    'id': sub_id,
                    'calr_id': calr_id,
                    'name': submission.name,
                    'description': submission.description,
                    'public': 1 if submission.public else 0,
                    'uploaded_by': submission.uploaded_by,
                })
            except IntegrityError as exc:
                conn.rollback()
                # Retry with a fresh calr_id only on a calr_id collision, and only
                # up to CALR_ID_ATTEMPTS; any other integrity error is re-raised
                # immediately.
                if not _is_calr_id_collision(exc) or attempt == CALR_ID_ATTEMPTS - 1:
                    raise
                continue

            conn.commit()
            return sub_id, calr_id


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
            SELECT s.id AS submission_id, s.calr_id, s.name, s.description, s.public, s.shared,
                   s.uploaded_by, s.uploaded_at, s.metadata,
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
            SELECT s.id AS submission_id, s.calr_id, s.name, s.description, s.public, s.shared,
                   s.uploaded_by, s.uploaded_at, s.metadata,
                   f.id AS file_id, f.file_type, f.file_name, f.file_size
            FROM calr_submissions s
            LEFT JOIN calr_files f ON f.submission_id = s.id
            WHERE s.public = 1
            ORDER BY s.uploaded_at DESC, f.file_type
        """)).mappings().all()

        return _group_calr_submissions(rows)


def get_viewable_calr_submission(engine, key: str):
    """Return a submission bundle addressable by a link, or None.

    `key` is either the public-facing calr_id or the legacy submission uuid, so
    share links already in circulation keep resolving. A submission is viewable
    when it is public or has link sharing enabled; callers should not
    distinguish "private" from "missing" in what they return to the client,
    since that difference lets an id space be enumerated.

    Excludes s3_path (via _group_calr_submissions).
    """
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT s.id AS submission_id, s.calr_id, s.name, s.description, s.public, s.shared,
                   s.uploaded_by, s.uploaded_at, s.metadata,
                   f.id AS file_id, f.file_type, f.file_name, f.file_size
            FROM calr_submissions s
            LEFT JOIN calr_files f ON f.submission_id = s.id
            WHERE (s.calr_id = :key OR s.id = :key)
              AND (s.public = 1 OR s.shared = 1)
            ORDER BY f.file_type
        """), {'key': key}).mappings().all()
        grouped = _group_calr_submissions(rows)
        return grouped[0] if grouped else None


def _group_calr_submissions(rows):
    """Group flat submission+file rows into nested submission dicts."""
    submissions = {}
    for row in rows:
        row = dict(row)
        sub_id = row['submission_id']
        if sub_id not in submissions:
            raw_metadata = row['metadata']
            submissions[sub_id] = {
                'id': sub_id,
                'calr_id': row['calr_id'],
                'name': row['name'],
                'description': row['description'],
                'public': bool(row['public']),
                'shared': bool(row['shared']),
                'uploaded_by': row['uploaded_by'],
                'uploaded_at': row['uploaded_at'],
                'metadata': json.loads(raw_metadata) if isinstance(raw_metadata, str) else (raw_metadata or {}),
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
                   s.calr_id, s.public, s.shared, s.uploaded_by
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
                   s.calr_id, s.uploaded_by
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


def set_calr_submission_shared(engine, submission_id: str, shared: bool) -> bool:
    """Set the shared flag on a submission. Returns True if the record was found and updated."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            UPDATE calr_submissions SET shared = :shared WHERE id = :id
        """), {'shared': 1 if shared else 0, 'id': submission_id})
        conn.commit()
        return result.rowcount > 0


def patch_calr_submission_metadata(engine, submission_id: str, patch: dict) -> bool:
    """Merge patch values into the submission. name/description update their columns directly;
    all other fields are merged into the metadata JSON blob. Explicit None values remove JSON keys.
    Returns True if the submission was found and updated."""
    column_fields = {k: patch.pop(k) for k in ('name', 'description') if k in patch}

    with engine.connect() as conn:
        row = conn.execute(text(
            "SELECT metadata FROM calr_submissions WHERE id = :id"
        ), {'id': submission_id}).first()
        if row is None:
            return False

        raw = row[0]
        existing = json.loads(raw) if isinstance(raw, str) else (raw or {})
        for k, v in patch.items():
            if v is None:
                existing.pop(k, None)
            else:
                existing[k] = v

        set_clauses = "metadata = :metadata"
        params = {'metadata': json.dumps(existing) if existing else None, 'id': submission_id}
        if 'name' in column_fields and column_fields['name'] is not None:
            set_clauses += ", name = :name"
            params['name'] = column_fields['name']
        if 'description' in column_fields:
            set_clauses += ", description = :description"
            params['description'] = column_fields['description']

        result = conn.execute(text(
            f"UPDATE calr_submissions SET {set_clauses} WHERE id = :id"
        ), params)
        conn.commit()
        return result.rowcount > 0


def update_calr_file(engine, file_id: str, file_name: str, file_size: int, s3_path: str) -> bool:
    """Update mutable attributes of an existing calr_files record. Returns True if found and updated."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            UPDATE calr_files SET file_name = :file_name, file_size = :file_size, s3_path = :s3_path
            WHERE id = :id
        """), {'file_name': file_name, 'file_size': file_size, 's3_path': s3_path, 'id': file_id})
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

