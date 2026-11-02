"""add public-facing calr_id to calr_submissions

Adds the short, user-visible experiment identifier (CalR + 8 base58 chars) that
share links and /analysis/<calr_id> URLs resolve by, backfills one for every
existing submission, and drops the old hand-typed experiment_id out of the
metadata blob now that the generated id replaces it.

The submission primary key is untouched: it is a binary(32) column and is
embedded in S3 object keys, so it stays the internal identifier.

Revision ID: add_calr_submission_calr_id
Revises: create_sgc_gwas_munge_results
Create Date: 2026-11-02 12:00:00.000000

"""
import json
import secrets

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'add_calr_submission_calr_id'
down_revision = 'create_sgc_gwas_munge_results'
branch_labels = None
depends_on = None

# Deliberately duplicated from dataregistry.api.calr_ids rather than imported:
# a migration has to keep running against a fresh database even if the
# application module is later renamed or reworked.
ALPHABET = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'
CALR_ID_PREFIX = 'CalR'
CALR_ID_LENGTH = 8


def _generate_calr_id() -> str:
    return CALR_ID_PREFIX + ''.join(secrets.choice(ALPHABET) for _ in range(CALR_ID_LENGTH))


def upgrade() -> None:
    conn = op.get_bind()

    conn.execute(text("""
        ALTER TABLE `calr_submissions`
            ADD COLUMN `calr_id` varchar(16) NULL
                COMMENT 'public-facing experiment id, CalR + 8 base58 chars',
            ADD UNIQUE KEY `calr_submissions_calr_id_uniq` (`calr_id`)
    """))

    rows = conn.execute(text(
        "SELECT id, metadata FROM calr_submissions"
    )).mappings().all()

    # Generate in Python and let the UNIQUE key reject duplicates; the retry
    # below keeps the backfill going rather than failing the whole migration on
    # a collision that is vanishingly unlikely but not impossible.
    assigned = set()
    for row in rows:
        for _ in range(5):
            calr_id = _generate_calr_id()
            if calr_id not in assigned:
                break
        else:
            raise RuntimeError("Could not generate a unique calr_id after 5 attempts")
        assigned.add(calr_id)

        conn.execute(text(
            "UPDATE calr_submissions SET calr_id = :calr_id WHERE id = :id"
        ), {'calr_id': calr_id, 'id': row['id']})

        # The generated id supersedes any hand-typed experiment_id. Leaving it
        # in the blob would give the frontend two competing values to read.
        raw = row['metadata']
        metadata = json.loads(raw) if isinstance(raw, str) else raw
        if isinstance(metadata, dict) and 'experiment_id' in metadata:
            metadata.pop('experiment_id')
            conn.execute(text(
                "UPDATE calr_submissions SET metadata = :metadata WHERE id = :id"
            ), {'metadata': json.dumps(metadata) if metadata else None, 'id': row['id']})


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("""
        ALTER TABLE `calr_submissions`
            DROP INDEX `calr_submissions_calr_id_uniq`,
            DROP COLUMN `calr_id`
    """))
