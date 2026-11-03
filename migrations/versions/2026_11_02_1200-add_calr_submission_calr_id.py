"""add public-facing calr_id to calr_submissions

Adds the short, user-visible experiment identifier (CalR + 8 base58 chars) that
share links and /analysis/<calr_id> URLs resolve by, backfills one for every
existing submission, and drops the old hand-typed experiment_id out of the
metadata blob now that the generated id replaces it.

The column is case-sensitive (utf8mb4_bin): base58 ids differ by case, so a
case-insensitive unique key would reject ids that are distinct and let a lookup
with the wrong case match another submission. It ends up NOT NULL.

The submission primary key is untouched: it is a binary(32) column and is
embedded in S3 object keys, so it stays the internal identifier.

MySQL DDL is not transactional, so a failure part-way through leaves the table
half-migrated. Each step either checks what already exists or is idempotent, and
the backfill only touches rows that still have no calr_id, so the migration is
safe to re-run after a partial failure.

Deploy this together with the new application code: calr_id has no default once
it is NOT NULL, so the old code's INSERT into calr_submissions, which omits
calr_id, fails against the migrated table.

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

UNIQUE_KEY = 'calr_submissions_calr_id_uniq'
COLUMN_COMMENT = 'public-facing experiment id, CalR + 8 base58 chars'
COLUMN_TYPE = 'varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin'


def _generate_calr_id() -> str:
    return CALR_ID_PREFIX + ''.join(secrets.choice(ALPHABET) for _ in range(CALR_ID_LENGTH))


def _column_exists(conn) -> bool:
    return conn.execute(text("""
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'calr_submissions'
          AND column_name = 'calr_id'
    """)).scalar() > 0


def _unique_key_exists(conn) -> bool:
    return conn.execute(text("""
        SELECT COUNT(*) FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = 'calr_submissions'
          AND index_name = :index_name
    """), {'index_name': UNIQUE_KEY}).scalar() > 0


def upgrade() -> None:
    conn = op.get_bind()

    # 1. The column, nullable until the backfill is done. If it is already there
    #    (a prior run died, or an earlier revision of this migration created it
    #    with the default case-insensitive collation) bring it to the same
    #    definition rather than failing on a duplicate column.
    nullable_column = f"`calr_id` {COLUMN_TYPE} NULL COMMENT '{COLUMN_COMMENT}'"
    if _column_exists(conn):
        conn.execute(text(f"ALTER TABLE `calr_submissions` MODIFY COLUMN {nullable_column}"))
    else:
        conn.execute(text(f"ALTER TABLE `calr_submissions` ADD COLUMN {nullable_column}"))

    # 2. The unique key.
    if not _unique_key_exists(conn):
        conn.execute(text(f"ALTER TABLE `calr_submissions` ADD UNIQUE KEY `{UNIQUE_KEY}` (`calr_id`)"))

    # 3. Backfill only the rows that have no calr_id yet. The ids already
    #    assigned seed the set, so a new draw cannot collide with one a previous
    #    run handed out. The set compares case-sensitively, like the column.
    assigned = {
        row[0] for row in conn.execute(text(
            "SELECT calr_id FROM calr_submissions WHERE calr_id IS NOT NULL"
        ))
    }
    rows = conn.execute(text(
        "SELECT id, metadata FROM calr_submissions WHERE calr_id IS NULL"
    )).mappings().all()

    # Generate in Python and check against the set; redraw on a collision rather
    # than failing the whole migration on one that is vanishingly unlikely but
    # not impossible. The UNIQUE key is the backstop.
    for row in rows:
        for _ in range(5):
            calr_id = _generate_calr_id()
            if calr_id not in assigned:
                break
        else:
            raise RuntimeError("Could not generate a unique calr_id after 5 attempts")
        assigned.add(calr_id)

        # The generated id supersedes any hand-typed experiment_id. Leaving it
        # in the blob would give the frontend two competing values to read.
        # One UPDATE per row, so a row never has a calr_id while its metadata
        # still needs stripping; rows without an experiment_id keep their
        # metadata untouched.
        raw = row['metadata']
        metadata = json.loads(raw) if isinstance(raw, str) else raw
        if isinstance(metadata, dict) and 'experiment_id' in metadata:
            metadata.pop('experiment_id')
            conn.execute(text(
                "UPDATE calr_submissions SET calr_id = :calr_id, metadata = :metadata WHERE id = :id"
            ), {
                'calr_id': calr_id,
                'metadata': json.dumps(metadata) if metadata else None,
                'id': row['id'],
            })
        else:
            conn.execute(text(
                "UPDATE calr_submissions SET calr_id = :calr_id WHERE id = :id"
            ), {'calr_id': calr_id, 'id': row['id']})

    # 4. Every row has an id now; make it required.
    conn.execute(text(
        f"ALTER TABLE `calr_submissions` MODIFY COLUMN `calr_id` {COLUMN_TYPE} NOT NULL "
        f"COMMENT '{COLUMN_COMMENT}'"
    ))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("""
        ALTER TABLE `calr_submissions`
            DROP INDEX `calr_submissions_calr_id_uniq`,
            DROP COLUMN `calr_id`
    """))
