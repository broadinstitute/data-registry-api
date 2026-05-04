"""drop file_uploads.genome_build (read from metadata.referenceGenome)

Revision ID: drop_file_uploads_genome_build
Revises: add_liftover_tables
Create Date: 2026-04-23 12:00:00.000000

The assembly build for a Hermes upload has always been persisted as part of
the JSON metadata blob (`metadata.referenceGenome`, values "Hg19"/"Hg38").
The dedicated column added in add_liftover_tables was redundant: the upload
path duplicated the build into both places, and the list endpoint never
selected the column, so every `/hermes` row displayed the default 'n/a'.

We drop the column and normalize at read time in application queries
(CASE on `metadata->>'$.referenceGenome'`).  Liftover completion continues
to update the build in-place — now via JSON_SET on the metadata column.
"""
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'drop_file_uploads_genome_build'
down_revision = 'add_liftover_tables'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("""
        ALTER TABLE `file_uploads`
        DROP COLUMN `genome_build`
    """))


def downgrade() -> None:
    conn = op.get_bind()
    # Re-add the column, then backfill from metadata.referenceGenome using the
    # same normalization the application's SELECT does at read-time.
    conn.execute(text("""
        ALTER TABLE `file_uploads`
        ADD COLUMN `genome_build` VARCHAR(16) NOT NULL DEFAULT 'n/a' AFTER `qc_job_completed_at`
    """))
    conn.execute(text("""
        UPDATE `file_uploads`
        SET `genome_build` = CASE LOWER(metadata->>'$.referenceGenome')
            WHEN 'hg19'   THEN 'hg19'
            WHEN 'hg38'   THEN 'grch38'
            WHEN 'grch37' THEN 'hg19'
            WHEN 'grch38' THEN 'grch38'
            ELSE 'n/a'
        END
    """))
