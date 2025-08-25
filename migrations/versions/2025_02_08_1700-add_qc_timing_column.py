"""add_qc_timing_column

Revision ID: add_qc_timing_column
Revises: d2ebd3a31541
Create Date: 2025-02-08 17:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'add_qc_timing_column'
down_revision = 'd2ebd3a31541'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    query = """
        ALTER TABLE `file_uploads` 
        ADD COLUMN `qc_job_submitted_at` datetime NULL,
        ADD COLUMN `qc_job_completed_at` datetime NULL
        """
    conn.execute(text(query))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("""
        ALTER TABLE `file_uploads` 
        DROP COLUMN `qc_job_submitted_at`,
        DROP COLUMN `qc_job_completed_at`
    """))