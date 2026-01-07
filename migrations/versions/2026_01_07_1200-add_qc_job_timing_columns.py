"""add qc job timing columns to file_uploads

Revision ID: add_qc_job_timing_columns
Revises: add_unique_mskkp_dataset_name
Create Date: 2026-01-07 12:00:00.000000

"""
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'add_qc_job_timing_columns'
down_revision = 'add_unique_mskkp_dataset_name'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # Add qc_job_submitted_at and qc_job_completed_at columns to file_uploads table
    # These columns track when QC batch jobs are submitted and completed for monitoring
    conn.execute(text("""
        ALTER TABLE `file_uploads`
        ADD COLUMN `qc_job_submitted_at` DATETIME NULL AFTER `qc_status`,
        ADD COLUMN `qc_job_completed_at` DATETIME NULL AFTER `qc_job_submitted_at`
    """))


def downgrade() -> None:
    conn = op.get_bind()

    # Remove the timing columns
    conn.execute(text("""
        ALTER TABLE `file_uploads`
        DROP COLUMN `qc_job_submitted_at`,
        DROP COLUMN `qc_job_completed_at`
    """))
