"""add metadata column to calr_submissions

Revision ID: add_calr_submission_metadata
Revises: create_hcm_gwas_validation_jobs
Create Date: 2026-03-27 12:00:00.000000

"""
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'add_calr_submission_metadata'
down_revision = 'create_hcm_gwas_validation_jobs'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("ALTER TABLE calr_submissions ADD COLUMN metadata JSON NULL"))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("ALTER TABLE calr_submissions DROP COLUMN metadata"))
