"""add cohort_metadata column to sgc_cohorts

Revision ID: pqr012stu345
Revises: mno789pqr012
Create Date: 2025-02-08 19:00:00.000000

"""
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'pqr012stu345'
down_revision = 'mno789pqr012'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    
    # Add cohort_metadata JSON column to sgc_cohorts table
    query = """
        ALTER TABLE `sgc_cohorts` 
        ADD COLUMN `cohort_metadata` json NULL COMMENT 'additional metadata as JSON object'
        """
    conn.execute(text(query))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("ALTER TABLE `sgc_cohorts` DROP COLUMN `cohort_metadata`"))