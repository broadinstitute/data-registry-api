"""add validation_status column to sgc_cohorts

Revision ID: def123ghi456
Revises: abc456def789
Create Date: 2025-09-26 18:08:00.000000

"""
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'def123ghi456'
down_revision = 'abc456def789'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    
    # Add validation_status column to sgc_cohorts table
    query = """
        ALTER TABLE `sgc_cohorts` 
        ADD COLUMN `validation_status` boolean DEFAULT FALSE NOT NULL COMMENT 'whether the cohort has passed all validations'
        """
    conn.execute(text(query))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("ALTER TABLE `sgc_cohorts` DROP COLUMN `validation_status`"))
