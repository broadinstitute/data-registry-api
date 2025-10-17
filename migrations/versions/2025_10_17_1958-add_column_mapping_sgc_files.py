"""add column_mapping to sgc_cohort_files

Revision ID: add_column_mapping_sgc_files
Revises: 278efbee1b61
Create Date: 2025-10-17 19:58:00.000000

"""
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'add_column_mapping_sgc_files'
down_revision = '278efbee1b61'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    
    # Add column_mapping field to sgc_cohort_files table
    query = """
        ALTER TABLE `sgc_cohort_files` 
        ADD COLUMN `column_mapping` JSON NULL COMMENT 'JSON mapping of file columns to standard names'
    """
    conn.execute(text(query))


def downgrade() -> None:
    conn = op.get_bind()
    
    # Remove column_mapping field from sgc_cohort_files table
    query = """
        ALTER TABLE `sgc_cohort_files` 
        DROP COLUMN `column_mapping`
    """
    conn.execute(text(query))