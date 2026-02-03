"""add cases and controls columns to sgc_gwas_files

Revision ID: sgc_gwas_cases_ctrl
Revises: create_sgc_gwas_files
Create Date: 2026-02-02 23:00:00.000000

"""
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'sgc_gwas_cases_ctrl'
down_revision = 'create_sgc_gwas_files'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    
    # Add cases and controls columns to sgc_gwas_files table
    query = """
        ALTER TABLE `sgc_gwas_files`
        ADD COLUMN `cases` int NULL COMMENT 'number of cases (optional)',
        ADD COLUMN `controls` int NULL COMMENT 'number of controls (optional)'
    """
    conn.execute(text(query))


def downgrade() -> None:
    conn = op.get_bind()
    query = """
        ALTER TABLE `sgc_gwas_files`
        DROP COLUMN `cases`,
        DROP COLUMN `controls`
    """
    conn.execute(text(query))
