"""add accession_number to peg_studies

Revision ID: add_peg_accession
Revises: 2026_01_07_1200
Create Date: 2026-01-15 15:42:00.000000

"""
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'add_peg_accession'
down_revision = '2026_01_07_1200'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    
    # Add accession_number column with AUTO_INCREMENT
    conn.execute(text("""
        ALTER TABLE `peg_studies`
        ADD COLUMN `accession_number` int NOT NULL AUTO_INCREMENT UNIQUE,
        ADD INDEX `idx_accession_number` (`accession_number`)
    """))


def downgrade() -> None:
    conn = op.get_bind()
    
    # Remove accession_number column
    conn.execute(text("""
        ALTER TABLE `peg_studies`
        DROP COLUMN `accession_number`
    """))
