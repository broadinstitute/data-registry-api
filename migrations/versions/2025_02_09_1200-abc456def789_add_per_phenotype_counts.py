"""add_per_phenotype_counts

Revision ID: abc456def789
Revises: pqr012stu345
Create Date: 2025-02-09 12:00:00.000000

"""
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'abc456def789'
down_revision = 'add_meta_analysis_timing'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # Add phenotype_counts column to sgc_cases_controls_metadata table
    query1 = """
        ALTER TABLE `sgc_cases_controls_metadata`
        ADD COLUMN `phenotype_counts` json DEFAULT NULL COMMENT 'per-phenotype case and control counts as JSON object'
        """
    conn.execute(text(query1))

    # Add phenotype_pair_counts column to sgc_cooccurrence_metadata table
    query2 = """
        ALTER TABLE `sgc_cooccurrence_metadata`
        ADD COLUMN `phenotype_pair_counts` json DEFAULT NULL COMMENT 'per-phenotype-pair co-occurrence counts as JSON object'
        """
    conn.execute(text(query2))


def downgrade() -> None:
    conn = op.get_bind()

    # Remove the added columns
    conn.execute(text("ALTER TABLE `sgc_cases_controls_metadata` DROP COLUMN `phenotype_counts`"))
    conn.execute(text("ALTER TABLE `sgc_cooccurrence_metadata` DROP COLUMN `phenotype_pair_counts`"))