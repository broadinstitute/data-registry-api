"""add lambda_maf01 to sgc_gwas_plot_results

Revision ID: add_lambda_maf01_plot_results
Revises: add_lambda_1000_plot_results
Create Date: 2026-06-03 12:00:00.000000

"""
from alembic import op
from sqlalchemy import text

revision = 'add_lambda_maf01_plot_results'
down_revision = 'add_lambda_1000_plot_results'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("""
        ALTER TABLE `sgc_gwas_plot_results`
        ADD COLUMN `lambda_maf01` double NULL
            COMMENT 'lambda_gc over common variants (effect AF in [0.01, 0.99])'
            AFTER `lambda_1000`
    """))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("ALTER TABLE `sgc_gwas_plot_results` DROP COLUMN `lambda_maf01`"))
