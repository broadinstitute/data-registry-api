"""add lambda_1000 to sgc_gwas_plot_results

Revision ID: add_lambda_1000_plot_results
Revises: create_sgc_gwas_plot_results
Create Date: 2026-05-27 12:00:00.000000

"""
from alembic import op
from sqlalchemy import text

revision = 'add_lambda_1000_plot_results'
down_revision = 'create_sgc_gwas_plot_results'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("""
        ALTER TABLE `sgc_gwas_plot_results`
        ADD COLUMN `lambda_1000` double NULL
            COMMENT 'sample-size-adjusted lambda: lambda at 1000 cases vs 1000 controls'
            AFTER `lambda_gc`
    """))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("ALTER TABLE `sgc_gwas_plot_results` DROP COLUMN `lambda_1000`"))
