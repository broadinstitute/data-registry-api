"""add shared flag to calr_submissions

Revision ID: add_calr_shared_flag
Revises: add_ldsc_columns_plot_results
Create Date: 2026-06-25 12:00:00.000000

"""
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'add_calr_shared_flag'
down_revision = 'add_ldsc_columns_plot_results'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text(
        "ALTER TABLE calr_submissions "
        "ADD COLUMN shared TINYINT(1) NOT NULL DEFAULT 0 AFTER public"
    ))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("ALTER TABLE calr_submissions DROP COLUMN shared"))
