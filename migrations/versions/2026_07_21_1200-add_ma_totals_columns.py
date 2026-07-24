"""add total_cases/total_controls to sgc_gwas_ma_results

Revision ID: add_ma_totals_columns
Revises: add_qc_step_index
Create Date: 2026-07-21 12:00:00.000000

"""
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'add_ma_totals_columns'
down_revision = 'add_qc_step_index'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text(
        "ALTER TABLE sgc_gwas_ma_results "
        "ADD COLUMN total_cases bigint NULL AFTER n_cohorts_used, "
        "ADD COLUMN total_controls bigint NULL AFTER total_cases"
    ))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text(
        "ALTER TABLE sgc_gwas_ma_results "
        "DROP COLUMN total_controls, DROP COLUMN total_cases"
    ))
