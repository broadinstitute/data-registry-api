"""add step_index column to qc_step_result

Revision ID: add_qc_step_index
Revises: create_qc_run_tables
Create Date: 2026-07-13 10:00:00.000000

"""
from alembic import op
from sqlalchemy import text

revision = 'add_qc_step_index'
down_revision = 'create_qc_run_tables'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("ALTER TABLE qc_step_result ADD COLUMN step_index int NOT NULL DEFAULT 0"))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("ALTER TABLE qc_step_result DROP COLUMN step_index"))
