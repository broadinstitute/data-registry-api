"""add_phenotypes_column_record

Revision ID: 760d34648997
Revises: d1031a6d0f48
Create Date: 2023-02-15 11:48:57.160822

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = '760d34648997'
down_revision = 'd1031a6d0f48'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("ALTER TABLE records add column phenotypes json"))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("ALTER TABLE records drop column phenotypes"))
