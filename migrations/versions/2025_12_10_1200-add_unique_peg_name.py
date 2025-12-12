"""add unique constraint to peg_studies name

Revision ID: add_unique_peg_name
Revises: abc123peg001
Create Date: 2025-12-10 12:00:00.000000

"""
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'add_unique_peg_name'
down_revision = 'abc123peg001'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # Add unique constraint to name column
    conn.execute(text("""
        ALTER TABLE `peg_studies`
        ADD UNIQUE INDEX `idx_unique_name` (`name`)
    """))


def downgrade() -> None:
    conn = op.get_bind()

    # Remove unique constraint
    conn.execute(text("""
        ALTER TABLE `peg_studies`
        DROP INDEX `idx_unique_name`
    """))
