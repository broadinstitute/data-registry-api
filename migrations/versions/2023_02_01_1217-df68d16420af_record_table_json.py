"""add json column, remove description

Revision ID: df68d16420af
Revises: 7843e8bdb14c
Create Date: 2023-02-01 12:17:13.325236

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = 'df68d16420af'
down_revision = '7843e8bdb14c'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute("ALTER TABLE records drop column description")
    conn.execute("ALTER TABLE records add column metadata json")


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute("ALTER TABLE records add column description varchar(45)")
    conn.execute("ALTER TABLE records drop column metadata")
