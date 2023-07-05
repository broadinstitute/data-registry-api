"""share_files

Revision ID: 08ff322a9f4a
Revises: 6ca0cd68f992
Create Date: 2023-07-05 10:11:07.371170

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = '08ff322a9f4a'
down_revision = '6ca0cd68f992'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    query = """
    ALTER TABLE `datasets` add column publicly_available boolean not null default false
    """
    conn.execute(text(query))


def downgrade() -> None:
    conn = op.get_bind()
    query = """
    ALTER TABLE `datasets` drop column publicly_available
    """
    conn.execute(text(query))
