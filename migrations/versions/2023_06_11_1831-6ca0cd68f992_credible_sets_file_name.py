"""credible_sets_file_name

Revision ID: 6ca0cd68f992
Revises: 843f0b914eaa
Create Date: 2023-06-11 18:31:20.206898

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = '6ca0cd68f992'
down_revision = '843f0b914eaa'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    query = """
    ALTER TABLE `credible_sets` add column file_name varchar(100)
    """
    conn.execute(text(query))


def downgrade() -> None:
    conn = op.get_bind()
    query = """
    ALTER TABLE `credible_sets` drop column file_name
    """
    conn.execute(text(query))
