"""file_sizes

Revision ID: ad740a30c452
Revises: 08ff322a9f4a
Create Date: 2023-07-06 16:38:38.110582

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'ad740a30c452'
down_revision = '08ff322a9f4a'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    query = """
    ALTER TABLE `dataset_phenotypes` add column file_size int not null default 0 
    """
    conn.execute(text(query))
    query = """
    ALTER TABLE `credible_sets` add column file_size int not null default 0
    """
    conn.execute(text(query))


def downgrade() -> None:
    conn = op.get_bind()
    query = """
    ALTER TABLE `dataset_phenotypes` drop column file_size
    """
    conn.execute(text(query))
    query = """
    ALTER TABLE `credible_sets` drop column file_size
    """
    conn.execute(text(query))
