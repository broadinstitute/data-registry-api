"""increase_file_sizes

Revision ID: 60c1f4a1a4d4
Revises: ad740a30c452
Create Date: 2023-09-06 08:45:17.035145

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = '60c1f4a1a4d4'
down_revision = 'ad740a30c452'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    query = """
        ALTER TABLE `dataset_phenotypes` change file_size file_size bigint not null default 0  
    """
    conn.execute(text(query))


def downgrade() -> None:
    conn = op.get_bind()
    query = """
        ALTER TABLE `dataset_phenotypes` change file_size file_size int not null default 0  
    """
    conn.execute(text(query))
