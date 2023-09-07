"""increase_credible_set_file_sizes

Revision ID: c92900eecce4
Revises: 60c1f4a1a4d4
Create Date: 2023-09-06 20:41:23.915576

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'c92900eecce4'
down_revision = '60c1f4a1a4d4'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    query = """
        ALTER TABLE `credible_sets` change file_size file_size bigint not null default 0  
    """
    conn.execute(text(query))


def downgrade() -> None:
    conn = op.get_bind()
    query = """
        ALTER TABLE `credible_sets` change file_size file_size int not null default 0  
    """
    conn.execute(text(query))
