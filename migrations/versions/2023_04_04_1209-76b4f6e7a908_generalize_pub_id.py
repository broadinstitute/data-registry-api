"""generalize_pub_id

Revision ID: 76b4f6e7a908
Revises: 21bd9bbea08b
Create Date: 2023-04-04 12:09:09.231823

"""
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = '76b4f6e7a908'
down_revision = '21bd9bbea08b'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    query = """
        ALTER TABLE `datasets` change pmid pub_id varchar(30)  
    """
    conn.execute(text(query))


def downgrade() -> None:
    conn = op.get_bind()
    query = """
        ALTER TABLE `datasets` change pub_id pmid bigint  
    """
    conn.execute(text(query))
