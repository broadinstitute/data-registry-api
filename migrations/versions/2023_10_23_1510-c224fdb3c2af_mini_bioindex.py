"""mini-bioindex

Revision ID: c224fdb3c2af
Revises: 49b29207738d
Create Date: 2023-10-23 15:10:26.242353

"""
from alembic import op
import sqlalchemy as sa
from bioindex.lib.migrate import create_indexes_table, index_migration_1, create_keys_table

# revision identifiers, used by Alembic.
revision = 'c224fdb3c2af'
down_revision = '49b29207738d'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    engine = conn.engine
    create_indexes_table(engine)
    index_migration_1(engine)
    create_keys_table(engine)


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(sa.text('DROP TABLE __Indexes'))
    conn.execute(sa.text('DROP TABLE __Keys'))
