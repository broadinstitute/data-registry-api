"""change datasets description to phenotype

Revision ID: aaa800cb0c9c
Revises: d1031a6d0f48
Create Date: 2023-02-16 19:25:16.145003

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'aaa800cb0c9c'
down_revision = 'd1031a6d0f48'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("ALTER TABLE datasets rename column description to phenotype"))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("ALTER TABLE datasets rename column phenotype to description"))
