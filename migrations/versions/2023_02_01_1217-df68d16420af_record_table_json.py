"""foo

Revision ID: df68d16420af
Revises: 7843e8bdb14c
Create Date: 2023-02-01 12:17:13.325236

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import Column, JSON, String

# revision identifiers, used by Alembic.
revision = 'df68d16420af'
down_revision = '7843e8bdb14c'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_column('records', 'description')
    op.add_column('records', Column('metadata', JSON))


def downgrade() -> None:
    op.drop_column('records', 'metadata')
    op.add_column('records', Column('description', String(45)))
