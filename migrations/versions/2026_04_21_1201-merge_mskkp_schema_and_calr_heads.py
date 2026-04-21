"""merge mskkp schema and calr heads

Revision ID: merge_mskkp_calr_heads
Revises: update_mskkp_datasets_schema, add_calr_submission_metadata
Create Date: 2026-04-21 12:01:00.000000

"""
from alembic import op

revision = 'merge_mskkp_calr_heads'
down_revision = ('update_mskkp_datasets_schema', 'add_calr_submission_metadata')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
