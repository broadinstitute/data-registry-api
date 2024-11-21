"""add qc script options for hermes

Revision ID: 3040fbdb1d82
Revises: 2b16be652f47
Create Date: 2024-11-20 13:17:54.916692

"""
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = '3040fbdb1d82'
down_revision = '2b16be652f47'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("ALTER TABLE file_uploads ADD COLUMN qc_script_options JSON NULL"))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("ALTER TABLE file_uploads DROP COLUMN qc_script_options"))
