"""merge_calr_and_sgc_gwas_heads

Revision ID: 3ee8662aa028
Revises: sgc_gwas_cases_ctrl, create_calr_files
Create Date: 2026-02-02 19:45:59.909407

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '3ee8662aa028'
down_revision = ('sgc_gwas_cases_ctrl', 'create_calr_files')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
