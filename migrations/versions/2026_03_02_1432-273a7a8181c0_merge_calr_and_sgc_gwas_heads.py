"""merge calr and sgc gwas heads

Revision ID: 273a7a8181c0
Revises: create_sgc_gwas_cohorts, add_calr_public_and_format
Create Date: 2026-03-02 14:32:30.591724

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '273a7a8181c0'
down_revision = ('create_sgc_gwas_cohorts', 'add_calr_public_and_format')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
