"""add unique constraint to mskkp dataset name

Revision ID: add_unique_mskkp_dataset_name
Revises: create_mskkp_datasets
Create Date: 2026-01-06 18:34:00.000000

"""
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'add_unique_mskkp_dataset_name'
down_revision = 'create_mskkp_datasets'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    
    # Add unique constraint to name column
    conn.execute(text("""
        ALTER TABLE `mskkp_datasets`
        ADD UNIQUE KEY `mskkp_datasets_name_unique` (`name`)
    """))


def downgrade() -> None:
    conn = op.get_bind()
    
    # Remove unique constraint
    conn.execute(text("""
        ALTER TABLE `mskkp_datasets`
        DROP INDEX `mskkp_datasets_name_unique`
    """))
