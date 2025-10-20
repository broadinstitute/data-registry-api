"""fix_sgc_phenotypes_remove_k_suffix

Revision ID: 278efbee1b61
Revises: def123ghi456
Create Date: 2025-10-16 15:07:29.668049

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '278efbee1b61'
down_revision = 'def123ghi456'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    
    # Remove the erroneous 'K' suffix from all phenotype_code values
    # This fixes the data corruption from the previous migration
    update_query = """
        UPDATE sgc_phenotypes 
        SET phenotype_code = LEFT(phenotype_code, LENGTH(phenotype_code) - 1)
        WHERE phenotype_code LIKE '%K' 
        AND LENGTH(phenotype_code) > 1
    """
    
    from sqlalchemy import text
    conn.execute(text(update_query))


def downgrade() -> None:
    conn = op.get_bind()
    
    # Restore the 'K' suffix (reverse the fix)
    # This should only be used if we need to rollback for some reason
    restore_query = """
        UPDATE sgc_phenotypes 
        SET phenotype_code = CONCAT(phenotype_code, 'K')
    """
    
    from sqlalchemy import text
    conn.execute(text(restore_query))
