"""add meta analysis timing fields

Revision ID: add_meta_analysis_timing
Revises: pqr012stu345  
Create Date: 2025-01-08 17:40:00.000000

"""
from alembic import op
from sqlalchemy import text


# revision identifiers, used by Alembic.
revision = 'add_meta_analysis_timing'
down_revision = 'pqr012stu345'  # This should be updated to the latest migration
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    
    # Add timing fields to meta_analyses table
    conn.execute(text("""
        ALTER TABLE meta_analyses 
        ADD COLUMN job_submitted_at datetime NULL,
        ADD COLUMN job_completed_at datetime NULL
    """))


def downgrade() -> None:
    conn = op.get_bind()
    
    # Remove timing fields from meta_analyses table
    conn.execute(text("""
        ALTER TABLE meta_analyses 
        DROP COLUMN job_submitted_at,
        DROP COLUMN job_completed_at
    """))
