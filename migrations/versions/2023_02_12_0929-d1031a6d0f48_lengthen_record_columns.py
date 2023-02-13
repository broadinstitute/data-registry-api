"""lengthen_record_columns

Revision ID: d1031a6d0f48
Revises: bf402cdb0fa8
Create Date: 2023-02-12 09:29:51.138656

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'd1031a6d0f48'
down_revision = 'bf402cdb0fa8'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("ALTER TABLE records modify column name varchar(100)"))
    conn.execute(text("ALTER TABLE records modify column s3_bucket_id varchar(100)"))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("ALTER TABLE records modify column name varchar(45)"))
    conn.execute(text("ALTER TABLE records modify column s3_bucket_id varchar(45)"))

