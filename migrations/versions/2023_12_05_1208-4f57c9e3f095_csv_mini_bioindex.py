"""csv-mini-bioindex

Revision ID: 4f57c9e3f095
Revises: c224fdb3c2af
Create Date: 2023-12-05 12:08:43.793864

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = '4f57c9e3f095'
down_revision = 'c224fdb3c2af'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    query = """
        CREATE TABLE `bidx_tracking` (
        `name` binary(32) NOT NULL,
        `status` varchar(50) NOT NULL,
        `column` varchar(200) NOT NULL,
        `s3_path` varchar(500) NOT NULL,
        `already_sorted` boolean NOT NULL,
        `ip_address` varchar(50) NULL,
        `created_at` datetime NOT NULL,
        PRIMARY KEY (`name`)
        )
        """
    conn.execute(text(query))


def downgrade() -> None:
    conn = op.get_bind()
    query = """
        DROP TABLE `bidx_tracking` 
        """
    conn.execute(text(query))
