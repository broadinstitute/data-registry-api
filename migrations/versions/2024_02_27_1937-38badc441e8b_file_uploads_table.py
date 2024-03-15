"""file_uploads_table

Revision ID: 38badc441e8b
Revises: 31d60ce1d715
Create Date: 2024-02-27 19:37:35.016695

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = '38badc441e8b'
down_revision = '31d60ce1d715'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    query = """
        CREATE TABLE `file_uploads` (
        `id` binary(32) NOT NULL,
        `dataset` varchar(100) NOT NULL,
        `file_name` varchar(100) NOT NULL,
        `file_size` int NOT NULL,
        `uploaded_at` datetime NOT NULL,
        `uploaded_by` varchar(100) NOT NULL,
        `metadata` json not null,
        `s3_path` varchar(200) NOT NULL,
        `qc_status` varchar(100) NOT NULL,
        `qc_log` text NULL,
        PRIMARY KEY (`id`)
        )
        """
    conn.execute(text(query))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("DROP TABLE `file_uploads`"))
