"""create datasets table

Revision ID: 7843e8bdb14c
Revises: d21f6449aa7a
Create Date: 2023-01-11 15:59:16.963997

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = '7843e8bdb14c'
down_revision = 'd21f6449aa7a'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    query = """
    CREATE TABLE datasets(
        id INT PRIMARY KEY AUTO_INCREMENT,
        record_id INT NOT NULL,
        s3_bucket_id VARCHAR(45) NOT NULL,
        name VARCHAR(45) NOT NULL,
        description VARCHAR(45),
        data_type VARCHAR(45) NOT NULL,
        created_at DATETIME DEFAULT NOW(),
        deleted_at_unix_time INT(10) DEFAULT 0,
        CONSTRAINT record_id_s3_name_deleted_unique UNIQUE (record_id, name, deleted_at_unix_time),
        FOREIGN KEY (record_id) REFERENCES records(id) ON DELETE CASCADE
    );
    """
    conn.execute(text(query))


def downgrade() -> None:
    conn = op.get_bind()
    query = """
    DROP TABLE datasets;
    """
    conn.execute(text(query))
