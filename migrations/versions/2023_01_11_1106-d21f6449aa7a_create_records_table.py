"""create records table

Revision ID: d21f6449aa7a
Revises: 
Create Date: 2023-01-11 11:06:25.633652

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'd21f6449aa7a'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    query = """
    CREATE TABLE records(
        id INT PRIMARY KEY AUTO_INCREMENT,
        s3_bucket_id VARCHAR(45) NOT NULL,
        name VARCHAR(45) NOT NULL,
        description VARCHAR(45),
        created_at DATETIME DEFAULT NOW(),
        deleted_at_unix_time INT(10) DEFAULT 0,
        CONSTRAINT name_deleted_unique UNIQUE (name, deleted_at_unix_time)
    );
    """
    conn.execute(text(query))


def downgrade() -> None:
    conn = op.get_bind()
    query = """
    DROP TABLE records;
    """
    conn.execute(text(query))
