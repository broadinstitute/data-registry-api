"""user-auth

Revision ID: e6a081f21335
Revises: e6e408300151
Create Date: 2024-01-16 14:01:53.403304

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'e6a081f21335'
down_revision = 'e6e408300151'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    query = "ALTER TABLE datasets ADD COLUMN user_id INT"
    conn.execute(text(query))
    query = """
        ALTER TABLE datasets
        ADD CONSTRAINT fk_datasets_user_id
        FOREIGN KEY (user_id)
        REFERENCES users(id);
    """
    conn.execute(text(query))


def downgrade() -> None:
    conn = op.get_bind()
    query = "ALTER TABLE datasets DROP CONSTRAINT fk_datasets_user_id"
    conn.execute(text(query))
    query = "ALTER TABLE datasets DROP COLUMN user_id"
    conn.execute(text(query))
