"""add hermes phenotype list

Revision ID: 09c1d30b0efe
Revises: 3040fbdb1d82
Create Date: 2025-02-03 11:55:45.940555

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = '09c1d30b0efe'
down_revision = '3040fbdb1d82'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    query = """
        CREATE TABLE `hermes_phenotype` (
        `name` varchar(100) NOT NULL,
        `description` varchar(500) NOT NULL,
        `dichotomous` tinyint NOT NULL,
        PRIMARY KEY (`name`)
        )
        """
    conn.execute(text(query))



def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("DROP TABLE hermes_phenotype"))
