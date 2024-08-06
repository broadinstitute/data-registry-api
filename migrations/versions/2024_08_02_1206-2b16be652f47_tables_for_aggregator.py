"""tables for aggregator

Revision ID: 2b16be652f47
Revises: 9d8813a4558d
Create Date: 2024-08-02 12:06:52.276108

"""
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = '2b16be652f47'
down_revision = '9d8813a4558d'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    query = """
        CREATE TABLE `Datasets` (
        `name` varchar(500) NOT NULL,
        `ancestry` varchar(10) DEFAULT NULL,
        PRIMARY KEY (`name`)
        )
        """
    conn.execute(text(query))
    query = """
        CREATE TABLE `Phenotypes` (
        `name` varchar(500) NOT NULL, 
        `dichotomous` tinyint NOT NULL,
        PRIMARY KEY (`name`)
        )"""
    conn.execute(text(query))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("DROP TABLE Datasets"))
    conn.execute(text("DROP TABLE Phenotypes"))
