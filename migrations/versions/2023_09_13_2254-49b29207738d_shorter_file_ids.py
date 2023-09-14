"""shorter_file_ids

Revision ID: 49b29207738d
Revises: c92900eecce4
Create Date: 2023-09-13 22:54:54.371020

"""
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = '49b29207738d'
down_revision = 'c92900eecce4'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    query = """
    CREATE TABLE `data_file_ids` (
    `id` binary(32) NOT NULL,
    `short_id` char(6) NOT NULL,
    PRIMARY KEY (`short_id`),
    KEY `short_data_file_id_fk` (`id`),
    CONSTRAINT `short_data_file_id_fk` FOREIGN KEY (`id`) REFERENCES `dataset_phenotypes` (`id`) on delete cascade
    )
    """
    conn.execute(text(query))
    query = """
    CREATE TABLE `cs_file_ids` (
    `id` binary(32) NOT NULL,
    `short_id` char(6) NOT NULL,
    PRIMARY KEY (`short_id`),
    KEY `short_cs_file_id_fk` (`id`),
    CONSTRAINT `short_cs_file_id_fk` FOREIGN KEY (`id`) REFERENCES `credible_sets` (`id`) on delete cascade
    )
    """
    conn.execute(text(query))


def downgrade() -> None:
    conn = op.get_bind()
    query = """
    DROP TABLE data_file_ids;
    """
    conn.execute(text(query))
    query = """
    DROP TABLE cs_file_ids;
    """
    conn.execute(text(query))

