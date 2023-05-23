"""credible_sets

Revision ID: 843f0b914eaa
Revises: 76b4f6e7a908
Create Date: 2023-05-18 13:19:26.177843

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = '843f0b914eaa'
down_revision = '76b4f6e7a908'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    query = """
    CREATE TABLE `credible_sets` (
    `id` binary(32) NOT NULL,
    `phenotype_data_set_id` binary(32) NOT NULL,
    `name` varchar(30) NOT NULL,
    `s3_path` text NOT NULL,
    `created_at` datetime NOT NULL,
    PRIMARY KEY (`id`),
    KEY `phenotype_data_set_id_fk` (`phenotype_data_set_id`),
    CONSTRAINT `phenotype_data_set_id_fk` FOREIGN KEY (`phenotype_data_set_id`) REFERENCES `dataset_phenotypes` (`id`)
    )
    """
    conn.execute(text(query))


def downgrade() -> None:
    conn = op.get_bind()
    query = """
    DROP TABLE credible_sets
    """
    conn.execute(text(query))
