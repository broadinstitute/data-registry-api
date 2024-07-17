"""meta_analsyses

Revision ID: 9d8813a4558d
Revises: 603e009de736
Create Date: 2024-07-17 13:47:02.656723

"""
from alembic import op
from sqlalchemy import text


# revision identifiers, used by Alembic.
revision = '9d8813a4558d'
down_revision = '603e009de736'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    query = """
        CREATE TABLE `meta_analyses` (
        `id` binary(32) NOT NULL,
        `name` varchar(500) NOT NULL,
        `phenotype` varchar(100) NOT NULL,
        `status` varchar(100) NOT NULL,
        `created_at` datetime NOT NULL,
        `method` varchar(100) NOT NULL,
        `created_by` varchar(100) NOT NULL,
        PRIMARY KEY (`id`)
        )
        """
    conn.execute(text(query))
    query = """
        CREATE TABLE `meta_analysis_datasets` (
        `dataset_id` binary(32) NOT NULL, 
        `meta_analysis_id` binary(32) NOT NULL,
        primary key (`dataset_id`, `meta_analysis_id`),
        constraint `ma_id` foreign key (`meta_analysis_id`) references `meta_analyses`(`id`),
        constraint `ds_id` foreign key (`dataset_id`) references `file_uploads`(`id`)
        )"""
    conn.execute(text(query))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("DROP TABLE meta_analysis_datasets"))
    conn.execute(text("DROP TABLE meta_analyses"))
