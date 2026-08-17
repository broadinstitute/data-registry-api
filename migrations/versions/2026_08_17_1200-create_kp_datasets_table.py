"""create kp_datasets table (portal-facing dataset info, system of record)

Revision ID: create_kp_datasets
Revises: create_kpn_cms_tables
Create Date: 2026-08-17 12:00:00.000000

"""
from alembic import op
from sqlalchemy import text

revision = 'create_kp_datasets'
down_revision = 'create_kpn_cms_tables'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("""
        CREATE TABLE `kp_datasets` (
            `id` int NOT NULL AUTO_INCREMENT,
            `dataset_id` varchar(255) NULL,
            `title` varchar(500) NOT NULL,
            `body` mediumtext NOT NULL,
            `portals` varchar(500) NOT NULL,
            `published` tinyint(1) NOT NULL,
            `registry_dataset_id` binary(32) NULL,
            `drupal_nid` int NULL,
            `drupal_author` varchar(255) NULL,
            `migration_note` varchar(255) NULL,
            `created_at` datetime NOT NULL,
            `updated_at` datetime NOT NULL,
            PRIMARY KEY (`id`),
            UNIQUE KEY `kp_datasets_dataset_id_uq` (`dataset_id`),
            UNIQUE KEY `kp_datasets_drupal_nid_uq` (`drupal_nid`),
            KEY `kp_datasets_registry_dataset_idx` (`registry_dataset_id`)
        )
    """))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("DROP TABLE `kp_datasets`"))
