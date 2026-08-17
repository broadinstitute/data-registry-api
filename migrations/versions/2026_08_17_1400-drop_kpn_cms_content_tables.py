"""drop kpn cms content tables (cms_content_item, cms_request_miss)

The general kp4cd.org CMS replacement was retired -- the registry now owns
only dataset info (kp_datasets). cms_asset remains: it tracks the mirrored
assets that migrated dataset bodies reference via /api/kpn/files/.

Revision ID: drop_kpn_cms_content_tables
Revises: create_kp_datasets
Create Date: 2026-08-17 14:00:00.000000

"""
from alembic import op
from sqlalchemy import text

revision = 'drop_kpn_cms_content_tables'
down_revision = 'create_kp_datasets'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("DROP TABLE cms_request_miss"))
    conn.execute(text("DROP TABLE cms_content_item"))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("""
        CREATE TABLE `cms_content_item` (
            `id` int NOT NULL AUTO_INCREMENT,
            `view_name` varchar(128) NOT NULL,
            `portal` varchar(64) NULL,
            `nid` varchar(32) NULL,
            `item_key` varchar(255) NULL,
            `payload` mediumtext NOT NULL,
            `search_text` mediumtext NULL,
            `sort_order` int NOT NULL DEFAULT 0,
            `imported_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (`id`),
            KEY `cms_content_view_portal_idx` (`view_name`, `portal`),
            KEY `cms_content_nid_idx` (`nid`),
            KEY `cms_content_view_key_idx` (`view_name`, `item_key`),
            FULLTEXT KEY `cms_content_search_ft` (`search_text`)
        )
    """))
    conn.execute(text("""
        CREATE TABLE `cms_request_miss` (
            `id` int NOT NULL AUTO_INCREMENT,
            `view_name` varchar(128) NOT NULL,
            `query_string` varchar(600) NOT NULL,
            `proxied` tinyint(1) NOT NULL DEFAULT 0,
            `response_status` int NULL,
            `hit_count` int NOT NULL DEFAULT 1,
            `first_seen` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
            `last_seen` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (`id`),
            UNIQUE KEY `cms_miss_view_query_uq` (`view_name`, `query_string`)
        )
    """))
