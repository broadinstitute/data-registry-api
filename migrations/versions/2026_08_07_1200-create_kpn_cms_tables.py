"""create kpn cms tables (content items, assets, request misses)

Revision ID: create_kpn_cms_tables
Revises: create_hcm_liftover_jobs
Create Date: 2026-08-07 12:00:00.000000

"""
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'create_kpn_cms_tables'
down_revision = 'create_hcm_liftover_jobs'
branch_labels = None
depends_on = None


def upgrade() -> None:
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
        CREATE TABLE `cms_asset` (
            `id` int NOT NULL AUTO_INCREMENT,
            `remote_url` varchar(768) NOT NULL,
            `s3_key` varchar(1024) NULL,
            `content_type` varchar(255) NULL,
            `size` bigint NULL,
            `status` varchar(16) NOT NULL,
            `imported_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (`id`),
            UNIQUE KEY `cms_asset_remote_url_uq` (`remote_url`)
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


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("DROP TABLE `cms_request_miss`"))
    conn.execute(text("DROP TABLE `cms_asset`"))
    conn.execute(text("DROP TABLE `cms_content_item`"))
