"""create sgc_gwas_munge_results table

Revision ID: create_sgc_gwas_munge_results
Revises: drop_kpn_cms_content_tables
Create Date: 2026-10-20 12:00:00.000000

"""
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'create_sgc_gwas_munge_results'
down_revision = 'drop_kpn_cms_content_tables'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("""
        CREATE TABLE `sgc_gwas_munge_results` (
            `id` binary(32) NOT NULL,
            `file_id` binary(32) NOT NULL,
            `status` varchar(20) NOT NULL COMMENT 'PENDING, RUNNING, SUCCEEDED, FAILED',
            `batch_job_id` varchar(64) NULL,
            `n_in` int NULL,
            `n_out` int NULL,
            `n_with_rsid` int NULL,
            `n_swapped` int NULL,
            `n_dropped` json NULL,
            `has_eaf` tinyint(1) NULL,
            `has_info` tinyint(1) NULL,
            `maf_min` double NULL,
            `info_min` double NULL,
            `ref_version` varchar(64) NULL,
            `engine` varchar(64) NULL,
            `standardised_s3_key` varchar(512) NULL,
            `munge_json_s3_key` varchar(512) NULL,
            `ref_mismatch_s3_key` varchar(512) NULL,
            `error_message` text NULL,
            `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
            `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (`id`),
            UNIQUE KEY `sgc_gwas_munge_results_file_id_uniq` (`file_id`),
            KEY `sgc_gwas_munge_results_status_idx` (`status`),
            CONSTRAINT `sgc_gwas_munge_results_file_fk` FOREIGN KEY (`file_id`)
                REFERENCES `sgc_gwas_files` (`id`) ON DELETE CASCADE
        )
    """))


def downgrade() -> None:
    op.get_bind().execute(text("DROP TABLE `sgc_gwas_munge_results`"))
