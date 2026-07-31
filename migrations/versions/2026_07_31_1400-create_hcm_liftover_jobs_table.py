"""create hcm_liftover_jobs table + seed hcm portal_liftover_config

Revision ID: create_hcm_liftover_jobs
Revises: create_hcm_gwas_ma_results
Create Date: 2026-07-31 14:00:00.000000

"""
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'create_hcm_liftover_jobs'
down_revision = 'create_hcm_gwas_ma_results'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("""
        CREATE TABLE `hcm_liftover_jobs` (
            `id` binary(32) NOT NULL,
            `file_id` binary(32) NOT NULL,
            `source_genome_build` varchar(16) NOT NULL,
            `target_genome_build` varchar(16) NOT NULL,
            `batch_job_id` varchar(128) NULL,
            `status` varchar(64) NOT NULL,
            `submitted_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
            `completed_at` datetime NULL,
            `submitted_by` varchar(255) NOT NULL,
            `original_s3_path` varchar(1024) NULL,
            `unmapped_s3_path` varchar(1024) NULL,
            `summary` json NULL,
            `log` mediumtext NULL,
            PRIMARY KEY (`id`),
            KEY `hcm_liftover_jobs_file_id_idx` (`file_id`),
            KEY `hcm_liftover_jobs_status_idx` (`status`),
            CONSTRAINT `hcm_liftover_jobs_file_fk` FOREIGN KEY (`file_id`)
                REFERENCES `hcm_gwas_files` (`id`) ON DELETE CASCADE
        )
    """))
    conn.execute(text("""
        INSERT IGNORE INTO `portal_liftover_config`
            (`portal_id`, `target_genome_build`, `updated_at`, `updated_by`)
        VALUES ('hcm', 'hg38', NOW(), 'system')
    """))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("DELETE FROM `portal_liftover_config` WHERE `portal_id` = 'hcm'"))
    conn.execute(text("DROP TABLE `hcm_liftover_jobs`"))
