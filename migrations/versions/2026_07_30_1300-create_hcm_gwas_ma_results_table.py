"""create hcm_gwas_ma_results table

Revision ID: create_hcm_gwas_ma_results
Revises: sgc_ma_ignore_file_based
Create Date: 2026-07-30 13:00:00.000000
"""
from alembic import op
from sqlalchemy import text

revision = 'create_hcm_gwas_ma_results'
down_revision = 'sgc_ma_ignore_file_based'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.get_bind().execute(text("""
        CREATE TABLE `hcm_gwas_ma_results` (
            `id` binary(32) NOT NULL,
            `label` varchar(255) NULL,
            `status` varchar(20) NOT NULL COMMENT 'PENDING, RUNNING, SUCCEEDED, FAILED',
            `dataset_file_ids` json NULL,
            `maf_min` double NULL,
            `info_min` double NULL,
            `meta_lambda_gc` double NULL,
            `n_meta_variants` bigint NULL,
            `n_genome_wide_sig` int NULL,
            `n_cohorts` int NULL,
            `n_cohorts_used` int NULL,
            `total_cases` int NULL,
            `total_controls` int NULL,
            `manhattan_s3_key` varchar(500) NULL,
            `qq_s3_key` varchar(500) NULL,
            `meta_s3_key` varchar(500) NULL,
            `summary_json_s3_key` varchar(500) NULL,
            `summary_tsv_s3_key` varchar(500) NULL,
            `top_loci_s3_key` varchar(500) NULL,
            `batch_job_id` varchar(255) NULL,
            `error_message` text NULL,
            `submitted_by` varchar(255) NULL,
            `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
            `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (`id`),
            KEY `hcm_gwas_ma_results_status_idx` (`status`),
            KEY `hcm_gwas_ma_results_created_idx` (`created_at`)
        )
    """))


def downgrade() -> None:
    op.get_bind().execute(text("DROP TABLE `hcm_gwas_ma_results`"))
