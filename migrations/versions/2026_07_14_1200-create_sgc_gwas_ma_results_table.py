"""create sgc_gwas_ma_results table

Revision ID: create_sgc_gwas_ma_results
Revises: add_calr_shared_flag
Create Date: 2026-07-14 12:00:00.000000

"""
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'create_sgc_gwas_ma_results'
down_revision = 'add_calr_shared_flag'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("""
        CREATE TABLE `sgc_gwas_ma_results` (
            `id` binary(32) NOT NULL,
            `phenotype` varchar(100) NOT NULL,
            `ancestry` varchar(50) NOT NULL,
            `status` varchar(20) NOT NULL COMMENT 'PENDING, RUNNING, SUCCEEDED, FAILED',
            `meta_lambda_gc` double NULL,
            `n_meta_variants` bigint NULL,
            `n_genome_wide_sig` int NULL,
            `n_cohorts` int NULL,
            `n_cohorts_used` int NULL,
            `manhattan_s3_key` varchar(500) NULL,
            `qq_s3_key` varchar(500) NULL,
            `meta_s3_key` varchar(500) NULL,
            `summary_json_s3_key` varchar(500) NULL,
            `summary_tsv_s3_key` varchar(500) NULL,
            `top_loci_s3_key` varchar(500) NULL,
            `batch_job_id` varchar(255) NULL,
            `error_message` text NULL,
            `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
            `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (`id`),
            UNIQUE KEY `sgc_gwas_ma_results_pheno_ancestry_uniq` (`phenotype`, `ancestry`),
            KEY `sgc_gwas_ma_results_status_idx` (`status`)
        )
    """))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("DROP TABLE `sgc_gwas_ma_results`"))
