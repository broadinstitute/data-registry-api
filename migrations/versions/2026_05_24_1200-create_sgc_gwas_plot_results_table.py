"""create sgc_gwas_plot_results table

Revision ID: create_sgc_gwas_plot_results
Revises: drop_file_uploads_genome_build
Create Date: 2026-05-24 12:00:00.000000

"""
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'create_sgc_gwas_plot_results'
down_revision = 'drop_file_uploads_genome_build'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("""
        CREATE TABLE `sgc_gwas_plot_results` (
            `id` binary(32) NOT NULL,
            `file_id` binary(32) NOT NULL,
            `batch_job_id` varchar(100) NULL COMMENT 'AWS Batch job ID',
            `status` varchar(20) NOT NULL COMMENT 'PENDING, RUNNING, SUCCEEDED, FAILED',
            `lambda_gc` double NULL COMMENT 'genomic inflation factor (median chi^2 / 0.4549)',
            `n_variants` bigint NULL COMMENT 'count of variants with valid chromosome and 0 < p <= 1',
            `n_sig_5e8` bigint NULL COMMENT 'count of variants with p <= 5e-8 (genome-wide significant)',
            `n_sig_1e5` bigint NULL COMMENT 'count of variants with p <= 1e-5 (suggestive)',
            `manhattan_s3_key` varchar(500) NULL,
            `qq_s3_key` varchar(500) NULL,
            `error_message` text NULL,
            `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
            `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (`id`),
            UNIQUE KEY `sgc_gwas_plot_results_file_id_uniq` (`file_id`),
            KEY `sgc_gwas_plot_results_status_idx` (`status`),
            CONSTRAINT `sgc_gwas_plot_results_file_fk` FOREIGN KEY (`file_id`)
                REFERENCES `sgc_gwas_files` (`id`) ON DELETE CASCADE
        )
    """))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("DROP TABLE `sgc_gwas_plot_results`"))
