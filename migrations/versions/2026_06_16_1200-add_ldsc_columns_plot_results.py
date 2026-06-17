"""add ldsc_* columns to sgc_gwas_plot_results

Revision ID: add_ldsc_columns_plot_results
Revises: add_lambda_maf01_plot_results
Create Date: 2026-06-16 12:00:00.000000
"""
from alembic import op
from sqlalchemy import text

revision = 'add_ldsc_columns_plot_results'
down_revision = 'add_lambda_maf01_plot_results'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.get_bind().execute(text("""
        ALTER TABLE `sgc_gwas_plot_results`
            ADD COLUMN `ldsc_status` varchar(20) NULL COMMENT 'LDSC lifecycle: PENDING/RUNNING/SUCCEEDED/FAILED' AFTER `qq_s3_key`,
            ADD COLUMN `ldsc_batch_job_id` varchar(100) NULL AFTER `ldsc_status`,
            ADD COLUMN `ldsc_intercept` double NULL COMMENT 'univariate LDSC intercept (>1 => confounding/stratification)' AFTER `ldsc_batch_job_id`,
            ADD COLUMN `ldsc_h2` double NULL COMMENT 'univariate LDSC SNP heritability (observed scale)' AFTER `ldsc_intercept`,
            ADD COLUMN `ldsc_ratio` double NULL COMMENT '(intercept-1)/(mean chi^2 - 1): fraction of inflation from confounding' AFTER `ldsc_h2`,
            ADD COLUMN `ldsc_effective_n` double NULL COMMENT 'mean per-variant effective N used by LDSC' AFTER `ldsc_ratio`,
            ADD COLUMN `ldsc_n_snps` bigint NULL COMMENT 'SNPs in the LDSC regression after filtering' AFTER `ldsc_effective_n`,
            ADD COLUMN `ldsc_error` text NULL AFTER `ldsc_n_snps`
    """))


def downgrade() -> None:
    op.get_bind().execute(text("""
        ALTER TABLE `sgc_gwas_plot_results`
            DROP COLUMN `ldsc_status`, DROP COLUMN `ldsc_batch_job_id`,
            DROP COLUMN `ldsc_intercept`, DROP COLUMN `ldsc_h2`, DROP COLUMN `ldsc_ratio`,
            DROP COLUMN `ldsc_effective_n`, DROP COLUMN `ldsc_n_snps`, DROP COLUMN `ldsc_error`
    """))
