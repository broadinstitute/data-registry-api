"""sgc_gwas_ma_results multi-run: drop pheno/ancestry unique, add run fields

Revision ID: sgc_ma_multi_run
Revises: create_sgc_liftover_jobs
Create Date: 2026-07-29 12:00:00.000000
"""
from alembic import op
from sqlalchemy import text

revision = 'sgc_ma_multi_run'
down_revision = 'create_sgc_liftover_jobs'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("ALTER TABLE `sgc_gwas_ma_results` "
                      "DROP INDEX `sgc_gwas_ma_results_pheno_ancestry_uniq`"))
    conn.execute(text("""
        ALTER TABLE `sgc_gwas_ma_results`
            ADD COLUMN `label` varchar(255) NULL,
            ADD COLUMN `run_type` varchar(16) NOT NULL DEFAULT 'auto',
            ADD COLUMN `dataset_file_ids` json NULL,
            ADD COLUMN `maf_min` double NULL,
            ADD COLUMN `info_min` double NULL,
            ADD COLUMN `submitted_by` varchar(255) NULL,
            ADD KEY `sgc_gwas_ma_results_pheno_anc_created_idx` (`phenotype`, `ancestry`, `created_at`)
    """))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("""
        ALTER TABLE `sgc_gwas_ma_results`
            DROP KEY `sgc_gwas_ma_results_pheno_anc_created_idx`,
            DROP COLUMN `label`,
            DROP COLUMN `run_type`,
            DROP COLUMN `dataset_file_ids`,
            DROP COLUMN `maf_min`,
            DROP COLUMN `info_min`,
            DROP COLUMN `submitted_by`
    """))
    conn.execute(text("ALTER TABLE `sgc_gwas_ma_results` "
                      "ADD UNIQUE KEY `sgc_gwas_ma_results_pheno_ancestry_uniq` (`phenotype`, `ancestry`)"))
