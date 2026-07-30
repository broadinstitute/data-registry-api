"""sgc_ma_ignore -> file-id based (revert of target-based ignore)

Revision ID: sgc_ma_ignore_file_based
Revises: sgc_ma_sex_buckets
Create Date: 2026-07-31 12:00:00.000000
"""
from alembic import op
from sqlalchemy import text

revision = 'sgc_ma_ignore_file_based'
down_revision = 'sgc_ma_sex_buckets'
branch_labels = None
depends_on = None

_BUCKET_CHECK = ("ancestry IN ('Combined','AFR','AMR','EAS','EUR','MID','SAS') "
                 "AND sex IN ('All','Male','Female') "
                 "AND (sex = 'All' OR ancestry = 'Combined')")

_FILE_BASED = """
    CREATE TABLE `sgc_ma_ignore` (
        `id` binary(32) NOT NULL,
        `file_id` binary(32) NOT NULL,
        `reason` text NOT NULL,
        `excluded_by` varchar(255) NOT NULL,
        `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (`id`),
        UNIQUE KEY `sgc_ma_ignore_file_uniq` (`file_id`),
        CONSTRAINT `sgc_ma_ignore_file_fk` FOREIGN KEY (`file_id`)
            REFERENCES `sgc_gwas_files` (`id`) ON DELETE CASCADE
    )
"""


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("DROP TABLE `sgc_ma_ignore`"))
    conn.execute(text(_FILE_BASED))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("DROP TABLE `sgc_ma_ignore`"))
    conn.execute(text(f"""
        CREATE TABLE `sgc_ma_ignore` (
            `id` binary(32) NOT NULL,
            `cohort_id` binary(32) NOT NULL,
            `phenotype` varchar(100) NOT NULL,
            `ancestry` varchar(50) NOT NULL,
            `sex` varchar(10) NOT NULL DEFAULT 'All',
            `reason` text NOT NULL,
            `excluded_by` varchar(255) NOT NULL,
            `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (`id`),
            UNIQUE KEY `sgc_ma_ignore_cohort_pheno_anc_sex_uniq` (`cohort_id`,`phenotype`,`ancestry`,`sex`),
            CONSTRAINT `sgc_ma_ignore_cohort_fk` FOREIGN KEY (`cohort_id`)
                REFERENCES `sgc_cohorts` (`id`) ON DELETE CASCADE,
            CONSTRAINT `sgc_ma_ignore_bucket_chk` CHECK ({_BUCKET_CHECK})
        )
    """))
