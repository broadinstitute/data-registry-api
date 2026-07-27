"""create sgc_ma_ignore table

Revision ID: create_sgc_ma_ignore
Revises: add_ma_totals_columns
Create Date: 2026-07-23 12:00:00.000000

"""
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'create_sgc_ma_ignore'
down_revision = 'add_ma_totals_columns'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("""
        CREATE TABLE `sgc_ma_ignore` (
            `id` binary(32) NOT NULL,
            `cohort_id` binary(32) NOT NULL,
            `phenotype` varchar(100) NOT NULL,
            `ancestry` varchar(50) NOT NULL,
            `reason` text NOT NULL,
            `excluded_by` varchar(255) NOT NULL,
            `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (`id`),
            UNIQUE KEY `sgc_ma_ignore_cohort_pheno_anc_uniq` (`cohort_id`, `phenotype`, `ancestry`),
            CONSTRAINT `sgc_ma_ignore_cohort_fk` FOREIGN KEY (`cohort_id`)
                REFERENCES `sgc_cohorts` (`id`) ON DELETE CASCADE
        )
    """))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("DROP TABLE `sgc_ma_ignore`"))
