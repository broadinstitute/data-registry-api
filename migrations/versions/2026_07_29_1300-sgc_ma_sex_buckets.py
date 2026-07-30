"""sgc MA sex dimension + nine-bucket CHECK + cohort-code seed table

Revision ID: sgc_ma_sex_buckets
Revises: sgc_ma_multi_run
Create Date: 2026-07-29 13:00:00.000000
"""
from alembic import op
from sqlalchemy import text

revision = 'sgc_ma_sex_buckets'
down_revision = 'sgc_ma_multi_run'
branch_labels = None
depends_on = None

_CHECK = ("ancestry IN ('Combined','AFR','AMR','EAS','EUR','MID','SAS') "
          "AND sex IN ('All','Male','Female') "
          "AND (sex = 'All' OR ancestry = 'Combined')")

# code -> cohort_id (32-char hex), authoritative from cohort_codes_SGC.xlsx
_COHORT_CODES = [
    ('MVP', '378d143ab13e4aff90ee8c91792552fa'),
    ('AOU', 'fcbcb26abaee4e9e86e306075853c3fd'),
    ('FG', 'e6904a8b765a4ff1a277627766403f8a'),
    ('BV', '4052f0fa56d54672a6b2e52d6f17bc69'),
    ('PENN', '40555cf6ff744171a206bc4e5f9a5ae8'),
    ('GEL', '260b921863f040d78bf2c6892ba06108'),
    ('CHOP', 'a034dbbc1fb44f97b118db54c44ab1ad'),
    ('UKB', '9e52dfe5f5d04d988850792738cb18ca'),
    ('BBJ1', '83dffccc28064ee9b1672126e70d2343'),
    ('BBJ2', '3455eaaa2b8544768205f10bd5f13e1d'),
    ('HEL', '020eb32f44664d3495e520c79d6c5e7d'),
    ('MGI', 'bd84034504044303b0aa7c9d8cd3a1f5'),
    ('EBB', '83fefc3533a34ff3b74cb74476082480'),
    ('HUNT', 'f9ccb165f89340768c9320953420df57'),
    ('TMM', 'aae85fea8cdd45dca3bb9321e9de68b2'),
    ('GNH', 'f269776cbe3b45ca97e4eb7210f59c95'),
    ('ROT', '7d503906308b435a859b8c924a1175d3'),
    ('MGB', '4b5a1294137446c68fe8d4e5d1a57941'),
    ('TPMI', '4b40c0abadaf46c89d1230ac9ee8f087'),
    ('BBGE', '09bae152190f4f27b5fe9178a466bf36'),
    ('BBCE', '584ed62d143548db98394beea03d5a2b'),
    ('BBCS', '17584cd72e0043db8a811281c8d7fc2e'),
    ('BBGS', '80bd8318282446c3a3e7f42c5f7ff350'),
    ('MoBc', 'e010f363307e4667b06fbb431670f71d'),
    ('MoBp', '86c6aa94147e4cbfb107f9e60360e759'),
    ('BPG', 'dc38144d001e4d4d950971055ee87f53'),
    ('BIBo', 'fba6df62a7be4ff39f3e64c7b67c6382'),
    ('BIB', '7d65882959de4850b8577826f616f4d3'),
]


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text(
        "ALTER TABLE `sgc_gwas_ma_results` "
        "ADD COLUMN `sex` varchar(10) NOT NULL DEFAULT 'All', "
        f"ADD CONSTRAINT `sgc_ma_results_bucket_chk` CHECK ({_CHECK})"))
    # DROP INDEX and the replacement ADD UNIQUE KEY must be in the same ALTER TABLE
    # statement: sgc_ma_ignore_cohort_fk relies on sgc_ma_ignore_cohort_pheno_anc_uniq
    # as its only covering index, and MySQL rejects a bare DROP INDEX that would leave
    # the FK momentarily uncovered (error 1553). A single multi-clause ALTER TABLE
    # evaluates the FK requirement against the resulting table, not the intermediate one.
    conn.execute(text(
        "ALTER TABLE `sgc_ma_ignore` "
        "DROP INDEX `sgc_ma_ignore_cohort_pheno_anc_uniq`, "
        "ADD COLUMN `sex` varchar(10) NOT NULL DEFAULT 'All', "
        "ADD UNIQUE KEY `sgc_ma_ignore_cohort_pheno_anc_sex_uniq` "
        "(`cohort_id`,`phenotype`,`ancestry`,`sex`), "
        f"ADD CONSTRAINT `sgc_ma_ignore_bucket_chk` CHECK ({_CHECK})"))
    conn.execute(text("""
        CREATE TABLE `sgc_cohort_codes` (
            `code` varchar(16) NOT NULL,
            `cohort_id` binary(32) NOT NULL,
            PRIMARY KEY (`code`),
            CONSTRAINT `sgc_cohort_codes_cohort_fk` FOREIGN KEY (`cohort_id`)
                REFERENCES `sgc_cohorts` (`id`) ON DELETE CASCADE
        )
    """))
    for code, cid in _COHORT_CODES:
        conn.execute(text("""
            INSERT INTO `sgc_cohort_codes` (`code`, `cohort_id`)
            SELECT :code, :cid FROM DUAL
            WHERE EXISTS (SELECT 1 FROM `sgc_cohorts` WHERE `id` = :cid)
        """), {"code": code, "cid": cid})


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("DROP TABLE `sgc_cohort_codes`"))
    conn.execute(text(
        "ALTER TABLE `sgc_ma_ignore` "
        "DROP CHECK `sgc_ma_ignore_bucket_chk`, "
        "DROP INDEX `sgc_ma_ignore_cohort_pheno_anc_sex_uniq`, "
        "DROP COLUMN `sex`, "
        "ADD UNIQUE KEY `sgc_ma_ignore_cohort_pheno_anc_uniq` (`cohort_id`,`phenotype`,`ancestry`)"))
    conn.execute(text(
        "ALTER TABLE `sgc_gwas_ma_results` "
        "DROP CHECK `sgc_ma_results_bucket_chk`, "
        "DROP COLUMN `sex`"))
