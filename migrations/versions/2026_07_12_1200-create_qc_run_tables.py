"""create qc_run and qc_step_result tables

Revision ID: create_qc_run_tables
Revises: create_sgc_gwas_ma_results
Create Date: 2026-07-12 12:00:00.000000

"""
from alembic import op
from sqlalchemy import text

revision = 'create_qc_run_tables'
down_revision = 'create_sgc_gwas_ma_results'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("""
        CREATE TABLE `qc_run` (
            `id` binary(32) NOT NULL,
            `input_s3_path` varchar(1000) NOT NULL,
            `pipeline` varchar(200) NOT NULL,
            `pinned_commit` varchar(64) NULL,
            `image_digest` varchar(200) NULL,
            `status` varchar(20) NOT NULL COMMENT 'SUBMITTED, RUNNING, COMPLETED, FAILED',
            `overall_verdict` varchar(20) NULL COMMENT 'pass, warn, fail',
            `gwas_filtered_s3_key` varchar(1000) NULL,
            `qc_report_s3_key` varchar(1000) NULL,
            `batch_job_id` varchar(100) NULL,
            `error_message` text NULL,
            `submitted_by` varchar(255) NOT NULL DEFAULT '',
            `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
            `completed_at` datetime NULL,
            PRIMARY KEY (`id`),
            KEY `qc_run_status_idx` (`status`)
        )
    """))
    conn.execute(text("""
        CREATE TABLE `qc_step_result` (
            `id` binary(32) NOT NULL,
            `run_id` binary(32) NOT NULL,
            `step` varchar(200) NOT NULL,
            `verdict` varchar(20) NULL COMMENT 'pass, warn, fail',
            `metrics` text NULL,
            `messages` text NULL,
            `artifacts` text NULL,
            `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (`id`),
            KEY `qc_step_result_run_idx` (`run_id`),
            CONSTRAINT `qc_step_result_run_fk` FOREIGN KEY (`run_id`)
                REFERENCES `qc_run` (`id`) ON DELETE CASCADE
        )
    """))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("DROP TABLE `qc_step_result`"))
    conn.execute(text("DROP TABLE `qc_run`"))
