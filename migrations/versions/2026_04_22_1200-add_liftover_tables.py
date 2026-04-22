"""add liftover tables

Revision ID: add_liftover_tables
Revises: merge_mskkp_calr_heads
Create Date: 2026-04-22 12:00:00.000000

"""
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'add_liftover_tables'
down_revision = 'merge_mskkp_calr_heads'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # Create portal_liftover_config table
    conn.execute(text("""
        CREATE TABLE `portal_liftover_config` (
        `portal_id` VARCHAR(64) NOT NULL,
        `target_genome_build` VARCHAR(16) NOT NULL,
        `updated_at` DATETIME NOT NULL,
        `updated_by` VARCHAR(255) NOT NULL,
        PRIMARY KEY (`portal_id`)
        )
    """))

    # Create liftover_jobs table
    conn.execute(text("""
        CREATE TABLE `liftover_jobs` (
        `id` BINARY(32) NOT NULL,
        `file_id` BINARY(32) NOT NULL,
        `source_genome_build` VARCHAR(16) NOT NULL,
        `target_genome_build` VARCHAR(16) NOT NULL,
        `batch_job_id` VARCHAR(128) NULL,
        `status` VARCHAR(64) NOT NULL,
        `submitted_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        `completed_at` DATETIME NULL,
        `submitted_by` VARCHAR(255) NOT NULL,
        `original_s3_path` VARCHAR(1024) NULL,
        `unmapped_s3_path` VARCHAR(1024) NULL,
        `summary` JSON NULL,
        `log` MEDIUMTEXT NULL,
        PRIMARY KEY (`id`),
        KEY `liftover_jobs_file_id_idx` (`file_id`),
        KEY `liftover_jobs_status_idx` (`status`),
        CONSTRAINT `liftover_jobs_file_fk` FOREIGN KEY (`file_id`) REFERENCES `file_uploads` (`id`) ON DELETE CASCADE
        )
    """))

    # Add genome_build column to file_uploads
    conn.execute(text("""
        ALTER TABLE `file_uploads`
        ADD COLUMN `genome_build` VARCHAR(16) NOT NULL DEFAULT 'na' AFTER `qc_job_completed_at`
    """))

    # Seed hermes portal config row
    conn.execute(text("""
        INSERT IGNORE INTO `portal_liftover_config` (`portal_id`, `target_genome_build`, `updated_at`, `updated_by`)
        VALUES ('hermes', 'hg19', NOW(), 'system')
    """))


def downgrade() -> None:
    conn = op.get_bind()

    # Delete seed row
    conn.execute(text("""
        DELETE FROM `portal_liftover_config` WHERE `portal_id` = 'hermes'
    """))

    # Remove genome_build column from file_uploads
    conn.execute(text("""
        ALTER TABLE `file_uploads`
        DROP COLUMN `genome_build`
    """))

    # Drop liftover_jobs table
    conn.execute(text("DROP TABLE `liftover_jobs`"))

    # Drop portal_liftover_config table
    conn.execute(text("DROP TABLE `portal_liftover_config`"))
