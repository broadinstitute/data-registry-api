"""create peg_studies and peg_files tables

Revision ID: abc123peg001
Revises: 2025_11_20_1436
Create Date: 2025-12-04 18:46:00.000000

"""
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'abc123peg001'
down_revision = 'replace_sgc_phenotypes_data'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    
    # Create peg_studies table
    conn.execute(text("""
        CREATE TABLE `peg_studies` (
            `id` varchar(32) NOT NULL COMMENT 'hex UUID without dashes',
            `name` varchar(255) NOT NULL COMMENT 'name of the PEG study',
            `created_by` varchar(255) NOT NULL COMMENT 'username of creator',
            `metadata` TEXT NOT NULL COMMENT 'JSON metadata including evidence streams and integration analyses',
            `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
            `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (`id`),
            INDEX `idx_created_at` (`created_at`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """))
    
    # Create peg_files table
    conn.execute(text("""
        CREATE TABLE `peg_files` (
            `id` varchar(32) NOT NULL COMMENT 'hex UUID without dashes',
            `study_id` varchar(32) NOT NULL COMMENT 'reference to peg_studies.id',
            `file_type` varchar(50) NOT NULL COMMENT 'peg_list or peg_matrix',
            `file_name` varchar(255) NOT NULL,
            `file_path` varchar(1000) NOT NULL COMMENT 'S3 path to file',
            `file_size` bigint NOT NULL COMMENT 'file size in bytes',
            `uploaded_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (`id`),
            FOREIGN KEY (`study_id`) REFERENCES `peg_studies`(`id`) ON DELETE CASCADE,
            INDEX `idx_study_id` (`study_id`),
            INDEX `idx_file_type` (`file_type`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """))


def downgrade() -> None:
    conn = op.get_bind()
    
    # Drop tables in reverse order (files first due to foreign key)
    conn.execute(text("DROP TABLE IF EXISTS `peg_files`"))
    conn.execute(text("DROP TABLE IF EXISTS `peg_studies`"))
