"""update mskkp_datasets schema: phenotype required, add readme_s3_path

Revision ID: update_mskkp_datasets_schema
Revises: add_unique_mskkp_dataset_name
Create Date: 2026-04-21 12:00:00.000000

"""
from alembic import op
from sqlalchemy import text

revision = 'update_mskkp_datasets_schema'
down_revision = 'add_unique_mskkp_dataset_name'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    # Set existing NULL phenotype values to empty string before making NOT NULL
    conn.execute(text("UPDATE mskkp_datasets SET phenotype = '' WHERE phenotype IS NULL"))
    # Drop the index on phenotype (TEXT columns cannot be indexed without a key length)
    conn.execute(text("ALTER TABLE mskkp_datasets DROP INDEX mskkp_datasets_phenotype_idx"))
    # Expand phenotype to TEXT and make NOT NULL
    conn.execute(text(
        "ALTER TABLE mskkp_datasets MODIFY COLUMN phenotype TEXT NOT NULL "
        "COMMENT 'phenotype description (required) - describe phenotype and how it was defined'"
    ))
    # Add optional readme_s3_path column
    conn.execute(text(
        "ALTER TABLE mskkp_datasets ADD COLUMN readme_s3_path VARCHAR(500) NULL "
        "COMMENT 's3 key path to uploaded README file' AFTER s3_path"
    ))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("ALTER TABLE mskkp_datasets DROP COLUMN readme_s3_path"))
    conn.execute(text(
        "ALTER TABLE mskkp_datasets MODIFY COLUMN phenotype VARCHAR(100) NULL "
        "COMMENT 'phenotype description (optional)'"
    ))
    conn.execute(text(
        "ALTER TABLE mskkp_datasets ADD KEY mskkp_datasets_phenotype_idx (phenotype)"
    ))
