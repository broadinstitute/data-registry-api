"""more_record_columns

Revision ID: bf402cdb0fa8
Revises: df68d16420af
Create Date: 2023-02-10 12:31:05.170635

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'bf402cdb0fa8'
down_revision = 'df68d16420af'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("ALTER TABLE records add column data_source_type varchar(10)"))
    conn.execute(text("ALTER TABLE records add column data_source text"))
    conn.execute(text("ALTER TABLE records add column data_type varchar(10)"))
    conn.execute(text("ALTER TABLE records add column genome_build varchar(10)"))
    conn.execute(text("ALTER TABLE records add column ancestry varchar(10)"))
    conn.execute(text("ALTER TABLE records add column data_submitter text"))
    conn.execute(text("ALTER TABLE records add column data_submitter_email text"))
    conn.execute(text("ALTER TABLE records add column institution text"))
    conn.execute(text("ALTER TABLE records add column sex varchar(10)"))
    conn.execute(text("ALTER TABLE records add column global_sample_size int"))
    conn.execute(text("ALTER TABLE records add column t1d_sample_size int"))
    conn.execute(text("ALTER TABLE records add column bmi_adj_sample_size int"))
    conn.execute(text("ALTER TABLE records add column status varchar(10)"))
    conn.execute(text("ALTER TABLE records add column additional_data text"))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("ALTER TABLE records drop column data_source_type"))
    conn.execute(text("ALTER TABLE records drop column data_source"))
    conn.execute(text("ALTER TABLE records drop column data_type"))
    conn.execute(text("ALTER TABLE records drop column genome_build"))
    conn.execute(text("ALTER TABLE records drop column ancestry"))
    conn.execute(text("ALTER TABLE records drop column data_submitter"))
    conn.execute(text("ALTER TABLE records drop column data_submitter_email"))
    conn.execute(text("ALTER TABLE records drop column institution"))
    conn.execute(text("ALTER TABLE records drop column sex"))
    conn.execute(text("ALTER TABLE records drop column global_sample_size"))
    conn.execute(text("ALTER TABLE records drop column t1d_sample_size"))
    conn.execute(text("ALTER TABLE records drop column bmi_adj_sample_size"))
    conn.execute(text("ALTER TABLE records drop column status"))
    conn.execute(text("ALTER TABLE records drop column additional_data"))
