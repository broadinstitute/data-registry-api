"""more record fields

Revision ID: d269874897b7
Revises: aaa800cb0c9c
Create Date: 2023-02-21 15:11:02.532454

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'd269874897b7'
down_revision = 'aaa800cb0c9c'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("alter table records modify s3_bucket_id varchar(100) not null"))
    conn.execute(text("alter table records modify name varchar(100) not null"))
    conn.execute(text("alter table records modify created_at datetime default CURRENT_TIMESTAMP not null"))
    conn.execute(text("alter table records modify deleted_at_unix_time int default 0 not null"))
    conn.execute(text("alter table records modify data_source_type varchar(10) not null"))
    conn.execute(text("alter table records modify data_source text not null"))
    conn.execute(text("alter table records modify data_type varchar(10) not null"))
    conn.execute(text("alter table records modify genome_build varchar(10) not null"))
    conn.execute(text("alter table records modify ancestry varchar(10) not null"))
    conn.execute(text("alter table records modify data_submitter text not null"))
    conn.execute(text("alter table records modify data_submitter_email text not null"))
    conn.execute(text("alter table records modify institution text not null"))
    conn.execute(text("alter table records modify sex varchar(10) not null"))
    conn.execute(text("alter table records modify global_sample_size int not null"))
    conn.execute(text("alter table records modify t1d_sample_size int not null"))
    conn.execute(text("alter table records modify bmi_adj_sample_size int not null"))
    conn.execute(text("alter table records modify status varchar(10) not null"))
    conn.execute(text("alter table records modify additional_data text not null"))
    conn.execute(text("alter table records add credible_set text"))
    conn.execute(text("alter table records drop column metadata"))



def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("alter table records modify s3_bucket_id varchar(100)"))
    conn.execute(text("alter table records modify name varchar(100)"))
    conn.execute(text("alter table records modify created_at datetime default CURRENT_TIMESTAMP"))
    conn.execute(text("alter table records modify deleted_at_unix_time int default 0"))
    conn.execute(text("alter table records modify data_source_type varchar(10)"))
    conn.execute(text("alter table records modify data_source text"))
    conn.execute(text("alter table records modify data_type varchar(10)"))
    conn.execute(text("alter table records modify genome_build varchar(10)"))
    conn.execute(text("alter table records modify ancestry varchar(10)"))
    conn.execute(text("alter table records modify data_submitter text"))
    conn.execute(text("alter table records modify data_submitter_email text"))
    conn.execute(text("alter table records modify institution text"))
    conn.execute(text("alter table records modify sex varchar(10)"))
    conn.execute(text("alter table records modify global_sample_size int"))
    conn.execute(text("alter table records modify t1d_sample_size int"))
    conn.execute(text("alter table records modify bmi_adj_sample_size int"))
    conn.execute(text("alter table records modify status varchar(10)"))
    conn.execute(text("alter table records modify additional_data text"))
    conn.execute(text("alter table records drop column credible_set"))
    conn.execute(text("alter table records add column metadata json"))
