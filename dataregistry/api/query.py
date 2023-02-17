import datetime
import json
import re

from sqlalchemy import text

from dataregistry.api import s3
from dataregistry.api.model import Record, SavedRecord


def get_all_records(engine) -> list:
    with engine.connect() as conn:
        results = conn.execute(text(
            """
            SELECT s3_bucket_id, name, metadata, data_source_type, data_source, data_type, genome_build,
                ancestry, data_submitter, data_submitter_email, institution, sex, global_sample_size, t1d_sample_size, 
                bmi_adj_sample_size, status, additional_data, deleted_at_unix_time as deleted_at, id, created_at 
                FROM records WHERE deleted_at_unix_time = 0
            """)
        )
    return [SavedRecord(**fix_json(row._asdict())) for row in results]


def fix_json(r: dict) -> dict:
    r.update({'metadata': json.loads(r['metadata'])})
    return r


def get_record(engine, index) -> SavedRecord:
    with engine.connect() as conn:
        result = conn.execute(
            text("""
            SELECT s3_bucket_id, name, metadata, data_source_type, data_source, data_type, genome_build,
                ancestry, data_submitter, data_submitter_email, institution, sex, global_sample_size, t1d_sample_size, 
                bmi_adj_sample_size, status, additional_data, deleted_at_unix_time as deleted_at, id, created_at
                FROM records r WHERE r.id = :id and deleted_at_unix_time = 0
            """), {'id': index}
        ).first()

        if result is None:
            raise ValueError(f"No records for id {index}")
        else:
            return SavedRecord(**fix_json(result._asdict()))


def convert_name_to_s3_bucket_id(name):
    dt = datetime.datetime.now()
    return '{}-{}'.format(re.sub(r'[^\w-]+', '', name.replace(' ', '-')), dt.strftime('%Y-%m-%d-%H-%M-%S'))


def insert_record(engine, data: Record):
    s3_record_id = convert_name_to_s3_bucket_id(data.name)
    with engine.connect() as conn:
        sql_params = data.dict()
        sql_params.update({'s3_bucket_id': s3_record_id, 'metadata': json.dumps(data.metadata)})
        res = conn.execute(text("""
            INSERT INTO records (s3_bucket_id, name, metadata, data_source_type, data_source, data_type, genome_build,
            ancestry, data_submitter, data_submitter_email, institution, sex, global_sample_size, t1d_sample_size, 
            bmi_adj_sample_size, status, additional_data) VALUES(:s3_bucket_id, :name, :metadata, :data_source_type, 
            :data_source, :data_type, :genome_build, :ancestry, :data_submitter, :data_submitter_email, :institution, 
            :sex, :global_sample_size, :t1d_sample_size, :bmi_adj_sample_size, :status, :additional_data)
        """), sql_params)
        conn.commit()
        s3.create_record_directory(s3_record_id)
    return s3_record_id, res.lastrowid


def delete_record(engine, index):
    with engine.connect() as conn:
        s3_record_id = get_record(engine, index).s3_bucket_id
        conn.execute(
            text("""
            UPDATE records r SET r.deleted_at_unix_time = UNIX_TIMESTAMP() WHERE r.id = :id 
            """), {'id': index}
        )
        conn.commit()
        s3.delete_record_directory(s3_record_id)
    return s3_record_id
