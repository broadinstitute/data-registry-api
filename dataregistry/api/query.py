import datetime
import re

from sqlalchemy import text

from dataregistry.api import s3
from dataregistry.api.model import SavedRecord, Record


def get_all_records(engine) -> list:
    with engine.connect() as conn:
        results = conn.execute(text("""
                SELECT s3_bucket_id, name, data_source_type, data_source, data_type, genome_build, credible_set,
                ancestry, data_submitter, data_submitter_email, institution, sex, global_sample_size, t1d_sample_size, 
                bmi_adj_sample_size, status, additional_data, deleted_at_unix_time as deleted_at, id, created_at 
                FROM records WHERE deleted_at_unix_time = 0
            """)
        )
    return [SavedRecord(**row._asdict()) for row in results]


def get_record(engine, index) -> SavedRecord:
    with engine.connect() as conn:
        result = conn.execute(
            text("""
            SELECT s3_bucket_id, name, data_source_type, data_source, data_type, genome_build, credible_set,
            ancestry, data_submitter, data_submitter_email, institution, sex, global_sample_size, t1d_sample_size, 
            bmi_adj_sample_size, status, additional_data, deleted_at_unix_time as deleted_at, id, created_at 
            FROM records WHERE deleted_at_unix_time = 0 and id = :id
            """), {'id': index}
        ).first()

    if result is None:
        raise ValueError(f"No records for id {index}")
    else:
        return SavedRecord(**result._asdict())


def convert_name_to_s3_bucket_id(name):
    dt = datetime.datetime.now()
    return '{}-{}'.format(re.sub(r'[^\w-]+', '', name.replace(' ', '-')), dt.strftime('%Y-%m-%d-%H-%M-%S'))


def insert_record(engine, data: Record):
    s3_record_id = convert_name_to_s3_bucket_id(data.name)
    with engine.connect() as conn:
        sql_params = data.dict()
        sql_params.update({'s3_bucket_id': s3_record_id})
        res = conn.execute(text("""
            INSERT INTO records (s3_bucket_id, name, data_source_type, data_source, data_type, genome_build,
            ancestry, data_submitter, data_submitter_email, institution, sex, global_sample_size, t1d_sample_size, 
            bmi_adj_sample_size, status, additional_data, credible_set) VALUES(:s3_bucket_id, :name, :data_source_type, 
            :data_source, :data_type, :genome_build, :ancestry, :data_submitter, :data_submitter_email, :institution, 
            :sex, :global_sample_size, :t1d_sample_size, :bmi_adj_sample_size, :status, :additional_data, :credible_set)
        """), sql_params)
        conn.commit()
        s3.create_record_directory(s3_record_id)
    return s3_record_id, res.lastrowid


def insert_data_set(engine, record_id: int, s3_bucket_id: str, phenotype: str, data_type: str, name: str):
    with engine.connect() as conn:
        sql_params = {'record_id': record_id, 's3_bucket_id': s3_bucket_id, 'phenotype': phenotype,
                      'data_type': data_type, 'name': name}
        conn.execute(text("""
            INSERT INTO datasets (record_id, s3_bucket_id, name, phenotype, data_type) 
            VALUES(:record_id, :s3_bucket_id, :name, :phenotype, :data_type)
        """), sql_params)
        conn.commit()


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
