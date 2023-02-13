import datetime
import json
import re

from sqlalchemy.orm import Session

from dataregistry.api.model import Record, SavedRecord


def get_all_records(engine) -> list:
    results = engine.execute(
        """
        SELECT s3_bucket_id, name, metadata, data_source_type, data_source, data_type, genome_build,
            ancestry, data_submitter, data_submitter_email, institution, sex, global_sample_size, t1d_sample_size, 
            bmi_adj_sample_size, status, additional_data, deleted_at_unix_time as deleted_at, id, created_at 
            FROM records WHERE deleted_at_unix_time = 0
        """
    ).fetchall()

    return [SavedRecord(**fix_json(dict(result))) for result in results]


def fix_json(r: dict) -> dict:
    r.update({'metadata': json.loads(r['metadata'])})
    return r


def get_record(engine, index) -> SavedRecord:
    session = Session(engine)
    with session.begin():
        results = session.execute(
            """
            SELECT s3_bucket_id, name, metadata, data_source_type, data_source, data_type, genome_build,
                ancestry, data_submitter, data_submitter_email, institution, sex, global_sample_size, t1d_sample_size, 
                bmi_adj_sample_size, status, additional_data, deleted_at_unix_time as deleted_at, id, created_at
                FROM records r WHERE r.id = :id 
            """, {'id': index}
        ).fetchall()

        if len(results) == 0:
            raise ValueError(f"No records for id {index}")
        elif len(results) > 1:
            raise ValueError(f"{len(results)} records for id {index}, should be unique")
        else:
            return SavedRecord(**fix_json(dict(results[0])))


def convert_name_to_s3_bucket_id(name):
    dt = datetime.datetime.now()
    return '{}-{}'.format(re.sub(r'[^\w-]+', '', name.replace(' ', '-')), dt.strftime('%Y-%m-%d-%H-%M-%S'))


def insert_record(engine, data: Record):
    s3_record_id = convert_name_to_s3_bucket_id(data.name)
    session = Session(engine)
    with session.begin():
        sql_params = data.dict()
        sql_params.update({'s3_bucket_id': s3_record_id, 'metadata': json.dumps(data.metadata)})
        session.execute("""
            INSERT INTO records (s3_bucket_id, name, metadata, data_source_type, data_source, data_type, genome_build,
            ancestry, data_submitter, data_submitter_email, institution, sex, global_sample_size, t1d_sample_size, 
            bmi_adj_sample_size, status, additional_data) VALUES(:s3_bucket_id, :name, :metadata, :data_source_type, 
            :data_source, :data_type, :genome_build, :ancestry, :data_submitter, :data_submitter_email, :institution, 
            :sex, :global_sample_size, :t1d_sample_size, :bmi_adj_sample_size, :status, :additional_data)
        """, sql_params)
    return s3_record_id


def delete_record(engine, index):
    session = Session(engine)
    with session.begin():
        s3_record_id = get_record(engine, index).s3_bucket_id
        engine.execute(
            """
            UPDATE records r SET r.deleted_at_unix_time = UNIX_TIMESTAMP() WHERE r.id = {} 
            """.format(index)
        )
    return s3_record_id
