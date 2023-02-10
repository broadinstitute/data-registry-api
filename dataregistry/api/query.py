import json

from dataregistry.api.domain import Record
from dataregistry.api import s3
import datetime
import re
from sqlalchemy.orm import Session

from dataregistry.api.model import RecordRequest


def get_all_records(engine):
    results = engine.execute(
        """
        SELECT r.* FROM records r WHERE r.deleted_at_unix_time = 0
        """
    ).fetchall()

    return [Record(**result) for result in results]


def get_record(engine, index):
    results = engine.execute(
        """
        SELECT 
            r.id, 
            r.s3_bucket_id, 
            r.name, 
            r.metadata, 
            r.created_at, 
            r.deleted_at_unix_time FROM records r WHERE r.id = {} 
        """.format(index)
    ).fetchall()

    if len(results) == 0:
        raise ValueError(f"No records for id {index}")
    elif len(results) > 1:
        raise ValueError(f"{len(results)} records for id {index}, should be unique")
    else:
        return Record(**results[0])


def convert_name_to_s3_bucket_id(name):
    dt = datetime.datetime.now()
    return '{}-{}'.format(re.sub(r'[^\w-]+', '', name.replace(' ', '-')), dt.strftime('%Y-%m-%d-%H-%M-%S'))


def insert_record(engine, data: RecordRequest):
    s3_record_id = convert_name_to_s3_bucket_id(data.name)
    session = Session(engine)
    with session.begin():
        session.execute("""
            INSERT INTO records (s3_bucket_id, name, metadata) VALUES("{}", "{}", '{}')
        """.format(s3_record_id, data.name, json.dumps(data.metadata)))
        s3.create_record_directory(s3_record_id)
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
        s3.delete_record_directory(s3_record_id)
    return s3_record_id
