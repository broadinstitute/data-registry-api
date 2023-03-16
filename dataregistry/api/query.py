import datetime
import re
import uuid

from sqlalchemy import text

from dataregistry.api import s3
from dataregistry.api.model import SavedRecord, DataSet, Study, SavedStudy


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


def insert_study(engine, data: Study):
    with engine.connect() as conn:
        sql_params = data.dict()
        study_id = str(uuid.uuid4()).replace('-', '')
        sql_params.update({'id': study_id})
        conn.execute(text("""
            INSERT INTO studies (id, name, institution, created_at) VALUES(:id, :name, :institution, NOW())
        """), sql_params)
        conn.commit()
    return study_id


def get_studies(engine):
    with engine.connect() as conn:
        results = conn.execute(text("""
                SELECT id, name, institution, created_at 
                FROM studies 
            """)
                               )
    return [SavedStudy(**row._asdict()) for row in results]


def insert_dataset(engine, data: DataSet):
    with engine.connect() as conn:
        sql_params = data.dict()
        dataset_id = str(uuid.uuid4()).replace('-', '')
        sql_params.update({'id': dataset_id})
        conn.execute(text("""
            INSERT INTO datasets (id, name, data_source_type, data_type, genome_build,
            ancestry, data_contributor, data_contributor_email, data_submitter, data_submitter_email,  
            sex, global_sample_size, status, description, pmid, publication, study_id, created_at) VALUES(:id, :name, 
            :data_source_type, :data_type, :genome_build, :ancestry, :data_contributor, :data_contributor_email, 
            :data_submitter, :data_submitter_email, :sex, :global_sample_size, :status, :description, :pmid,
            :publication, :study_id, NOW())
        """), sql_params)
        conn.commit()
    return dataset_id


def insert_phenotype_data_set(engine, dataset_id: str, phenotype: str, s3_path: str, dichotomous: bool,
                              sample_size: int, cases: int, controls: int):
    with engine.connect() as conn:
        pd_id = str(uuid.uuid4()).replace('-', '')
        sql_params = {'id': pd_id, 'dataset_id': dataset_id, 's3_path': s3_path, 'phenotype': phenotype,
                      'dichotomous': dichotomous, 'sample_size': sample_size, 'cases': cases, 'file_name': 'boo',
                      'controls': controls
                      }
        conn.execute(text("""
            INSERT INTO dataset_phenotypes (id, dataset_id, phenotype, s3_path, dichotomous, sample_size, cases, 
            created_at, file_name, controls) 
            VALUES(:id, :dataset_id, :phenotype, :s3_path, :dichotomous, :sample_size, :cases, NOW(), :file_name, 
            :controls)"""), sql_params)
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
