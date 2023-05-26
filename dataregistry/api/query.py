import datetime
import re
import uuid

from sqlalchemy import text

from dataregistry.api.model import SavedDataset, DataSet, Study, SavedStudy, SavedPhenotypeDataSet, SavedCredibleSet


def get_all_datasets(engine) -> list:
    with engine.connect() as conn:
        results = conn.execute(text("""select id, name, data_source_type, data_type, genome_build, ancestry, sex, 
        global_sample_size, status, data_submitter, data_submitter_email, data_contributor, data_contributor_email, 
        study_id, description, pub_id, publication, created_at from datasets"""))
    return [SavedDataset(**row._asdict()) for row in results]


def get_dataset(engine, index: uuid.UUID) -> SavedDataset:
    with engine.connect() as conn:
        result = conn.execute(
            text("""
            SELECT * FROM datasets WHERE id = :id
            """), {'id': str(index).replace('-', '')}
        ).first()

    if result is None:
        raise ValueError(f"No records for id {index}")
    else:
        return SavedDataset(**result._asdict())


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
            sex, global_sample_size, status, description, pub_id, publication, study_id, created_at) VALUES(:id, :name, 
            :data_source_type, :data_type, :genome_build, :ancestry, :data_contributor, :data_contributor_email, 
            :data_submitter, :data_submitter_email, :sex, :global_sample_size, :status, :description, :pub_id,
            :publication, :study_id, NOW())
        """), sql_params)
        conn.commit()
        return dataset_id


def update_dataset(engine, data: SavedDataset):
    with engine.connect() as conn:
        sql_params = data.dict()
        sql_params.update({'id': str(data.id).replace('-', '')})
        conn.execute(text("""
            UPDATE datasets SET name = :name, data_source_type = :data_source_type, data_type = :data_type, 
            genome_build = :genome_build, ancestry = :ancestry, data_contributor = :data_contributor,
            data_contributor_email = :data_contributor_email, data_submitter = :data_submitter, 
            data_submitter_email = :data_submitter_email, sex = :sex, global_sample_size = :global_sample_size, 
            status = :status, description = :description, pub_id = :pub_id, publication = :publication, 
            study_id = :study_id where id = :id
        """), sql_params)
        conn.execute(text("""delete from dataset_phenotypes where dataset_id = :id"""), sql_params)
        conn.commit()


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
        return pd_id


def insert_credible_set(engine, phenotype_dataset_id: str, s3_path: str, name: str):
    with engine.connect() as conn:
        credible_set_id = str(uuid.uuid4()).replace('-', '')
        sql_params = {'id': credible_set_id, 'phenotype_data_set_id': phenotype_dataset_id, 's3_path': s3_path,
                      'name': name}
        conn.execute(text("""
            INSERT INTO credible_sets (id, phenotype_data_set_id, s3_path, name, created_at) 
            VALUES(:id, :phenotype_data_set_id, :s3_path, :name, NOW())"""), sql_params)
        conn.commit()
        return credible_set_id


def get_study_for_dataset(engine, study_id: str) -> SavedStudy:
    with engine.connect() as conn:
        result = conn.execute(text("""
                SELECT id, name, institution, created_at 
                FROM studies where id = :id
            """), {'id': study_id}).first()
        if result is None:
            raise ValueError(f"No records for id {study_id}")
        else:
            return SavedStudy(**result._asdict())


def get_phenotypes_for_dataset(engine, dataset_id: uuid.UUID) -> SavedPhenotypeDataSet:
    with engine.connect() as conn:
        results = conn.execute(text("""SELECT id, phenotype, dichotomous, sample_size, cases, controls, created_at 
                FROM dataset_phenotypes where dataset_id = :id
            """), {'id': str(dataset_id).replace('-', '')})
        if results is None:
            raise ValueError(f"No records for id {dataset_id}")
        else:
            return [SavedPhenotypeDataSet(**row._asdict()) for row in results]


def delete_dataset(engine, data_set_id):
    with engine.connect() as conn:
        no_dash_id = str(data_set_id).replace('-', '')
        conn.execute(text("""
            DELETE FROM credible_sets where phenotype_data_set_id in 
            ( select id from dataset_phenotypes where dataset_id = :id)
        """), {'id': no_dash_id})
        conn.execute(text("""
            DELETE FROM dataset_phenotypes where dataset_id = :id
        """), {'id': no_dash_id})
        conn.execute(text("""
            DELETE FROM datasets where id = :id
        """), {'id': no_dash_id})
        conn.commit()


def get_credible_sets_for_dataset(engine, phenotype_ids: list) -> list:
    if len(phenotype_ids) == 0:
        return []
    with engine.connect() as conn:
        params = {'ids': tuple([str(p_id).replace('-', '') for p_id in phenotype_ids])}
        results = conn.execute(text("""SELECT id, phenotype_data_set_id, name, s3_path, created_at 
                FROM credible_sets where phenotype_data_set_id in :ids
            """), params)
        if results is None:
            raise ValueError(f"No records for id {phenotype_ids}")
        else:
            return [SavedCredibleSet(**row._asdict()) for row in results]
