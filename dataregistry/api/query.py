import datetime
import json
import re
import uuid
from functools import lru_cache
from typing import Optional, List, Tuple, Any, Union

import bcrypt
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from dataregistry.api.model import SavedDataset, DataSet, Study, SavedStudy, SavedPhenotypeDataSet, SavedCredibleSet, \
    CsvBioIndexRequest, SavedCsvBioIndexRequest, User, FileUpload, NewUserRequest, HermesUser, MetaAnalysisRequest, \
    HermesMetaAnalysisStatus, SavedMetaAnalysisRequest, HermesPhenotype, SGCPhenotype, SGCGWASFile, CALRFile
from dataregistry.id_shortener import shorten_uuid


def get_all_datasets(engine) -> list:
    with engine.connect() as conn:
        results = conn.execute(text("""select id, name, data_source_type, data_type, genome_build, ancestry, sex,
        global_sample_size, status, data_submitter, data_submitter_email, data_contributor, data_contributor_email,
        study_id, description, pub_id, publication, created_at, publicly_available from datasets"""))
    return [SavedDataset(**row._asdict()) for row in results]


def get_all_datasets_for_user(engine, user: User) -> list:
    with engine.connect() as conn:
        results = conn.execute(text("""select id, name, data_source_type, data_type, genome_build, ancestry, sex,
        global_sample_size, status, data_submitter, data_submitter_email, data_contributor, data_contributor_email,
        study_id, description, pub_id, publication, created_at, publicly_available
        from datasets where user_id = :user_id"""), {'user_id': user.id})
    return [SavedDataset(**row._asdict()) for row in results]


def get_bioindex_schema(engine, dataset_id: str) -> str:
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT `schema` FROM `__Indexes` WHERE name= :id
            """), {'id': dataset_id}).first()
    return result.schema if result is not None else None


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


def insert_dataset(engine, data: DataSet, user_id: int):
    with engine.connect() as conn:
        sql_params = data.dict()
        dataset_id = str(uuid.uuid4()).replace('-', '')
        sql_params.update({'id': dataset_id, 'user_id': user_id})
        conn.execute(text("""
            INSERT INTO datasets (id, name, data_source_type, data_type, genome_build,
            ancestry, data_contributor, data_contributor_email, data_submitter, data_submitter_email,
            sex, global_sample_size, status, description, pub_id, publication, study_id, created_at,
            publicly_available, user_id)
            VALUES(:id, :name, :data_source_type, :data_type, :genome_build, :ancestry, :data_contributor,
            :data_contributor_email, :data_submitter, :data_submitter_email, :sex, :global_sample_size, :status,
            :description, :pub_id, :publication, :study_id, NOW(), :publicly_available, :user_id)
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
            study_id = :study_id, publicly_available = :publicly_available where id = :id
        """), sql_params)
        conn.commit()


def insert_phenotype_data_set(engine, dataset_id: str, phenotype: str, s3_path: str, dichotomous: bool,
                              sample_size: int, cases: int, controls: int, file_name: str, file_size: int):
    with engine.connect() as conn:
        pd_id = str(uuid.uuid4()).replace('-', '')
        sql_params = {'id': pd_id, 'dataset_id': dataset_id, 's3_path': s3_path, 'phenotype': phenotype,
                      'dichotomous': dichotomous, 'sample_size': sample_size, 'cases': cases, 'file_name': file_name,
                      'controls': controls, 'file_size': file_size
                      }
        conn.execute(text("""
            INSERT INTO dataset_phenotypes (id, dataset_id, phenotype, s3_path, dichotomous, sample_size, cases,
            created_at, file_name, controls, file_size)
            VALUES(:id, :dataset_id, :phenotype, :s3_path, :dichotomous, :sample_size, :cases, NOW(), :file_name,
            :controls, :file_size)"""), sql_params)
        save_shortened_file_id(conn, pd_id, 'd')
        conn.commit()
        return pd_id


def insert_credible_set(engine, phenotype_dataset_id: str, s3_path: str, name: str, file_name: str, file_size: int):
    with engine.connect() as conn:
        credible_set_id = str(uuid.uuid4()).replace('-', '')
        sql_params = {'id': credible_set_id, 'phenotype_data_set_id': phenotype_dataset_id, 's3_path': s3_path,
                      'name': name, 'file_name': file_name, 'file_size': file_size}
        conn.execute(text("""
            INSERT INTO credible_sets (id, phenotype_data_set_id, s3_path, name, file_name, file_size, created_at)
            VALUES(:id, :phenotype_data_set_id, :s3_path, :name, :file_name, :file_size, NOW())"""), sql_params)
        save_shortened_file_id(conn, credible_set_id, 'cs')
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


def get_phenotypes_for_dataset(engine, dataset_id: uuid.UUID) -> list:
    with engine.connect() as conn:
        results = conn.execute(text("""SELECT ds.id, ds.dataset_id, ds.phenotype, ds.dichotomous, ds.sample_size, ds.cases,
        ds.controls, ds.created_at, ds.file_name, ds.s3_path, ds.file_size, df.short_id
        FROM dataset_phenotypes ds join data_file_ids df on df.id = ds.id where dataset_id = :id
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


def delete_phenotype(engine, phenotype_id):
    with engine.connect() as conn:
        no_dash_id = str(phenotype_id).replace('-', '')
        conn.execute(text("""
            DELETE FROM credible_sets where phenotype_data_set_id = :id
        """), {'id': no_dash_id})
        conn.execute(text("""
            DELETE FROM dataset_phenotypes where id = :id
        """), {'id': no_dash_id})
        conn.commit()


def get_credible_set_file(engine, credible_set_id: str) -> str:
    with engine.connect() as conn:
        result = conn.execute(text("""SELECT cs.s3_path FROM credible_sets cs
        join dataset_phenotypes p on cs.phenotype_data_set_id = p.id
        join datasets d on p.dataset_id = d.id
        where cs.id = :id and d.publicly_available = true
            """), {'id': credible_set_id}).first()
        if result is None:
            raise ValueError(f"No records for id {credible_set_id}")
        else:
            return result.s3_path


def get_phenotype_file(engine, phenotype_id: str) -> str:
    with engine.connect() as conn:
        result = conn.execute(text("""SELECT p.s3_path FROM dataset_phenotypes p
        join datasets d on p.dataset_id = d.id where p.id = :id and d.publicly_available = true
            """), {'id': phenotype_id}).first()
        if result is None:
            raise ValueError(f"No records for id {phenotype_id}")
        else:
            return result.s3_path


def get_dataset_id_for_phenotype(engine, phenotype_data_set_id: str) -> Optional[str]:
    with engine.connect() as conn:
        result = conn.execute(text("""SELECT dataset_id FROM dataset_phenotypes where id = :id"""),
                              {'id': phenotype_data_set_id}).fetchone()
        if result:
            return result[0].decode('UTF-8')
        else:
            return None


def get_credible_sets_for_dataset(engine, phenotype_ids: list) -> list:
    if len(phenotype_ids) == 0:
        return []
    with engine.connect() as conn:
        params = {'ids': tuple([str(p_id).replace('-', '') for p_id in phenotype_ids])}
        results = conn.execute(text("""SELECT cs.id, cs.phenotype_data_set_id, cs.name, cs.s3_path, cs.file_name,
        cs.created_at, cs.file_size, p.phenotype, cfi.short_id FROM credible_sets cs join dataset_phenotypes p
        on cs.phenotype_data_set_id = p.id join cs_file_ids cfi on cfi.id = cs.id where phenotype_data_set_id in :ids
            """), params)
        if results is None:
            raise ValueError(f"No records for id {phenotype_ids}")
        else:
            return [SavedCredibleSet(**row._asdict()) for row in results]


def save_shortened_file_id(conn, file_id: str, file_type: str):
    short_id = shorten_uuid(file_id)
    if file_type == 'd':
        conn.execute(text("""
            INSERT INTO data_file_ids (id, short_id)
            VALUES (:full, :short)
        """), {'full': file_id, 'short': short_id})
    elif file_type == 'cs':
        conn.execute(text("""
            INSERT INTO cs_file_ids (id, short_id)
            VALUES (:full, :short)
        """), {'full': file_id, 'short': short_id})
    return short_id


def shortened_file_id_lookup(short_file_id: str, file_type: str, engine) -> str:
    with engine.connect() as conn:
        result = None
        if file_type == 'd':
            result = conn.execute(text("""SELECT id  FROM data_file_ids where short_id = :id
            """), {'id': short_file_id}).first()
        elif file_type == 'cs':
            result = conn.execute(text("""SELECT id  FROM cs_file_ids where short_id = :id
            """), {'id': short_file_id}).first()
        if result is None:
            raise ValueError(f"No records for id {short_file_id}")
        else:
            return result.id


def add_bioindex_tracking(engine, request: CsvBioIndexRequest):
    idx_id = uuid.uuid4()
    str_id = str(idx_id).replace('-', '')
    with engine.connect() as conn:
        sql_params = {'name': str_id, 'status': request.status, 'column': request.column,
                      'already_sorted': request.already_sorted, 's3_path': request.s3_path}
        conn.execute(text("""INSERT INTO bidx_tracking (name, status, `column`, already_sorted, s3_path, created_at)
            VALUES(:name, :status, :column, :already_sorted, :s3_path, NOW())"""), sql_params)
        conn.commit()
        return idx_id


def get_bioindex_tracking(engine, req_id) -> SavedCsvBioIndexRequest:
    with engine.connect() as conn:
        params = {'name': str(req_id).replace('-', '')}
        result = conn.execute(text("""SELECT name, status, `column`, already_sorted, s3_path, created_at, ip_address
        from bidx_tracking where name = :name"""), params).first()
    if result is None:
        raise ValueError(f"No records for id {req_id}")
    else:
        return SavedCsvBioIndexRequest(**result._asdict())


def update_bioindex_tracking(engine, req_id, new_status):
    with engine.connect() as conn:
        params = {'name': str(req_id).replace('-', ''), 'status': new_status}
        conn.execute(text("""UPDATE bidx_tracking SET status = :status where name = :name"""), params)
        conn.commit()


def get_user(engine, creds) -> Optional[User]:
    with engine.connect() as conn:
        params = {'user_name': creds.user_name}
        if creds.password is not None:
            return get_internal_user_info(conn, creds, params)
        else:
            return get_user_info(conn, params)


def get_internal_user_info(conn, creds, params) -> Optional[User]:
    result = conn.execute(text("SELECT password FROM users WHERE user_name = :user_name and oauth_provider is null"),
                          params).fetchone()

    if result and bcrypt.checkpw(creds.password.encode('utf-8'), result[0].encode('utf-8')):
        return get_user_info(conn, params)
    else:
        return None


def get_user_info(conn, params) -> Optional[User]:
    result = conn.execute(text("SELECT u.id, u.user_name, u.first_name, u.last_name, u.email, u.avatar, u.is_active, "
                               "u.last_login, r.role, p.permission, g.group_name as `group`, "
                               "(oauth_provider IS NULL) AS is_internal FROM users u "
                               "LEFT JOIN user_roles ur on ur.user_id = u.id "
                               "LEFT JOIN roles r on ur.role_id = r.id "
                               "LEFT JOIN role_permissions rp ON rp.role_id = r.id "
                               "LEFT JOIN permissions p on p.id = rp.permission_id "
                               "LEFT JOIN user_groups ug on ug.user_id = u.id "
                               "LEFT JOIN `groups` g on ug.group_id = g.id "
                               "WHERE u.user_name = :user_name"), params).mappings().all()
    if not result:
        return None

    return User(**process_user_roles_permissions(result))


def process_user_roles_permissions(result):
    user_dict = {}
    roles = set()
    permissions = set()
    groups = set()

    for row in result:
        if not user_dict:
            user_dict = {
                'id': row['id'],
                'user_name': row['user_name'],
                'first_name': row['first_name'],
                'last_name': row['last_name'],
                'email': row['email'],
                'avatar': row['avatar'],
                'is_active': row['is_active'],
                'last_login': row['last_login'],
                'is_internal': row['is_internal'],
                'roles': [],
                'permissions': [],
                'groups': []
            }
        if row['role'] and row['role'] not in roles:
            roles.add(row['role'])
            user_dict['roles'].append(row['role'])
        if row['permission'] and row['permission'] not in permissions:
            permissions.add(row['permission'])
            user_dict['permissions'].append(row['permission'])
        if row['group'] and row['group'] not in groups:
            groups.add(row['group'])
            user_dict['groups'].append(row['group'])

    return user_dict


def log_user_in(engine, user):
    with engine.connect() as conn:
        conn.execute(text("UPDATE users SET last_login = NOW() where user_name = :user_name"),
                     {'user_name': user.user_name})
        conn.commit()


def update_password(engine, new_password: str, user: User):
    with engine.connect() as conn:
        conn.execute(text("UPDATE users SET password = :password WHERE user_name = :user_name"),
                     {'password': bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt()),
                      'user_name': user.user_name})
        conn.commit()


def get_data_set_owner(engine, ds_id):
    with engine.connect() as conn:
        result = conn.execute(text("select user_id from datasets where id = :ds_id"),
                              {'ds_id': ds_id.replace('-', '')}).fetchone()
        return result[0] if result else None

def retrieve_meta_data_mapping(engine, user: str) -> [dict]:
    with engine.connect() as conn:
        if user == 'dhite@broadinstitute.org':
            results = conn.execute(text("""select dataset, metadata from file_uploads"""))
        else:
            results = conn.execute(text("""select dataset, metadata from file_uploads where uploaded_by = :user_name
            """), {'user_name': user})

        return {row.dataset: json.loads(row.metadata) for row in results}

def save_file_upload_info(engine, dataset, metadata, s3_path, filename, file_size, uploader, qc_script_options) -> str:
    with engine.connect() as conn:
        new_guid = str(uuid.uuid4())
        conn.execute(text("""INSERT INTO file_uploads(id, dataset, file_name, file_size, uploaded_at, uploaded_by,
        metadata, s3_path, qc_script_options, qc_status) VALUES(:id, :dataset, :file_name, :file_size, NOW(), :uploaded_by, :metadata,
         :s3_path, :qc_script_options, 'SUBMITTED TO QC')"""), {'id': new_guid.replace('-', ''), 'dataset': dataset,
                                            'file_name': filename,
                                            'file_size': file_size, 'uploaded_by': uploader,
                                            'metadata': json.dumps(metadata), 's3_path': s3_path,
                                            'qc_script_options': json.dumps(qc_script_options)})
        conn.commit()
        return new_guid

def update_file_qc_options(engine, file_id: str, qc_script_options: dict):
    with engine.connect() as conn:
        conn.execute(text("""UPDATE file_uploads 
                           SET qc_script_options = :qc_script_options,
                               qc_status = 'SUBMITTED TO QC'
                           WHERE id = :file_id"""),
                    {'qc_script_options': json.dumps(qc_script_options),
                     'file_id': str(file_id).replace('-', '')})
        conn.commit()


def gen_fetch_ds_sql(params, param_to_where):
    sql = "select id, dataset as dataset_name, file_name, file_size, uploaded_at, uploaded_by, qc_status, " \
          "qc_log, metadata->>'$.phenotype' as phenotype, metadata->>'$.ancestry' as ancestry, metadata, s3_path from file_uploads "

    for index, (col, value) in enumerate(params.items(), start=0):
        if col in {"limit", "offset"}:
            break
        if index == 0:
            sql += f"WHERE {param_to_where.get(col)} "
        else:
            sql += f" AND {param_to_where.get(col)} "

    sql += " order by uploaded_at desc"
    return f"{sql} {param_to_where.get('limit', '')} {param_to_where.get('offset', '')}".rstrip()


def fetch_file_uploads(engine, statuses=None, limit=None, offset=None, phenotype=None,
                       uploader=None) -> List[FileUpload]:
    with engine.connect() as conn:
        sql, params = get_file_upload_sql_and_params(limit, offset, phenotype, statuses, uploader)
        results = conn.execute(text(sql), params)
        file_uploads = []
        for row in results:
            row_dict = row._asdict()
            if row_dict['metadata'] is not None:
                row_dict['metadata'] = json.loads(row_dict['metadata'])
            file_uploads.append(FileUpload(**row_dict))
        return file_uploads


def get_file_upload_sql_and_params(limit, offset, phenotype, statuses, uploader):
    param_to_sql = {}
    params = {}
    if statuses:
        params['qc_status'] = statuses
        param_to_sql['qc_status'] = "qc_status in :qc_status"
    if phenotype:
        params['phenotype'] = phenotype
        param_to_sql['phenotype'] = "metadata->>'$.phenotype' = :phenotype"
    if uploader:
        params['uploaded_by'] = uploader
        param_to_sql['uploaded_by'] = "uploaded_by = :uploaded_by"
    if limit:
        params['limit'] = limit
        param_to_sql['limit'] = "limit :limit"
    if offset:
        params['offset'] = offset
        param_to_sql['limit'] = "offset :offset"
    sql = gen_fetch_ds_sql(params, param_to_sql)
    return sql, params


def update_file_upload_qc_log(engine, qc_log: str, file_upload_id: str, qc_status: str):
    with engine.connect() as conn:
        conn.execute(text("""UPDATE file_uploads 
                           SET qc_log=:qc_log, qc_status=:qc_status, qc_job_completed_at=NOW()
                           WHERE id=:file_upload_id"""),
                    {'qc_log': qc_log, 'qc_status': qc_status, 'file_upload_id': file_upload_id.replace('-', '')})
        conn.commit()


def record_qc_job_submission_time(engine, file_upload_id: str):
    with engine.connect() as conn:
        conn.execute(text("UPDATE file_uploads SET qc_job_submitted_at=NOW() WHERE id=:file_upload_id"),
                     {'file_upload_id': file_upload_id.replace('-', '')})
        conn.commit()




def record_meta_analysis_job_submission_time(engine, meta_analysis_id: str):
    """Record when a meta-analysis job is submitted to AWS Batch."""
    with engine.connect() as conn:
        conn.execute(text("UPDATE meta_analyses SET job_submitted_at=NOW() WHERE id=:meta_analysis_id"),
                     {'meta_analysis_id': meta_analysis_id.replace('-', '')})
        conn.commit()


def update_meta_analysis_log(engine, log: str, meta_analysis_id: str, status: str):
    """Update meta-analysis log and mark job as completed."""
    with engine.connect() as conn:
        conn.execute(text("UPDATE meta_analyses set log=:log, status = :status, job_completed_at=NOW() where id = :meta_analysis_id"),
                     {'log': log, 'status': status,
                      'meta_analysis_id': meta_analysis_id.replace('-', '')})
        conn.commit()


def fetch_file_upload(engine, file_id) -> Union[FileUpload, None]:
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT id, dataset as dataset_name, file_name, file_size, uploaded_at, uploaded_by, metadata, "
                 "s3_path, qc_log, metadata->>'$.phenotype' as phenotype, qc_status, qc_script_options "
                 "FROM file_uploads WHERE id = :file_id"),
            {'file_id': file_id}).first()

        if result is None:
            return None

        result_dict = result._asdict()

        if result_dict['metadata'] is not None:
            result_dict['metadata'] = json.loads(result_dict['metadata'])

        if result_dict['qc_script_options'] is not None:
            result_dict['qc_script_options'] = json.loads(result_dict['qc_script_options'])

        return FileUpload(**result_dict)


def get_file_owner(engine, file_id):
    with engine.connect() as conn:
        result = conn.execute(text("select uploaded_by from file_uploads where id = :file_id"),
                              {'file_id': str(file_id).replace('-', '')}).fetchone()
    return result[0] if result else None


def save_mskkp_dataset(engine, dataset_id: str, name: str, metadata: dict, s3_path: str, 
                       filename: str, file_size: int, uploader: str) -> str:
    """Save MSKKP dataset information to the database."""
    status = 'uploaded' if file_size > 0 else 'pending'
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO mskkp_datasets(
                id, name, phenotype, ancestry, genome_build, effective_n,
                file_name, file_size, s3_path, uploaded_at, uploaded_by,
                column_map, metadata, status
            ) VALUES(
                :id, :name, :phenotype, :ancestry, :genome_build, :effective_n,
                :file_name, :file_size, :s3_path, NOW(), :uploaded_by,
                :column_map, :metadata, :status
            )
        """), {
            'id': dataset_id.replace('-', ''),
            'name': name,
            'phenotype': metadata.get('phenotype'),
            'ancestry': metadata['ancestry'],
            'genome_build': metadata['genome_build'],
            'effective_n': metadata.get('effective_n'),
            'file_name': filename,
            'file_size': file_size,
            's3_path': s3_path,
            'uploaded_by': uploader,
            'column_map': json.dumps(metadata['column_map']),
            'metadata': json.dumps(metadata),
            'status': status
        })
        conn.commit()
    return dataset_id


def fetch_mskkp_dataset_by_id(engine, dataset_id: str):
    """Fetch an MSKKP dataset by ID."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT id, name, phenotype, ancestry, genome_build, effective_n,
                   file_name, file_size, s3_path, uploaded_at, uploaded_by, 
                   column_map, metadata, status
            FROM mskkp_datasets
            WHERE id = :id
        """), {'id': dataset_id.replace('-', '')}).first()
        
        if result:
            row_dict = result._asdict()
            # Parse JSON fields
            if row_dict.get('column_map'):
                row_dict['column_map'] = json.loads(row_dict['column_map'])
            if row_dict.get('metadata'):
                row_dict['metadata'] = json.loads(row_dict['metadata'])
            return row_dict
        return None


def update_mskkp_dataset_file_info(engine, dataset_id: str, s3_path: str, filename: str, file_size: int):
    """Update file information for an MSKKP dataset after upload."""
    with engine.connect() as conn:
        conn.execute(text("""
            UPDATE mskkp_datasets
            SET file_name = :filename, 
                file_size = :file_size, 
                s3_path = :s3_path,
                status = 'uploaded'
            WHERE id = :id
        """), {
            'filename': filename,
            'file_size': file_size,
            's3_path': s3_path,
            'id': dataset_id.replace('-', '')
        })
        conn.commit()


def update_file_qc_status(engine, file_id, qc_status):
    with engine.connect() as conn:
        conn.execute(text("UPDATE file_uploads set qc_status = :qc_status where id = :file_id"),
                     {'qc_status': qc_status, 'file_id': str(file_id).replace('-', '')})
        conn.commit()

def update_file_metadata(engine, file_id, metadata):
    with engine.connect() as conn:
        conn.execute(text("UPDATE file_uploads set metadata = :metadata where id = :file_id"),
                     {'metadata': json.dumps(metadata), 'file_id': str(file_id).replace('-', '')})
        conn.commit()


def fetch_used_phenotypes(engine, statuses) -> List[str]:
    with engine.connect() as conn:
        if statuses:
            result = conn.execute(text("SELECT distinct metadata->>'$.phenotype' as phenotype from file_uploads "
                                       "where qc_status in :statuses"), {'statuses': statuses})
        else:
            result = conn.execute(text("SELECT distinct metadata->>'$.phenotype' as phenotype from file_uploads"))
        return [row[0] for row in result]


def add_new_hermes_user(engine, user: NewUserRequest):
    role_map, group_map = get_role_and_group_maps(engine)
    with engine.connect() as conn:
        try:
            result = conn.execute(text("INSERT INTO users (user_name, email, password, created_at) "
                                       "values(:user_name, :email, :password, NOW())"),
                                  {'user_name': user.user_name, 'email': user.user_name,
                                   'password': bcrypt.hashpw(user.password.encode('utf-8'), bcrypt.gensalt())})
            new_user_id = result.lastrowid
            conn.execute(text("INSERT INTO user_groups (user_id, group_id) values (:user_id, :group_id)"),
                         {'user_id': new_user_id, 'group_id': group_map.get('hermes')})
            conn.execute(text("INSERT INTO user_roles (user_id, role_id) values (:new_user_id, :role_id)"),
                         {'new_user_id': new_user_id, 'role_id': role_map.get(user.user_type)})
            conn.commit()
        except IntegrityError:
            raise ValueError("User already exists")


@lru_cache(maxsize=None)
def get_role_and_group_maps(engine):
    with engine.connect() as conn:
        roles = conn.execute(text("SELECT role, id as role_id FROM roles")).fetchall()
        role_map = {role: role_id for role, role_id in roles}

        groups = conn.execute(text("SELECT group_name, id as group_id FROM `groups`")).fetchall()
        group_map = {group_name: group_id for group_name, group_id in groups}

    return role_map, group_map


def get_hermes_users(engine):
    with engine.connect() as conn:
        result = conn.execute(text("SELECT u.id, user_name, created_at, last_login, is_active, r.role from users u "
                                   "join user_groups ug on u.id = ug.user_id join `groups` g on g.id = ug.group_id "
                                   "join user_roles ur on ur.user_id = u.id join roles r on ur.role_id = r.id "
                                   "where g.group_name = 'hermes'"))
        return [HermesUser(**row._asdict()) for row in result]


def save_meta_analysis(engine, req: MetaAnalysisRequest):
    with engine.connect() as conn:
        new_guid = str(uuid.uuid4())
        insert_main = """
            INSERT INTO meta_analyses (id, name, phenotype, status, created_at, method, created_by) VALUES (:id, :name, 
            :phenotype, :status, NOW(), :method, :created_by)
        """
        insert_ds = """
            INSERT INTO meta_analysis_datasets(dataset_id, meta_analysis_id) VALUES (:dataset_id, :meta_analysis_id)
        """
        conn.execute(text(insert_main), {'id': new_guid.replace('-', ''), 'name': req.name,
                                         'phenotype': req.phenotype, 'status': HermesMetaAnalysisStatus.SUBMITTED,
                                         'method': req.method, 'created_by': req.created_by})
        for ds in req.datasets:
            conn.execute(text(insert_ds), {'dataset_id': str(ds).replace('-', ''),
                                           'meta_analysis_id': new_guid.replace('-', '')})
        conn.commit()
        return new_guid


def get_meta_analyses(engine):
    with engine.connect() as conn:
        sql = """
            select ma.id, ma.name, ma.phenotype, ma.status, ma.method, ma.created_at, ma.created_by, 
            group_concat(fu.dataset) as dataset_names, group_concat(mad.dataset_id) as datasets 
            from meta_analyses ma join meta_analysis_datasets mad on ma.id = mad.meta_analysis_id 
            join file_uploads fu on fu.id = mad.dataset_id group by ma.id
        """
        result = conn.execute(text(sql)).mappings().all()
        return [
            SavedMetaAnalysisRequest(
                id=row['id'],
                name=row['name'],
                phenotype=row['phenotype'],
                status=row['status'],
                method=row['method'],
                created_at=row['created_at'],
                created_by=row['created_by'],
                datasets=[uuid.UUID(hex=x) for x in row['datasets'].decode('utf-8').split(',') if x],
                dataset_names=row['dataset_names'].split(',')
            ) for row in result
        ]


def get_path_for_ds(engine, ds) -> str:
    with engine.connect() as conn:
        result = conn.execute(text("select s3_path from file_uploads where id = :id"),
                              {'id': str(ds).replace('-', '')}).first()

        return result[0]


def get_name_ancestry_for_ds(engine, ds) -> Tuple[str, str]:
    with engine.connect() as conn:
        result = conn.execute(text("select dataset, metadata->>'$.ancestry' as ancestry from file_uploads "
                                   "where id = :id"), {'id': str(ds).replace('-', '')}).first()
        return result[0], result[1]


def save_phenotype(engine, phenotype, dichotomous):
    with engine.connect() as conn:
        conn.execute(text("INSERT INTO Phenotypes (name, dichotomous) VALUES (:phenotype, :dichotomous) "
                          "ON DUPLICATE KEY UPDATE name = VALUES(name)"), {'phenotype': phenotype,
                                                                           'dichotomous': dichotomous})
        conn.commit()


def save_dataset_name(engine, ds_name, ancestry):
    with engine.connect() as conn:
        conn.execute(text("INSERT INTO Datasets (name, ancestry) VALUES (:dataset, :ancestry) "
                          "ON DUPLICATE KEY UPDATE name = VALUES(name)"),
                     {'dataset': ds_name, 'ancestry': ancestry})
        conn.commit()

def delete_hermes_dataset(engine, ds_id):
    no_hyphens = str(ds_id).replace('-', '')
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM meta_analysis_datasets WHERE dataset_id = :ds_id"), {'ds_id': no_hyphens})
        conn.execute(text("DELETE FROM meta_analyses WHERE id NOT IN (SELECT DISTINCT meta_analysis_id "
                          "FROM meta_analysis_datasets)"), {})
        conn.execute(text("DELETE FROM file_uploads WHERE id = :ds_id"), {'ds_id': no_hyphens})
        conn.commit()


def get_meta_analysis(engine, ma_id: uuid.UUID) -> SavedMetaAnalysisRequest:
    with engine.connect() as conn:
        sql = """
            select ma.id, ma.name, ma.phenotype, ma.status, ma.method, ma.created_at, ma.created_by, 
            group_concat(fu.dataset) as dataset_names, ma.log, group_concat(mad.dataset_id) as datasets 
            from meta_analyses ma join meta_analysis_datasets mad on ma.id = mad.meta_analysis_id 
            join file_uploads fu on fu.id = mad.dataset_id where ma.id = :id group by ma.id
        """
        result = conn.execute(text(sql), {'id': str(ma_id).replace('-', '')}).mappings().first()
        return SavedMetaAnalysisRequest(
            id=result['id'],
            name=result['name'],
            phenotype=result['phenotype'],
            status=result['status'],
            method=result['method'],
            created_at=result['created_at'],
            created_by=result['created_by'],
            log=result['log'],
            datasets=[uuid.UUID(hex=x) for x in result['datasets'].decode('utf-8').split(',') if x],
            dataset_names=result['dataset_names'].split(',')
        )


def get_hermes_phenotypes(engine) -> List[HermesPhenotype]:
    with engine.connect() as conn:
        result = conn.execute(text("SELECT name, description, dichotomous FROM hermes_phenotype")).mappings().all()
        return [HermesPhenotype(**row) for row in result]


def get_sgc_phenotypes(engine) -> List[SGCPhenotype]:
    """Get all SGC phenotypes from the database."""
    with engine.connect() as conn:
        result = conn.execute(text("SELECT phenotype_code, description, created_at FROM sgc_phenotypes")).mappings().all()
        return [SGCPhenotype(**row) for row in result]


def delete_sgc_phenotype(engine, phenotype_code: str) -> bool:
    """Delete an SGC phenotype by phenotype_code. Returns True if deleted, False if not found."""
    with engine.connect() as conn:
        result = conn.execute(text("DELETE FROM sgc_phenotypes WHERE phenotype_code = :phenotype_code"), 
                            {'phenotype_code': phenotype_code})
        conn.commit()
        return result.rowcount > 0


def insert_sgc_phenotype(engine, phenotype_code: str, description: str):
    """Insert a new SGC phenotype. Raises IntegrityError if phenotype_code already exists."""
    with engine.connect() as conn:
        conn.execute(text("INSERT INTO sgc_phenotypes (phenotype_code, description) VALUES (:phenotype_code, :description)"),
                    {'phenotype_code': phenotype_code, 'description': description})
        conn.commit()


def upsert_sgc_cohort(engine, cohort) -> str:
    """
    Upsert (insert or update) an SGC cohort record.
    - If cohort.id is provided, UPDATE existing record
    - If cohort.id is None, INSERT new record (will error on duplicate name/uploaded_by)
    Takes an SGCCohort object and returns the cohort ID (as hex string without dashes).
    """
    import json
    
    with engine.connect() as conn:
        cohort_metadata_json = json.dumps(cohort.cohort_metadata) if cohort.cohort_metadata else None
        
        if cohort.id:
            # Update existing cohort
            cohort_id = str(cohort.id).replace('-', '')
            conn.execute(text("""
                UPDATE sgc_cohorts 
                SET name = :name, total_sample_size = :total_sample_size, 
                    number_of_males = :number_of_males, number_of_females = :number_of_females,
                    cohort_metadata = :cohort_metadata, validation_status = :validation_status, updated_at = CURRENT_TIMESTAMP
                WHERE id = :id 
            """), {
                'id': cohort_id,
                'name': cohort.name,
                'total_sample_size': cohort.total_sample_size,
                'number_of_males': cohort.number_of_males,
                'number_of_females': cohort.number_of_females,
                'cohort_metadata': cohort_metadata_json,
                'validation_status': cohort.validation_status if cohort.validation_status is not None else False
            })
        else:
            # Insert new cohort
            cohort_id = str(uuid.uuid4()).replace('-', '')
            conn.execute(text("""
                INSERT INTO sgc_cohorts (id, name, uploaded_by, total_sample_size, number_of_males, number_of_females, cohort_metadata, validation_status)
                VALUES (:id, :name, :uploaded_by, :total_sample_size, :number_of_males, :number_of_females, :cohort_metadata, :validation_status)
            """), {
                'id': cohort_id,
                'name': cohort.name,
                'uploaded_by': cohort.uploaded_by,
                'total_sample_size': cohort.total_sample_size,
                'number_of_males': cohort.number_of_males,
                'number_of_females': cohort.number_of_females,
                'cohort_metadata': cohort_metadata_json,
                'validation_status': cohort.validation_status if cohort.validation_status is not None else False
            })
        
        conn.commit()
        return cohort_id


def insert_sgc_cohort_file(engine, cohort_file) -> str:
    """
    Insert a new SGC cohort file record. 
    Takes an SGCCohortFile object and returns the file ID (as hex string without dashes).
    Will raise IntegrityError if cohort_id + file_type already exists.
    """
    with engine.connect() as conn:
        # Generate ID if not provided
        file_id = str(cohort_file.id).replace('-', '') if cohort_file.id else str(uuid.uuid4()).replace('-', '')
        cohort_id_hex = str(cohort_file.cohort_id).replace('-', '')
        
        import json
        column_mapping_json = json.dumps(cohort_file.column_mapping) if cohort_file.column_mapping else None
        
        conn.execute(text("""
            INSERT INTO sgc_cohort_files (id, cohort_id, file_type, file_path, file_name, file_size, column_mapping)
            VALUES (:id, :cohort_id, :file_type, :file_path, :file_name, :file_size, :column_mapping)
        """), {
            'id': file_id,
            'cohort_id': cohort_id_hex,
            'file_type': cohort_file.file_type,
            'file_path': cohort_file.file_path,
            'file_name': cohort_file.file_name,
            'file_size': cohort_file.file_size,
            'column_mapping': column_mapping_json
        })
        conn.commit()
        return file_id


def get_sgc_cohort_files(engine, cohort_id: str):
    """Get all files for a specific SGC cohort."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT id, cohort_id, file_type, file_path, file_name, file_size, uploaded_at
            FROM sgc_cohort_files 
            WHERE cohort_id = :cohort_id
        """), {'cohort_id': cohort_id}).mappings().all()
        return [dict(row) for row in result]


def get_sgc_cohort_by_id(engine, cohort_id: str):
    """Get a single SGC cohort by ID with its associated files."""
    import json
    
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 
                c.id as cohort_id, c.name, c.uploaded_by, c.total_sample_size, 
                c.number_of_males, c.number_of_females, c.cohort_metadata, c.validation_status, c.created_at, c.updated_at,
                f.id as file_id, f.file_type, f.file_path, f.file_name, 
                f.file_size, f.column_mapping, f.uploaded_at as file_uploaded_at
            FROM sgc_cohorts c
            LEFT JOIN sgc_cohort_files f ON c.id = f.cohort_id
            WHERE c.id = :cohort_id
            ORDER BY f.file_type
        """), {'cohort_id': cohort_id}).mappings().all()
        
        if result:
            # Parse JSON metadata for each row
            parsed_result = []
            for row in result:
                row_dict = dict(row)
                if row_dict['cohort_metadata']:
                    row_dict['cohort_metadata'] = json.loads(row_dict['cohort_metadata'])
                if row_dict['column_mapping']:
                    row_dict['column_mapping'] = json.loads(row_dict['column_mapping'])
                parsed_result.append(row_dict)
            return parsed_result
        return None


def get_sgc_cohort_file_owner(engine, file_id: str) -> str:
    """Get the owner (uploaded_by) of an SGC cohort file."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT c.uploaded_by 
            FROM sgc_cohort_files f
            JOIN sgc_cohorts c ON f.cohort_id = c.id
            WHERE f.id = :file_id
        """), {'file_id': file_id}).first()
        return result[0] if result else None


def delete_sgc_cohort_file(engine, file_id: str) -> bool:
    """Delete an SGC cohort file by file_id. Returns True if deleted, False if not found."""
    with engine.connect() as conn:
        result = conn.execute(text("DELETE FROM sgc_cohort_files WHERE id = :file_id"), 
                            {'file_id': file_id})
        conn.commit()
        return result.rowcount > 0


def get_sgc_cohort_file_by_id(engine, file_id: str):
    """Get a single SGC cohort file by ID."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT id, cohort_id, file_type, file_path, file_name, file_size, uploaded_at
            FROM sgc_cohort_files 
            WHERE id = :file_id
        """), {'file_id': file_id}).mappings().first()
        return dict(result) if result else None


def get_sgc_cohorts_with_files(engine, uploaded_by: str = None):
    """
    Get SGC cohorts with their associated files using a LEFT JOIN.
    If uploaded_by is provided, filter by that user. Otherwise return all cohorts.
    Returns a list of cohorts with their files nested.
    """
    import json
    
    with engine.connect() as conn:
        query = """
            SELECT 
                c.id as cohort_id, c.name, c.uploaded_by, c.total_sample_size, 
                c.number_of_males, c.number_of_females, c.cohort_metadata, c.validation_status, c.created_at, c.updated_at,
                f.id as file_id, f.file_type, f.file_path, f.file_name, 
                f.file_size, f.uploaded_at as file_uploaded_at
            FROM sgc_cohorts c
            LEFT JOIN sgc_cohort_files f ON c.id = f.cohort_id
        """
        
        params = {}
        if uploaded_by:
            query += " WHERE c.uploaded_by = :uploaded_by"
            params['uploaded_by'] = uploaded_by
            
        query += " ORDER BY c.created_at DESC, f.file_type"
        
        result = conn.execute(text(query), params).mappings().all()
        
        # Group results by cohort
        cohorts_dict = {}
        for row in result:
            cohort_id = row['cohort_id']
            
            if cohort_id not in cohorts_dict:
                # Parse JSON metadata
                cohort_metadata = None
                if row['cohort_metadata']:
                    cohort_metadata = json.loads(row['cohort_metadata'])
                    
                cohorts_dict[cohort_id] = {
                    'id': row['cohort_id'],
                    'name': row['name'],
                    'uploaded_by': row['uploaded_by'],
                    'total_sample_size': row['total_sample_size'],
                    'number_of_males': row['number_of_males'],
                    'number_of_females': row['number_of_females'],
                    'cohort_metadata': cohort_metadata,
                    'validation_status': row['validation_status'],
                    'created_at': row['created_at'],
                    'updated_at': row['updated_at'],
                    'files': []
                }
            
            # Add file if it exists (LEFT JOIN may have NULL files)
            if row['file_id']:
                cohorts_dict[cohort_id]['files'].append({
                    'id': row['file_id'],
                    'cohort_id': row['cohort_id'],
                    'file_type': row['file_type'],
                    'file_path': row['file_path'],
                    'file_name': row['file_name'],
                    'file_size': row['file_size'],
                    'uploaded_at': row['file_uploaded_at']
                })
        
        # Add computed status based on file count
        for cohort in cohorts_dict.values():
            file_count = len(cohort['files'])
            cohort['status'] = 'complete' if file_count == 3 else 'incomplete'
        
        return list(cohorts_dict.values())


def update_sgc_cohort_validation_status(engine, cohort_id: str, validation_status: bool) -> bool:
    """Update the validation status of an SGC cohort. Returns True if updated, False if not found."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            UPDATE sgc_cohorts 
            SET validation_status = :validation_status, updated_at = CURRENT_TIMESTAMP
            WHERE id = :cohort_id
        """), {
            'cohort_id': cohort_id,
            'validation_status': validation_status
        })
        conn.commit()
        return result.rowcount > 0


def delete_sgc_cohort(engine, cohort_id: str) -> bool:
    """Delete an SGC cohort. Returns True if deleted, False if not found."""
    with engine.connect() as conn:
        result = conn.execute(text("DELETE FROM sgc_cohorts WHERE id = :cohort_id"), 
                            {'cohort_id': cohort_id})
        conn.commit()
        return result.rowcount > 0


def insert_sgc_cases_controls_metadata(engine, metadata) -> str:
    """
    Insert SGC cases/controls metadata record.
    Takes an SGCCasesControlsMetadata object and returns the metadata ID (as hex string without dashes).
    """
    import uuid
    import json
    
    with engine.connect() as conn:
        # Generate ID if not provided
        metadata_id = str(metadata.id).replace('-', '') if metadata.id else str(uuid.uuid4()).replace('-', '')
        file_id_hex = str(metadata.file_id).replace('-', '')
        
        conn.execute(text("""
            INSERT INTO sgc_cases_controls_metadata (id, file_id, distinct_phenotypes, total_cases, total_controls, phenotype_counts)
            VALUES (:id, :file_id, :distinct_phenotypes, :total_cases, :total_controls, :phenotype_counts)
        """), {
            'id': metadata_id,
            'file_id': file_id_hex,
            'distinct_phenotypes': json.dumps(metadata.distinct_phenotypes),
            'total_cases': metadata.total_cases,
            'total_controls': metadata.total_controls,
            'phenotype_counts': json.dumps(metadata.phenotype_counts)
        })
        conn.commit()
        return metadata_id


def get_sgc_cases_controls_metadata(engine, file_id: str):
    """Get SGC cases/controls metadata by file ID."""
    import json
    
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT id, file_id, distinct_phenotypes, total_cases, total_controls, phenotype_counts, created_at
            FROM sgc_cases_controls_metadata
            WHERE file_id = :file_id
        """), {"file_id": file_id}).fetchone()

        if result:
            # Handle backward compatibility for phenotype_counts field
            phenotype_counts = {}
            if result[5]:  # phenotype_counts field
                try:
                    phenotype_counts = json.loads(result[5])
                except (json.JSONDecodeError, TypeError):
                    phenotype_counts = {}

            return {
                'id': result[0],
                'file_id': result[1],
                'distinct_phenotypes': json.loads(result[2]),
                'total_cases': result[3],
                'total_controls': result[4],
                'phenotype_counts': phenotype_counts,
                'created_at': result[6]
            }
        return None


def delete_sgc_cases_controls_metadata(engine, file_id: str) -> bool:
    """Delete SGC cases/controls metadata by file ID. Returns True if deleted, False if not found."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            DELETE FROM sgc_cases_controls_metadata WHERE file_id = :file_id
        """), {"file_id": file_id})
        conn.commit()
        return result.rowcount > 0


def insert_sgc_cooccurrence_metadata(engine, metadata) -> str:
    """
    Insert SGC co-occurrence metadata record.
    Takes an SGCCoOccurrenceMetadata object and returns the metadata ID (as hex string without dashes).
    """
    import uuid
    import json
    
    with engine.connect() as conn:
        # Generate ID if not provided
        metadata_id = str(metadata.id).replace('-', '') if metadata.id else str(uuid.uuid4()).replace('-', '')
        file_id_hex = str(metadata.file_id).replace('-', '')
        
        conn.execute(text("""
            INSERT INTO sgc_cooccurrence_metadata (id, file_id, distinct_phenotypes, total_pairs, total_cooccurrence_count, phenotype_pair_counts)
            VALUES (:id, :file_id, :distinct_phenotypes, :total_pairs, :total_cooccurrence_count, :phenotype_pair_counts)
        """), {
            'id': metadata_id,
            'file_id': file_id_hex,
            'distinct_phenotypes': json.dumps(metadata.distinct_phenotypes),
            'total_pairs': metadata.total_pairs,
            'total_cooccurrence_count': metadata.total_cooccurrence_count,
            'phenotype_pair_counts': json.dumps(metadata.phenotype_pair_counts)
        })
        conn.commit()
        return metadata_id


def get_sgc_cooccurrence_metadata(engine, file_id: str):
    """Get SGC co-occurrence metadata by file ID."""
    import json
    
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT id, file_id, distinct_phenotypes, total_pairs, total_cooccurrence_count, phenotype_pair_counts, created_at
            FROM sgc_cooccurrence_metadata
            WHERE file_id = :file_id
        """), {"file_id": file_id}).fetchone()

        if result:
            # Handle backward compatibility for phenotype_pair_counts field
            phenotype_pair_counts = {}
            if result[5]:  # phenotype_pair_counts field
                try:
                    phenotype_pair_counts = json.loads(result[5])
                except (json.JSONDecodeError, TypeError):
                    phenotype_pair_counts = {}

            return {
                'id': result[0],
                'file_id': result[1],
                'distinct_phenotypes': json.loads(result[2]),
                'total_pairs': result[3],
                'total_cooccurrence_count': result[4],
                'phenotype_pair_counts': phenotype_pair_counts,
                'created_at': result[6]
            }
        return None


def delete_sgc_cooccurrence_metadata(engine, file_id: str) -> bool:
    """Delete SGC co-occurrence metadata by file ID. Returns True if deleted, False if not found."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            DELETE FROM sgc_cooccurrence_metadata WHERE file_id = :file_id
        """), {"file_id": file_id})
        conn.commit()
        return result.rowcount > 0


def get_sgc_phenotype_case_totals(engine):
    """
    Get total cases and controls across all SGC cohorts by phenotype.
    Returns a list of phenotype statistics aggregated from all cohorts.
    """
    import json
    
    with engine.connect() as conn:
        # First, get all the phenotype_counts data
        query = """
            SELECT 
                c.id as cohort_id,
                c.name as cohort_name,
                ccm.phenotype_counts
            FROM sgc_cohorts c
            JOIN sgc_cohort_files cf ON c.id = cf.cohort_id 
            JOIN sgc_cases_controls_metadata ccm ON cf.id = ccm.file_id
            WHERE cf.file_type = 'cases_controls_both'
            AND ccm.phenotype_counts IS NOT NULL
        """
        
        result = conn.execute(text(query)).mappings().all()
        
        # Process the data in Python since JSON operations are complex in MySQL
        phenotype_totals = {}
        
        for row in result:
            cohort_id = row['cohort_id']
            cohort_name = row['cohort_name']
            
            try:
                phenotype_counts = json.loads(row['phenotype_counts'])
                
                # Iterate through each phenotype in this cohort
                for phenotype_code, counts in phenotype_counts.items():
                    if isinstance(counts, dict) and 'cases' in counts:
                        cases = counts.get('cases', 0)
                        controls = counts.get('controls', 0)
                        
                        # Initialize if this is the first time we see this phenotype
                        if phenotype_code not in phenotype_totals:
                            phenotype_totals[phenotype_code] = {
                                'phenotype_code': phenotype_code,
                                'total_cases_across_cohorts': 0,
                                'total_controls_across_cohorts': 0,
                                'cohorts': set()
                            }
                        
                        # Add to totals
                        if cases is not None:
                            phenotype_totals[phenotype_code]['total_cases_across_cohorts'] += int(cases)
                        if controls is not None:
                            phenotype_totals[phenotype_code]['total_controls_across_cohorts'] += int(controls)
                        
                        phenotype_totals[phenotype_code]['cohorts'].add(cohort_id)
                        
            except (json.JSONDecodeError, TypeError) as e:
                # Skip malformed JSON data
                print(f"Error parsing phenotype_counts for cohort {cohort_id}: {e}")
                continue
        
        # Convert to final format
        results = []
        for phenotype_code, data in phenotype_totals.items():
            results.append({
                'phenotype_code': data['phenotype_code'],
                'total_cases_across_cohorts': data['total_cases_across_cohorts'],
                'total_controls_across_cohorts': data['total_controls_across_cohorts'],
                'num_cohorts': len(data['cohorts'])
            })
        
        # Sort by total cases descending
        results.sort(key=lambda x: x['total_cases_across_cohorts'], reverse=True)
        
        return results


def get_sgc_phenotype_case_counts_by_sex(engine):
    """
    Get case and control counts by phenotype and sex across all SGC cohorts.
    Returns a list of phenotype statistics broken down by sex (male, female, both).
    """
    import json
    
    with engine.connect() as conn:
        # Get phenotype_counts data for all three file types (male, female, both)
        query = """
            SELECT 
                c.id as cohort_id,
                c.name as cohort_name,
                cf.file_type,
                ccm.phenotype_counts
            FROM sgc_cohorts c
            JOIN sgc_cohort_files cf ON c.id = cf.cohort_id 
            JOIN sgc_cases_controls_metadata ccm ON cf.id = ccm.file_id
            WHERE cf.file_type IN ('cases_controls_male', 'cases_controls_female', 'cases_controls_both')
            AND ccm.phenotype_counts IS NOT NULL
        """
        
        result = conn.execute(text(query)).mappings().all()
        
        # Process the data by phenotype and sex
        # Structure: {phenotype_code: {sex: {cases: total, controls: total, cohorts: set}}}
        phenotype_sex_totals = {}
        
        for row in result:
            cohort_id = row['cohort_id']
            file_type = row['file_type']
            
            # Extract sex from file_type (cases_controls_male -> male)
            sex = file_type.replace('cases_controls_', '')
            
            try:
                phenotype_counts = json.loads(row['phenotype_counts'])
                
                # Iterate through each phenotype in this cohort
                for phenotype_code, counts in phenotype_counts.items():
                    if isinstance(counts, dict) and 'cases' in counts:
                        cases = counts.get('cases', 0)
                        controls = counts.get('controls', 0)
                        
                        # Initialize nested structure if needed
                        if phenotype_code not in phenotype_sex_totals:
                            phenotype_sex_totals[phenotype_code] = {}
                        
                        if sex not in phenotype_sex_totals[phenotype_code]:
                            phenotype_sex_totals[phenotype_code][sex] = {
                                'cases': 0,
                                'controls': 0,
                                'cohorts': set()
                            }
                        
                        # Add to totals for this phenotype and sex
                        if cases is not None:
                            phenotype_sex_totals[phenotype_code][sex]['cases'] += int(cases)
                        if controls is not None:
                            phenotype_sex_totals[phenotype_code][sex]['controls'] += int(controls)
                        
                        phenotype_sex_totals[phenotype_code][sex]['cohorts'].add(cohort_id)
                        
            except (json.JSONDecodeError, TypeError) as e:
                # Skip malformed JSON data
                print(f"Error parsing phenotype_counts for cohort {cohort_id}, file_type {file_type}: {e}")
                continue
        
        # Convert to final format
        results = []
        for phenotype_code, sex_data in phenotype_sex_totals.items():
            phenotype_entry = {'phenotype_code': phenotype_code}
            
            # Add data for each sex
            for sex in ['male', 'female', 'both']:
                if sex in sex_data:
                    phenotype_entry[f'{sex}_cases'] = sex_data[sex]['cases']
                    phenotype_entry[f'{sex}_controls'] = sex_data[sex]['controls']
                    phenotype_entry[f'{sex}_num_cohorts'] = len(sex_data[sex]['cohorts'])
                else:
                    # Set to 0 if no data for this sex
                    phenotype_entry[f'{sex}_cases'] = 0
                    phenotype_entry[f'{sex}_controls'] = 0
                    phenotype_entry[f'{sex}_num_cohorts'] = 0
            
            results.append(phenotype_entry)
        
        # Sort by total cases (both sexes) descending
        results.sort(key=lambda x: x['both_cases'], reverse=True)
        
        return results


# ============================================================================
# PEG (Prioritized Evidence Gene) Functions
# ============================================================================

def create_peg_study(engine, name: str, created_by: str, metadata: dict) -> dict:
    """Create a new PEG study. Returns a dict with study ID and accession_id."""
    with engine.connect() as conn:
        study_id = str(uuid.uuid4()).replace('-', '')
        conn.execute(text("""
            INSERT INTO peg_studies (id, name, created_by, metadata, created_at, updated_at)
            VALUES (:id, :name, :created_by, :metadata, NOW(), NOW())
        """), {
            'id': study_id,
            'name': name,
            'created_by': created_by,
            'metadata': json.dumps(metadata)
        })
        conn.commit()
        
        # Fetch the accession_number that was auto-generated
        result = conn.execute(text("""
            SELECT accession_number FROM peg_studies WHERE id = :id
        """), {'id': study_id}).first()
        
        accession_number = result[0]
        accession_id = f"PEGSt{accession_number:05d}"
        
        return {
            'id': study_id,
            'accession_id': accession_id
        }


def get_peg_studies(engine, created_by: Optional[str] = None) -> list:
    """Get PEG studies, optionally filtered by created_by.
    
    Args:
        engine: Database engine
        created_by: Optional username to filter studies. If None, returns all studies.
    """
    with engine.connect() as conn:
        if created_by:
            results = conn.execute(text("""
                SELECT id, name, created_by, metadata, created_at, updated_at, accession_number
                FROM peg_studies
                WHERE created_by = :created_by
                ORDER BY created_at DESC
            """), {'created_by': created_by}).mappings().all()
        else:
            results = conn.execute(text("""
                SELECT id, name, created_by, metadata, created_at, updated_at, accession_number
                FROM peg_studies
                ORDER BY created_at DESC
            """)).mappings().all()
        
        studies = []
        for row in results:
            study = dict(row)
            study['metadata'] = json.loads(row['metadata']) if row['metadata'] else {}
            study['accession_id'] = f"PEGSt{row['accession_number']:05d}"
            del study['accession_number']  # Remove raw accession_number from response
            studies.append(study)
        
        return studies


def get_peg_study(engine, study_id: str) -> Optional[dict]:
    """Get a specific PEG study by ID."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT id, name, created_by, metadata, created_at, updated_at, accession_number
            FROM peg_studies
            WHERE id = :study_id
        """), {'study_id': str(study_id).replace('-', '')}).mappings().first()
        
        if result:
            study = dict(result)
            study['metadata'] = json.loads(result['metadata']) if result['metadata'] else {}
            study['accession_id'] = f"PEGSt{result['accession_number']:05d}"
            del study['accession_number']  # Remove raw accession_number from response
            return study
        return None


def update_peg_study(engine, study_id: str, name: str, metadata: dict):
    """Update a PEG study's metadata."""
    with engine.connect() as conn:
        conn.execute(text("""
            UPDATE peg_studies
            SET name = :name, metadata = :metadata, updated_at = NOW()
            WHERE id = :study_id
        """), {
            'study_id': str(study_id).replace('-', ''),
            'name': name,
            'metadata': json.dumps(metadata)
        })
        conn.commit()


def delete_peg_study(engine, study_id: str):
    """Delete a PEG study and all associated files."""
    with engine.connect() as conn:
        study_id_hex = str(study_id).replace('-', '')
        # Delete files first
        conn.execute(text("DELETE FROM peg_files WHERE study_id = :study_id"), 
                    {'study_id': study_id_hex})
        # Delete study
        conn.execute(text("DELETE FROM peg_studies WHERE id = :study_id"), 
                    {'study_id': study_id_hex})
        conn.commit()


def create_peg_file(engine, study_id: str, file_type: str, file_name: str, 
                    file_path: str, file_size: int) -> str:
    """Create a PEG file record. Returns the file ID (as hex string without dashes)."""
    with engine.connect() as conn:
        file_id = str(uuid.uuid4()).replace('-', '')
        conn.execute(text("""
            INSERT INTO peg_files (id, study_id, file_type, file_name, file_path, file_size, uploaded_at)
            VALUES (:id, :study_id, :file_type, :file_name, :file_path, :file_size, NOW())
        """), {
            'id': file_id,
            'study_id': str(study_id).replace('-', ''),
            'file_type': file_type,
            'file_name': file_name,
            'file_path': file_path,
            'file_size': file_size
        })
        conn.commit()
        return file_id


def get_peg_files(engine, study_id: str) -> list:
    """Get all files for a PEG study."""
    with engine.connect() as conn:
        results = conn.execute(text("""
            SELECT id, study_id, file_type, file_name, file_path, file_size, uploaded_at
            FROM peg_files
            WHERE study_id = :study_id
            ORDER BY uploaded_at DESC
        """), {'study_id': str(study_id).replace('-', '')}).mappings().all()

        return [dict(row) for row in results]


def get_peg_file(engine, file_id: str):
    """Get a single PEG file by ID."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT id, study_id, file_type, file_name, file_path, file_size, uploaded_at
            FROM peg_files
            WHERE id = :file_id
        """), {'file_id': str(file_id).replace('-', '')}).mappings().first()

        return dict(result) if result else None


def delete_peg_file(engine, file_id: str):
    """Delete a PEG file."""
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM peg_files WHERE id = :file_id"), 
                    {'file_id': str(file_id).replace('-', '')})
        conn.commit()


def insert_sgc_gwas_file(engine, gwas_file) -> str:
    with engine.connect() as conn:
        file_id = str(uuid.uuid4()).replace('-', '')
        cohort_id_hex = str(gwas_file.cohort_id).replace('-', '')
        
        column_mapping_json = json.dumps(gwas_file.column_mapping)
        metadata_json = json.dumps(gwas_file.metadata) if gwas_file.metadata else None
        
        conn.execute(text("""
            INSERT INTO sgc_gwas_files 
            (id, cohort_id, dataset, phenotype, ancestry, file_name, file_size, s3_path, 
             uploaded_by, column_mapping, cases, controls, metadata)
            VALUES 
            (:id, :cohort_id, :dataset, :phenotype, :ancestry, :file_name, :file_size, :s3_path,
             :uploaded_by, :column_mapping, :cases, :controls, :metadata)
        """), {
            'id': file_id,
            'cohort_id': cohort_id_hex,
            'dataset': gwas_file.dataset,
            'phenotype': gwas_file.phenotype,
            'ancestry': gwas_file.ancestry,
            'file_name': gwas_file.file_name,
            'file_size': gwas_file.file_size,
            's3_path': gwas_file.s3_path,
            'uploaded_by': gwas_file.uploaded_by,
            'column_mapping': column_mapping_json,
            'cases': gwas_file.cases,
            'controls': gwas_file.controls,
            'metadata': metadata_json
        })
        conn.commit()
        return file_id


def get_sgc_gwas_file_by_id(engine, file_id: str):
    """Get a single SGC GWAS file by ID."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 
                id, cohort_id, dataset, phenotype, ancestry, 
                file_name, file_size, s3_path, uploaded_at, uploaded_by, 
                column_mapping, cases, controls, metadata
            FROM sgc_gwas_files
            WHERE id = :file_id
        """), {'file_id': str(file_id).replace('-', '')}).mappings().first()
        
        if not result:
            return None
        
        row_dict = dict(result)
        # Parse JSON fields
        if row_dict.get('column_mapping'):
            row_dict['column_mapping'] = json.loads(row_dict['column_mapping'])
        if row_dict.get('metadata'):
            row_dict['metadata'] = json.loads(row_dict['metadata'])
        
        return row_dict


def get_all_sgc_gwas_files(engine):
    """Get all GWAS files across all cohorts."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT
                id, cohort_id, dataset, phenotype, ancestry,
                file_name, file_size, s3_path, uploaded_at, uploaded_by,
                column_mapping, cases, controls, metadata
            FROM sgc_gwas_files
            ORDER BY phenotype ASC, ancestry ASC, uploaded_at DESC
        """)).mappings().all()

        # Parse JSON fields
        parsed_results = []
        for row in result:
            row_dict = dict(row)
            if row_dict.get('column_mapping'):
                row_dict['column_mapping'] = json.loads(row_dict['column_mapping'])
            if row_dict.get('metadata'):
                row_dict['metadata'] = json.loads(row_dict['metadata'])
            parsed_results.append(row_dict)

        return parsed_results


def get_sgc_gwas_files_by_cohort(engine, cohort_id: str):
    """Get all GWAS files for a specific SGC cohort."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT
                id, cohort_id, dataset, phenotype, ancestry,
                file_name, file_size, s3_path, uploaded_at, uploaded_by,
                column_mapping, cases, controls, metadata
            FROM sgc_gwas_files
            WHERE cohort_id = :cohort_id
            ORDER BY uploaded_at DESC
        """), {'cohort_id': str(cohort_id).replace('-', '')}).mappings().all()

        # Parse JSON fields
        parsed_results = []
        for row in result:
            row_dict = dict(row)
            if row_dict.get('column_mapping'):
                row_dict['column_mapping'] = json.loads(row_dict['column_mapping'])
            if row_dict.get('metadata'):
                row_dict['metadata'] = json.loads(row_dict['metadata'])
            parsed_results.append(row_dict)

        return parsed_results


# =============================================================================
# CALR Functions
# =============================================================================

def insert_calr_file(engine, calr_file: CALRFile) -> str:
    """Insert a new CALR file record. Returns the file ID."""
    with engine.connect() as conn:
        file_id = str(calr_file.id).replace('-', '') if calr_file.id else str(uuid.uuid4()).replace('-', '')

        conn.execute(text("""
            INSERT INTO calr_files (id, name, file_name, file_size, s3_path, uploaded_by)
            VALUES (:id, :name, :file_name, :file_size, :s3_path, :uploaded_by)
        """), {
            'id': file_id,
            'name': calr_file.name,
            'file_name': calr_file.file_name,
            'file_size': calr_file.file_size,
            's3_path': calr_file.s3_path,
            'uploaded_by': calr_file.uploaded_by
        })
        conn.commit()
        return file_id


def get_calr_files_by_user(engine, uploaded_by: str):
    """Get all CALR files uploaded by a specific user."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT id, name, file_name, file_size, s3_path, uploaded_at, uploaded_by
            FROM calr_files
            WHERE uploaded_by = :uploaded_by
            ORDER BY uploaded_at DESC
        """), {'uploaded_by': uploaded_by}).mappings().all()

        return [dict(row) for row in result]


def get_calr_file_by_id(engine, file_id: str):
    """Get a single CALR file by ID."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT id, name, file_name, file_size, s3_path, uploaded_at, uploaded_by
            FROM calr_files
            WHERE id = :file_id
        """), {'file_id': file_id}).mappings().first()

        return dict(result) if result else None


def delete_calr_file(engine, file_id: str) -> bool:
    """Delete a CALR file by ID. Returns True if deleted, False if not found."""
    with engine.connect() as conn:
        result = conn.execute(text("DELETE FROM calr_files WHERE id = :file_id"),
                            {'file_id': file_id})
        conn.commit()
        return result.rowcount > 0
