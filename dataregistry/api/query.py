import datetime
import json
import re
import uuid
from functools import lru_cache
from typing import Optional, List

import bcrypt
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from dataregistry.api.model import SavedDataset, DataSet, Study, SavedStudy, SavedPhenotypeDataSet, SavedCredibleSet, \
    CsvBioIndexRequest, SavedCsvBioIndexRequest, User, FileUpload, NewUserRequest, HermesUser
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


def save_file_upload_info(engine, dataset, metadata, s3_path, filename, file_size, uploader) -> str:
    with engine.connect() as conn:
        new_guid = str(uuid.uuid4())
        conn.execute(text("""INSERT INTO file_uploads(id, dataset, file_name, file_size, uploaded_at, uploaded_by,
        metadata, s3_path, qc_status) VALUES(:id, :dataset, :file_name, :file_size, NOW(), :uploaded_by, :metadata,
         :s3_path, 'SUBMITTED TO QC')"""), {'id': new_guid.replace('-', ''), 'dataset': dataset,
                                            'file_name': filename,
                                            'file_size': file_size, 'uploaded_by': uploader,
                                            'metadata': json.dumps(metadata), 's3_path': s3_path})
        conn.commit()
        return new_guid


def gen_fetch_ds_sql(params, param_to_where):
    sql = "select id, dataset as dataset_name, file_name, file_size, uploaded_at, uploaded_by, qc_status, " \
          "qc_log, metadata->>'$.phenotype' as phenotype, metadata, s3_path from file_uploads "

    for index, (col, value) in enumerate(params.items(), start=0):

        if index == 0:
            sql += f"WHERE {param_to_where.get(col)} "
        else:
            if col in {"limit", "offset"}:
                break
            else:
                sql += f" AND {param_to_where.get(col)} "

    sql += " order by uploaded_at desc"
    return f"{sql} {param_to_where.get('limit', '')} {param_to_where.get('offset', '')}".rstrip()


def fetch_file_uploads(engine, statuses=None, limit=None, offset=None, phenotype=None, uploader=None) -> (
        List)[FileUpload]:
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
        conn.execute(text("UPDATE file_uploads set qc_log=:qc_log, qc_status = :qc_status where id = :file_upload_id"),
                     {'qc_log': qc_log, 'qc_status': qc_status,
                      'file_upload_id': file_upload_id.replace('-', '')})
        conn.commit()


def fetch_file_upload(engine, file_id) -> FileUpload:
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT id, dataset as dataset_name, file_name, file_size, uploaded_at, uploaded_by, metadata, "
                 "s3_path, qc_log, metadata->>'$.phenotype' as phenotype, qc_status "
                 "FROM file_uploads WHERE id = :file_id"),
            {'file_id': file_id}).first()

        if result is None:
            return None

        result_dict = result._asdict()

        if result_dict['metadata'] is not None:
            result_dict['metadata'] = json.loads(result_dict['metadata'])

        return FileUpload(**result_dict)


def get_file_owner(engine, file_id):
    with engine.connect() as conn:
        result = conn.execute(text("select uploaded_by from file_uploads where id = :file_id"),
                              {'file_id': str(file_id).replace('-', '')}).fetchone()
    return result[0] if result else None


def update_file_qc_status(engine, file_id, qc_status):
    with engine.connect() as conn:
        conn.execute(text("UPDATE file_uploads set qc_status = :qc_status where id = :file_id"),
                     {'qc_status': qc_status, 'file_id': str(file_id).replace('-', '')})
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
