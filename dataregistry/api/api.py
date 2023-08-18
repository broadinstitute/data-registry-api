import json
import logging
import os
from uuid import UUID

import fastapi
import requests
import sqlalchemy
import xmltodict
from botocore.exceptions import ClientError
from fastapi import UploadFile, Depends
from fastapi.encoders import jsonable_encoder
from starlette.requests import Request
from starlette.responses import StreamingResponse, Response

from dataregistry.api import query, s3
from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api.jwt import get_encoded_cookie_data, get_decoded_cookie_data
from dataregistry.api.model import DataSet, Study, SavedDatasetInfo, SavedDataset, UserCredentials, User
from dataregistry.pub_ids import PubIdType, infer_id_type

AUTH_COOKIE_NAME = 'dr_auth_token'

router = fastapi.APIRouter()

# get root logger
logger = logging.getLogger(__name__)
# connect to database
engine = DataRegistryReadWriteDB().get_engine()

logger.info("Starting API")
NIH_API_EMAIL = "dhite@broadinstitute.org"
NIH_API_TOOL_NAME = "data-registry"


@router.get('/datasets', response_class=fastapi.responses.ORJSONResponse)
async def api_datasets():
    try:
        return query.get_all_datasets(engine)
    except ValueError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))


@router.get('/datasets/{index}', response_class=fastapi.responses.ORJSONResponse)
async def api_datasets(index: UUID):
    try:
        ds = query.get_dataset(engine, index)
        study = query.get_study_for_dataset(engine, ds.study_id)
        phenotypes = query.get_phenotypes_for_dataset(engine, index)
        credible_sets = query.get_credible_sets_for_dataset(engine, [phenotype.id for phenotype in phenotypes])
        return SavedDatasetInfo(
            **{'dataset': ds, 'study': study, 'phenotypes': phenotypes, 'credible_sets': credible_sets})
    except KeyError:
        raise fastapi.HTTPException(status_code=400, detail=f'Invalid index: {index}')
    except ValueError as e:
        raise fastapi.HTTPException(status_code=404, detail=str(e))


def format_authors(author_list):
    if len(author_list) < 2:
        return f"{author_list[0].get('LastName', '')} {author_list[0].get('Initials', '')}"
    else:
        result = ''
        for author in author_list[0:2]:
            result += f"{author.get('LastName', '')} {author.get('Initials', '')}, "
        if len(author_list) > 2:
            return result + 'et al.'
        else:
            return result[:-2]


def get_elocation_id(article_meta):
    eloc_dict_list = article_meta.get('ELocationID')
    if not eloc_dict_list:
        return None
    if isinstance(eloc_dict_list, list):
        eloc_dict_list = eloc_dict_list[0]
    return f"{eloc_dict_list.get('@EIdType')}: {eloc_dict_list.get('#text')}"


@router.get('/publications', response_class=fastapi.responses.ORJSONResponse)
async def api_publications(pub_id: str):
    if infer_id_type(pub_id) != PubIdType.PMID:
        http_res = requests.get(f'https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/?ids={pub_id}'
                                f'&format=json&email={NIH_API_EMAIL}&tool={NIH_API_TOOL_NAME}')
        if http_res.status_code != 200:
            raise fastapi.HTTPException(status_code=404, detail=f'Invalid publication id: {pub_id}')
        pub_id = json.loads(http_res.text)['records'][0]['pmid']

    http_res = requests.get(f'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id={pub_id}')
    if http_res.status_code != 200:
        raise fastapi.HTTPException(status_code=404, detail=f'Could not locate title and abstract for: {pub_id}')
    xml_doc = xmltodict.parse(http_res.text)
    article_meta = xml_doc['PubmedArticleSet']['PubmedArticle']['MedlineCitation']['Article']
    publication = article_meta.get('Journal').get('Title')
    authors = format_authors(article_meta.get('AuthorList').get('Author'))
    volume_issue = f"{article_meta.get('Journal').get('JournalIssue').get('Volume')}({article_meta.get('Journal').get('JournalIssue').get('Issue')})"
    pages = article_meta.get('Pagination').get('MedlinePgn')

    month_year_published = f"{article_meta.get('Journal').get('JournalIssue').get('PubDate').get('Year')} {article_meta.get('Journal').get('JournalIssue').get('PubDate').get('Month')}"

    return {"title": article_meta.get('ArticleTitle', ''), "publication": publication,
            'month_year_published': month_year_published, 'authors': authors, 'volume_issue': volume_issue,
            'pages': pages, 'elocation_id': get_elocation_id(article_meta)}


@router.post("/uploadfile/{data_set_id}/{phenotype}/{dichotomous}/{sample_size}")
async def upload_file_for_phenotype(data_set_id: str, phenotype: str, dichotomous: bool, file: UploadFile,
                                    sample_size: int, response: fastapi.Response, cases: int = None,
                                    controls: int = None):
    filename = file.filename
    try:
        saved_dataset = query.get_dataset(engine, UUID(data_set_id))
        file_path = f"{saved_dataset.name}/{phenotype}"
        file_size = await multipart_upload_to_s3(file, file_path)
        pd_id = query.insert_phenotype_data_set(engine, data_set_id, phenotype,
                                                f"s3://{s3.BASE_BUCKET}/{file_path}/{filename}", dichotomous,
                                                sample_size, cases, controls, filename, file_size)
        return {"message": f"Successfully uploaded {filename}", "phenotype_data_set_id": pd_id}
    except Exception as e:
        logger.exception("There was a problem uploading file", e)
        response.status_code = 400
        return {"message": f"There was an error uploading the file {filename}"}
    finally:
        await file.close()


@router.get("/filelist/{data_set_id}")
async def get_file_list(data_set_id: str):
    try:
        ds_uuid = UUID(data_set_id)
        ds = query.get_dataset(engine, ds_uuid)
        if not ds.publicly_available:
            raise fastapi.HTTPException(status_code=403, detail=f'{data_set_id} is not publicly available')
    except ValueError:
        raise fastapi.HTTPException(status_code=404, detail=f'Invalid index: {data_set_id}')
    return get_possible_files(ds_uuid)


@router.get("/files/{file_id}/{phenotype}/{file_type}/{file_name}", name="stream_file")
async def stream_file(phenotype: str, file_id: str, file_type: str, file_name: str):
    no_dash_id = file_id.replace('-', '')
    try:
        if file_type == "credible-set":
            s3_path = query.get_credible_set_file(engine, no_dash_id)
        elif file_type == "data":
            s3_path = query.get_phenotype_file(engine, no_dash_id)
        else:
            raise fastapi.HTTPException(status_code=404, detail=f'Invalid file type: {file_type}')
    except ValueError:
        raise fastapi.HTTPException(status_code=404, detail=f'Invalid file: {file_id}')

    obj = s3.get_file_obj(s3_path.replace(f's3://{s3.BASE_BUCKET}/', ''))

    def generator():
        for chunk in iter(lambda: obj['Body'].read(4096), b''):
            yield chunk

    return StreamingResponse(generator(), media_type='application/octet-stream')


def get_possible_files(ds_uuid):
    available_files = []
    phenos = query.get_phenotypes_for_dataset(engine, ds_uuid)
    available_files.extend(
        [{"path": f"files/{str(pheno.id).replace('-', '')}/{pheno.phenotype}/data/{pheno.file_name}",
          "phenotype": pheno.phenotype, "name": pheno.file_name,
          "size": f"{round(pheno.file_size / (1024 * 1024), 2)} mb",
          "type": "data", "createdAt": pheno.created_at.strftime("%Y-%m-%d")}
         for pheno in phenos])

    if phenos:
        credible_sets = query.get_credible_sets_for_dataset(engine, [pheno.id for pheno in phenos])
        available_files.extend(
            [{"path": f"files/{str(cs.id).replace('-', '')}/{cs.phenotype}/credible-set/{cs.file_name}",
              "phenotype": cs.phenotype, "name": cs.file_name,
              "size": f"{round(cs.file_size / (1024 * 1024), 2)} mb", "type": "credible set",
              "createdAt": cs.created_at.strftime("%Y-%m-%d")}
             for cs in credible_sets])

    return available_files


@router.post("/crediblesetupload/{phenotype_data_set_id}/{credible_set_name}")
async def upload_credible_set_for_phenotype(phenotype_data_set_id: str, credible_set_name: str,
                                            file: UploadFile, response: fastapi.Response):
    try:
        file_path = f"credible_sets/{phenotype_data_set_id}"
        file_size = await multipart_upload_to_s3(file, file_path)
        cs_id = query.insert_credible_set(engine, phenotype_data_set_id,
                                          f"s3://{s3.BASE_BUCKET}/{file_path}/{file.filename}", credible_set_name,
                                          file.filename, file_size)
    except Exception as e:
        logger.exception("There was a problem uploading file", e)
        response.status_code = 400
        return {"message": f"There was an error uploading the file {file.filename}"}
    finally:
        await file.close()

    return {"message": f"Successfully uploaded {file.filename}", "credible_set_id": cs_id}


@router.delete("/datasets/{data_set_id}")
async def delete_dataset(data_set_id: str, response: fastapi.Response):
    try:
        query.delete_dataset(engine, data_set_id)
    except Exception as e:
        logger.exception("There was a problem deleting dataset", e)
        response.status_code = 400
        return {"message": f"There was an error deleting the dataset {data_set_id}"}

    return {"message": f"Successfully deleted dataset {data_set_id}"}


@router.delete("/phenotypes/{phenotype_data_set_id}")
async def delete_phenotype(phenotype_data_set_id: str, response: fastapi.Response):
    try:
        query.delete_phenotype(engine, phenotype_data_set_id)
    except Exception as e:
        logger.exception("There was a problem deleting phenotype", e)
        response.status_code = 400
        return {"message": f"There was an error deleting the phenotype {phenotype_data_set_id}"}

    return {"message": f"Successfully deleted phenotype {phenotype_data_set_id}"}


async def multipart_upload_to_s3(file, file_path):
    upload = s3.initiate_multi_part(file_path, file.filename)
    part_number = 1
    parts = []
    size = 0
    # read and put 50 mb at a time--is that too small?
    while contents := await file.read(1024 * 1024 * 50):
        size += len(contents)
        upload_part_response = s3.put_bytes(file_path, file.filename, contents, upload, part_number)
        parts.append({
            'PartNumber': part_number,
            'ETag': upload_part_response['ETag']
        })
        part_number = part_number + 1
    s3.finalize_upload(file_path, file.filename, parts, upload)
    return size


@router.post('/studies', response_class=fastapi.responses.ORJSONResponse)
async def save_study(req: Study):
    study_id = query.insert_study(engine, req)
    return {
        'name': req.name,
        'study_id': study_id
    }


@router.get('/studies', response_class=fastapi.responses.ORJSONResponse)
async def get_studies():
    return query.get_studies(engine)


@router.post('/datasets', response_class=fastapi.responses.ORJSONResponse)
async def save_dataset(req: DataSet):
    """
    The body of the request contains the information to insert into the records db
    """
    try:
        dataset_id = query.insert_dataset(engine, req)

        return {
            'name': req.name,
            'dataset_id': dataset_id
        }
    except sqlalchemy.exc.IntegrityError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))
    except ClientError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))


@router.patch('/datasets', response_class=fastapi.responses.ORJSONResponse)
async def update_dataset(req: SavedDataset):
    """
    The body of the request contains the information to insert into the records db
    """
    try:
        query.update_dataset(engine, req)

        return fastapi.responses.Response(content=None, status_code=200)
    except sqlalchemy.exc.IntegrityError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))
    except ClientError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))


@router.delete('/records/{index}', response_class=fastapi.responses.ORJSONResponse)
async def api_record_delete(index: int):
    """
    Soft delete both the database (by setting the `deleted` field to the current timestamp)
    And s3 (by adding a file called _DELETED in which to identify deleted buckets from the CLI)
    """
    try:
        s3_record_id = query.delete_record(engine, index)

        return {
            's3_record_id': s3_record_id
        }
    except sqlalchemy.exc.IntegrityError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))
    except ClientError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))


async def get_current_user(request: Request):
    auth = request.cookies.get(AUTH_COOKIE_NAME)
    logger.info(f"Auth cookie: {auth}")
    if not auth:
        raise fastapi.HTTPException(status_code=401, detail='Not logged in')
    data = get_decoded_cookie_data(auth)
    if not data:
        raise fastapi.HTTPException(status_code=401, detail='Not logged in')
    user = User(**data)
    return user


@router.get('/is-logged-in')
def is_logged_in(user: User = Depends(get_current_user)):
    if user:
        return user
    else:
        raise fastapi.HTTPException(status_code=401, detail='Not logged in')


def is_drupal_user(creds):
    response = requests.post(f"{os.getenv('DRUPAL_HOST')}/user/login?_format=json", data=json.dumps({
        'name': creds.email,
        'pass': creds.password
    }), headers={'Content-Type': 'application/json'})
    return response.status_code == 200


@router.post('/login')
def login(request: Request, response: Response, creds: UserCredentials):
    in_list, user = is_user_in_list(creds)
    if not in_list and not is_drupal_user(creds):
        raise fastapi.HTTPException(status_code=401, detail='Invalid username or password')
    domain_from_request = request.headers.get("host", "").split(":")[0]
    response.set_cookie(key=AUTH_COOKIE_NAME, value=get_encoded_cookie_data(user if user else
                                                                           User(name=creds.email, email=creds.email,
                                                                                role='user')),
                        domain='.kpndataregistry.org' if 'kpndataregistry' in domain_from_request else '',
                        samesite='lax')
    return {'status': 'success'}


def is_user_in_list(creds: UserCredentials):
    user = next((user for user in get_users() if user.email == creds.email), None)
    return user is not None and 'password' == creds.password, user


def get_users() -> list:
    return [User(name='admin', email='admin@kpnteam.org', role='admin'),
            User(name='user', email='user@kpnteam.org', role='user')]


@router.post('/logout')
def logout(response: Response):
    response.delete_cookie(key=AUTH_COOKIE_NAME)
    return {'status': 'success'}
