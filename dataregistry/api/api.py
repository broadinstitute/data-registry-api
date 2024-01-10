import io
import json
import logging
import os
import subprocess
from datetime import datetime
from uuid import UUID

import fastapi
import requests
import smart_open
import sqlalchemy
import xmltodict
from botocore.exceptions import ClientError
from fastapi import Depends, Body
from fastapi.security import OAuth2AuthorizationCodeBearer
from starlette.background import BackgroundTasks
from starlette.requests import Request
from starlette.responses import StreamingResponse, Response
from streaming_form_data import StreamingFormDataParser
from streaming_form_data.targets import S3Target

from dataregistry.api import query, s3, csv_utils, ecs, bioidx
from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api.google_oauth import get_google_user
from dataregistry.api.jwt import get_encoded_cookie_data, get_decoded_cookie_data
from dataregistry.api.model import DataSet, Study, SavedDatasetInfo, SavedDataset, UserCredentials, User, SavedStudy, \
    CreateBiondexRequest, CsvBioIndexRequest, BioIndexCreationStatus, SavedCsvBioIndexRequest
from dataregistry.pub_ids import PubIdType, infer_id_type

AUTH_COOKIE_NAME = 'dr_auth_token'

router = fastapi.APIRouter()

# get root logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)

logger.addHandler(ch)
# connect to database
engine = DataRegistryReadWriteDB().get_engine()

logger.info("Starting API")
NIH_API_EMAIL = "dhite@broadinstitute.org"
NIH_API_TOOL_NAME = "data-registry"

oauth2_scheme = OAuth2AuthorizationCodeBearer(
    authorizationUrl="https://accounts.google.com/o/oauth2/v2/auth",
    tokenUrl="https://oauth2.googleapis.com/token",
    refreshUrl="https://oauth2.googleapis.com/token",
    scopes={"openid": "OpenID Connect scope", "email": "email scope"},
)


@router.get('/datasets', response_class=fastapi.responses.ORJSONResponse)
async def api_datasets():
    try:
        return query.get_all_datasets(engine)
    except ValueError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))


@router.post('/trackbioindex', response_class=fastapi.responses.ORJSONResponse)
async def track_bioindex(request: CsvBioIndexRequest):
    return {"id": query.add_bioindex_tracking(engine, request)}


@router.get('/trackbioindex/{req_id}', response_class=fastapi.responses.ORJSONResponse)
async def get_bioindex_tracking(req_id):
    return query.get_bioindex_tracking(engine, req_id)


@router.patch('/trackbioindex/{req_id}/{new_status}', response_class=fastapi.responses.ORJSONResponse)
async def update_bioindex_tracking(req_id, new_status: BioIndexCreationStatus):
    return query.update_bioindex_tracking(engine, req_id, new_status)


@router.post('/enqueue-csv-process', response_class=fastapi.responses.ORJSONResponse)
async def enqueue_csv_process(request: SavedCsvBioIndexRequest, background_tasks: BackgroundTasks):
    background_tasks.add_task(ecs.run_ecs_sort_and_convert_job, request.s3_path, request.column,
                              request.data_types, request.already_sorted, request.name)
    query.update_bioindex_tracking(engine, request.name, BioIndexCreationStatus.SUBMITTED_FOR_PROCESSING)
    return {"message": "Successfully enqueued csv processing"}


@router.post('/createbioindex', response_class=fastapi.responses.ORJSONResponse)
async def create_bioindex(request: CreateBiondexRequest):
    dataset = query.get_dataset(engine, request.dataset_id)
    s3_path = f"{dataset.name}/"
    idx_name = str(request.dataset_id)
    bioidx.create_new_bioindex(engine, request.dataset_id, s3_path, request.schema)
    return {"message": f"Successfully created index {idx_name}"}


@router.get('/bioindex/{idx_id}', response_class=fastapi.responses.ORJSONResponse)
async def get_bioindex(idx_id: UUID):
    schema = query.get_bioindex_schema(engine, str(idx_id))
    if schema:
        host = os.getenv('MINI_BIO_INDEX_HOST')
        if not host:
            raise fastapi.HTTPException(status_code=500, detail='No mini bio index host set')
        return {"url": f"{host}/api/bio/query/{idx_id}?q=<query value>", "schema": f"{schema}"}
    return {"message": f"No bioindex found for dataset {idx_id}"}


@router.get('/phenotypefiles', response_class=fastapi.responses.ORJSONResponse)
async def api_phenotype_files():
    try:
        return query.get_all_phenotypes(engine)
    except ValueError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))


@router.post("/preview-delimited-file")
async def preview_files(request: Request):
    head_of_request = b''
    async for chunk in request.stream():
        head_of_request += chunk

    lines = head_of_request.split(b'\n')[:-1]

    sampled_lines, file_name = await sample_file(lines)
    df = await csv_utils.parse_file(sampled_lines, file_name)
    return {"headers": {column: csv_utils.infer_data_type(df[column].dropna().iloc[0]) for column in df.columns}}


async def sample_file(lines):
    sample = ''
    sample_size = min(10, len(lines))
    for line in lines[4:sample_size + 4]:
        if line.decode('utf-8') == '\r':
            break
        sample += line.decode('utf-8') + '\n'
    return io.StringIO(sample), lines[1].decode('utf-8').split(';')[2].split('=')[1].strip().replace("\"", "")


@router.get('/datasets/{dataset_id}', response_class=fastapi.responses.ORJSONResponse)
async def api_datasets(dataset_id: UUID):
    try:
        ds = query.get_dataset(engine, dataset_id)
        study = query.get_study_for_dataset(engine, ds.study_id)
        phenotypes = query.get_phenotypes_for_dataset(engine, dataset_id)
        credible_sets = query.get_credible_sets_for_dataset(engine, [phenotype.id for phenotype in phenotypes])
        return SavedDatasetInfo(
            **{'dataset': ds, 'study': study, 'phenotypes': phenotypes, 'credible_sets': credible_sets})
    except KeyError:
        raise fastapi.HTTPException(status_code=400, detail=f'Invalid index: {dataset_id}')
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


@router.post("/google-login", response_class=fastapi.responses.ORJSONResponse)
async def google_login(response: Response, body: dict = Body(...)):
    user_info = get_google_user(body.get('code'))
    response.set_cookie(key=AUTH_COOKIE_NAME,
                        httponly=True, value=get_encoded_cookie_data(User(name=user_info['email'], email=user_info['email'], role='user')),
                        domain='.kpndataregistry.org', samesite='strict',
                        secure=os.getenv('USE_HTTPS') == 'true', expires=datetime.utcnow())
    return {'status': 'success'}


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


@router.post("/upload-csv")
async def upload_csv(request: Request):
    filename = request.headers.get('Filename')
    parser = StreamingFormDataParser(request.headers)
    parser.register("file", S3Target(s3.get_file_path("bioindex/uploads", filename), mode='wb'))
    file_size = 0
    async for chunk in request.stream():
        file_size += len(chunk)
        parser.data_received(chunk)
    return {"file_size": file_size, "s3_path": s3.get_file_path("bioindex/uploads", filename)}


@router.post("/uploadfile/{data_set_id}/{phenotype}/{dichotomous}/{sample_size}")
async def upload_file_for_phenotype(data_set_id: str, phenotype: str, dichotomous: bool, request: Request,
                                    sample_size: int, response: fastapi.Response, cases: int = None,
                                    controls: int = None):
    filename = request.headers.get('Filename')
    logger.info(f"Uploading file {filename} for phenotype {phenotype} in dataset {data_set_id}")
    try:
        saved_dataset = query.get_dataset(engine, UUID(data_set_id))
        file_path = f"{saved_dataset.name}/{phenotype}"
        parser = StreamingFormDataParser(request.headers)
        parser.register("file", GzipS3Target(s3.get_file_path(file_path, filename), mode='wb'))
        file_size = 0
        async for chunk in request.stream():
            file_size += len(chunk)
            parser.data_received(chunk)
        pd_id = query.insert_phenotype_data_set(engine, data_set_id, phenotype,
                                                f"s3://{s3.BASE_BUCKET}/{file_path}/{filename}", dichotomous,
                                                sample_size, cases, controls, filename, file_size)
        return {"message": f"Successfully uploaded {filename}", "phenotype_data_set_id": pd_id}
    except Exception as e:
        logger.exception("There was a problem uploading file", e)
        response.status_code = 400
        return {"message": f"There was an error uploading the file {filename}"}


@router.post("/savebioindexfile/{data_set_id}/{phenotype}/{dichotomous}/{sample_size}")
async def save_file_for_phenotype(data_set_id: str, phenotype: str, dichotomous: bool, sample_size: int,
                                  response: fastapi.Response, file_size: int, filename: str, file_path: str,
                                  cases: int = None, controls: int = None):
    try:
        pd_id = query.insert_phenotype_data_set(engine, data_set_id, phenotype,
                                                f"s3://dig-analysis-data/{file_path}/{filename}", dichotomous,
                                                sample_size, cases, controls, filename, file_size)
        return {"message": f"Successfully saved {filename}", "phenotype_data_set_id": pd_id}
    except Exception as e:
        logger.exception("There was a saving a bioindex file", e)
        response.status_code = 400
        return {"message": f"There was a saving a bioindex file {filename}"}


@router.get("/filelist/{data_set_id}")
async def get_file_list(data_set_id: str):
    try:
        ds_uuid = UUID(data_set_id)
    except ValueError:
        raise fastapi.HTTPException(status_code=404, detail=f'Invalid index: {data_set_id}')
    return get_possible_files(ds_uuid)


@router.get("/filecontents/{ft}/{file_id}", name="stream_file")
async def get_text_file(file_id: str, ft: str):
    file_name, obj = await get_file_obj(file_id, ft)

    # Read the text file content
    file_content = obj['Body'].read().decode('utf-8')

    # Return a JSON response with file name and content
    return {'file': file_name, 'file-contents': file_content}


async def get_file_obj(file_id, ft):
    no_dash_id = query.shortened_file_id_lookup(file_id, ft, engine)
    try:
        if ft == "cs":
            s3_path = query.get_credible_set_file(engine, no_dash_id)
        elif ft == "d":
            s3_path = query.get_phenotype_file(engine, no_dash_id)
        else:
            raise fastapi.HTTPException(status_code=404, detail=f'Invalid file type: {ft}')
    except ValueError:
        raise fastapi.HTTPException(status_code=404, detail=f'Invalid file: {file_id}')
    split = s3_path[5:].split('/')
    bucket = split[0]
    file_name = split[-1]
    file_path = '/'.join(split[1:])
    obj = s3.get_file_obj(file_path, bucket)
    return file_name, obj


@router.get("/{ft}/{file_id}", name="stream_file")
async def stream_file(file_id: str, ft: str):
    file_name, obj = await get_file_obj(file_id, ft)

    def generator():
        for chunk in iter(lambda: obj['Body'].read(4096), b''):
            yield chunk

    return StreamingResponse(generator(), media_type='application/octet-stream',
                             headers={"Content-Disposition": f"attachment; filename={file_name}"})


def get_possible_files(ds_uuid):
    available_files = []
    phenos = query.get_phenotypes_for_dataset(engine, ds_uuid)
    available_files.extend(
        [{"path": f"d/{pheno.short_id}",
          "phenotype": pheno.phenotype, "name": pheno.file_name,
          "size": f"{round(pheno.file_size / (1024 * 1024), 2)} mb",
          "type": "data", "createdAt": pheno.created_at.strftime("%Y-%m-%d")}
         for pheno in phenos])

    if phenos:
        credible_sets = query.get_credible_sets_for_dataset(engine, [pheno.id for pheno in phenos])
        available_files.extend(
            [{"path": f"cs/{cs.short_id}",
              "phenotype": cs.phenotype, "name": cs.file_name,
              "size": f"{round(cs.file_size / (1024 * 1024), 2)} mb", "type": "credible set",
              "createdAt": cs.created_at.strftime("%Y-%m-%d")}
             for cs in credible_sets])

    return available_files


@router.post("/crediblesetupload/{phenotype_data_set_id}/{credible_set_name}")
async def upload_credible_set_for_phenotype(phenotype_data_set_id: str, credible_set_name: str,
                                            request: Request, response: fastapi.Response):
    filename = request.headers.get('Filename')
    try:
        file_path = f"credible_sets/{phenotype_data_set_id}"
        parser = StreamingFormDataParser(request.headers)
        parser.register("file", GzipS3Target(s3.get_file_path(file_path, filename), mode='wb'))
        file_size = 0
        async for chunk in request.stream():
            file_size += len(chunk)
            parser.data_received(chunk)
        cs_id = query.insert_credible_set(engine, phenotype_data_set_id,
                                          f"s3://{s3.BASE_BUCKET}/{file_path}/{filename}", credible_set_name,
                                          filename, file_size)
    except Exception as e:
        logger.exception("There was a problem uploading file", e)
        response.status_code = 400
        return {"message": f"There was an error uploading the file {filename}"}

    return {"message": f"Successfully uploaded {filename}", "credible_set_id": cs_id}


def get_latest_git_hash():
    return subprocess.getoutput("git rev-parse HEAD")


@router.get("/version")
async def version():
    return {"git_hash": get_latest_git_hash()}


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
        logger.info(f"Uploading part {part_number} of {file.filename}")
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
    return SavedStudy(id=study_id, name=req.name, institution=req.institution, created_at=datetime.now())


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
        return SavedDataset(id=dataset_id, created_at=datetime.now(), **req.dict())
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
def login(response: Response, creds: UserCredentials):
    in_list, user = is_user_in_list(creds)
    if not in_list and not is_drupal_user(creds):
        raise fastapi.HTTPException(status_code=401, detail='Invalid username or password')
    response.set_cookie(key=AUTH_COOKIE_NAME, httponly=True, value=get_encoded_cookie_data(user if user else
                                                                            User(name=creds.email, email=creds.email,
                                                                                 role='user')),
                        domain='.kpndataregistry.org', samesite='strict',
                        secure=os.getenv('USE_HTTPS') == 'true')
    return {'status': 'success'}


def is_user_in_list(creds: UserCredentials):
    user = next((user for user in get_users() if user.email == creds.email), None)
    return user is not None and 'password' == creds.password, user


def get_users() -> list:
    return [User(name='admin', email='admin@kpnteam.org', role='admin'),
            User(name='user', email='user@kpnteam.org', role='user')]


@router.post('/logout')
def logout(response: Response):
    response.delete_cookie(key=AUTH_COOKIE_NAME, domain='.kpndataregistry.org',
                           samesite='strict', secure=os.getenv('USE_HTTPS') == 'true')
    return {'status': 'success'}


class GzipS3Target(S3Target):
    def __init__(self, path, mode='wb', transport_params=None):
        super().__init__(path, mode, transport_params)

    def on_start(self):
        self._fd = smart_open.open(
            self._file_path,
            self._mode,
            compression='disable',
            transport_params=self._transport_params,
        )
