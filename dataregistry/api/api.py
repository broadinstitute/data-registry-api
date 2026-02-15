import io
import json
import logging
import os
import re
import subprocess
from datetime import datetime
from typing import Optional, List
from uuid import UUID

import fastapi
import requests
import smart_open
import sqlalchemy
import xmltodict
from botocore.exceptions import ClientError
from fastapi import Depends, Body, Header, Query, UploadFile
from starlette.background import BackgroundTasks
from starlette.requests import Request
from starlette.responses import StreamingResponse, Response, RedirectResponse
from streaming_form_data import StreamingFormDataParser
from streaming_form_data.targets import S3Target

from dataregistry.api import query, s3, file_utils, ecs, bioidx, batch
from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api.google_oauth import get_google_user
from dataregistry.api.hermes_file_validation import validate_file
from dataregistry.api.jwt_utils import get_encoded_jwt_data, get_decoded_jwt_data
from dataregistry.api.model import DataSet, Study, SavedDatasetInfo, SavedDataset, UserCredentials, User, SavedStudy, \
    CreateBiondexRequest, CsvBioIndexRequest, BioIndexCreationStatus, SavedCsvBioIndexRequest, HermesFileStatus, \
    HermesUploadStatus, NewUserRequest, StartAggregatorRequest, MetaAnalysisRequest, QCHermesFileRequest, \
    QCScriptOptions, HermesPhenotype, FileType
from dataregistry.api.phenotypes import get_phenotypes
from dataregistry.api.validators import HermesValidator

HERMES_VALIDATOR = HermesValidator()
from dataregistry.pub_med import PubIdType, infer_id_type, format_authors, get_elocation_id

SUPER_USER = "admin"
VIEW_ALL_ROLES = {SUPER_USER, 'analyst', 'reviewer'}

AUTH_COOKIE_NAME = 'dr_auth_token'
AGGREGATOR_API_SECRET = os.getenv('AGGREGATOR_API_SECRET')
AGGREGATOR_BRANCH = os.getenv('AGGREGATOR_BRANCH', 'dh-meta-analysis-testing-qa')

router = fastapi.APIRouter()

# get root logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)

logger.addHandler(ch)
engine = DataRegistryReadWriteDB().get_engine()

logger.info("Starting API")
NIH_API_EMAIL = "dhite@broadinstitute.org"
NIH_API_TOOL_NAME = "data-registry"


async def get_current_user_quiet(request: Request, authorization: Optional[str] = Header(None)):
    try:
        return await get_current_user(request, authorization)
    except fastapi.HTTPException:
        return None


async def get_current_user(request: Request, authorization: Optional[str] = Header(None)):
    auth_cookie = request.cookies.get(AUTH_COOKIE_NAME)
    if auth_cookie:
        data = get_decoded_jwt_data(auth_cookie)
        if data:
            user = User(**data)
            user.api_token = auth_cookie
            return user

    if authorization:
        schema, _, token = authorization.partition(' ')
        if schema.lower() == 'bearer' and token:
            data = get_decoded_jwt_data(token)
            if data:
                return User(**data)

    raise fastapi.HTTPException(status_code=401, detail='Not logged in')


@router.get('/datasets', response_class=fastapi.responses.ORJSONResponse)
async def api_datasets(user: User = Depends(get_current_user)):
    try:
        if VIEW_ALL_ROLES.intersection(user.roles):
            return query.get_all_datasets(engine)
        else:
            return query.get_all_datasets_for_user(engine, user)
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
    bioidx.create_new_bioindex(engine, request.dataset_id, s3_path, request.schema_desc)
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


def find_dupe_cols(header, is_csv, panda_header):
    if is_csv:
        header_list = header.split(',')
    else:
        header_list = header.split('\t')

    header_list = [col.replace('"', '').rstrip() for col in header_list]
    renamed_columns = [col for col in panda_header if col not in header_list]
    return renamed_columns


@router.post("/preview-delimited-file")
async def preview_files(file: UploadFile):
    contents = await file.read(100)
    await file.seek(0)

    if contents.startswith(b'\x1f\x8b'):
        sample_lines = await file_utils.get_compressed_sample(file)
    else:
        sample_lines = await file_utils.get_text_sample(file)

    await check_column_counts(sample_lines)
    df = await file_utils.parse_file(io.StringIO('\n'.join(sample_lines)), file.filename)
    dupes = find_dupe_cols(sample_lines[0], ".csv" in file.filename, df.columns)
    # Filter out Unnamed columns - these are from missing headers and we don't need to validate them
    dupes = [col for col in dupes if not col.startswith('Unnamed:')]
    if len(dupes) > 0:
        duped_col_str = ', '.join(set([re.sub(r"\.\d+$", '', dupe) for dupe in dupes]))
        raise fastapi.HTTPException(detail=f"{duped_col_str} specified more than once", status_code=400)
    return {"columns": [column for column in df.columns]}


@router.get('/datasets/{dataset_id}', response_class=fastapi.responses.ORJSONResponse)
async def api_datasets(dataset_id: UUID, user: User = Depends(get_current_user)):
    try:
        check_perms(str(dataset_id), user, "You don't have permission to dataset")
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


@router.post("/google-login", response_class=fastapi.responses.ORJSONResponse)
async def google_login(response: Response, body: dict = Body(...)):
    user_info = get_google_user(body.get('code'))
    user = query.get_user(engine, UserCredentials(user_name=user_info.get('email'), password=None))
    if not user:
        raise fastapi.HTTPException(status_code=401, detail='Username is not in our system')
    else:
        token = get_encoded_jwt_data(user)
        user.api_token = token
        log_user_in(response, user, token)
        return {'status': 'success', 'user': user}


@router.post("/change-password", response_class=fastapi.responses.ORJSONResponse)
async def change_password(post_body: dict = Body(...), user: User = Depends(get_current_user)):
    new_password = post_body.get('password')
    query.update_password(engine, new_password, user)


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
    pages = article_meta.get('Pagination').get('MedlinePgn') if article_meta.get('Pagination') else 'N/A'

    month_year_published = f"{article_meta.get('Journal').get('JournalIssue').get('PubDate').get('Year')} {article_meta.get('Journal').get('JournalIssue').get('PubDate').get('Month')}"

    return {"title": article_meta.get('ArticleTitle', ''), "publication": publication,
            'month_year_published': month_year_published, 'authors': authors, 'volume_issue': volume_issue,
            'pages': pages, 'elocation_id': get_elocation_id(article_meta)}


@router.post("/upload-csv")
async def upload_csv(request: Request):
    filename = request.headers.get('Filename')
    parser = StreamingFormDataParser(request.headers)
    parser.register("file", GzipS3Target(s3.get_file_path("bioindex/uploads", filename), mode='wb'))
    file_size = 0
    async for chunk in request.stream():
        file_size += len(chunk)
        parser.data_received(chunk)
    return {"file_size": file_size, "s3_path": s3.get_file_path("bioindex/uploads", filename)}


@router.get("/hermes-upload-columns")
async def hermes_upload_columns():
    return HERMES_VALIDATOR.column_options()


@router.get("/hermes-uploaded-phenotypes")
async def hermes_uploads_phenotypes(statuses: List[str] = Query(None)):
    return query.fetch_used_phenotypes(engine, statuses)



@router.get("/hermes-users")
async def get_hermes_users(user: User = Depends(get_current_user)):
    if check_hermes_admin_perms(user):
        return query.get_hermes_users(engine)
    else:
        raise fastapi.HTTPException(status_code=403, detail="You need to be a reviewer")


@router.post("/add-hermes-user")
async def add_hermes_user(request: NewUserRequest, user: User = Depends(get_current_user)):
    if check_hermes_admin_perms(user):
        try:
            query.add_new_hermes_user(engine, request)
        except ValueError:
            raise fastapi.HTTPException(status_code=409, detail="User already exists")
    else:
        raise fastapi.HTTPException(status_code=403, detail="You need to be a reviewer")


def check_hermes_admin_perms(user):
    return {"reviewer", SUPER_USER}.intersection(user.roles)


@router.post("/start-aggregator")
async def start_aggregator(req: StartAggregatorRequest, authorization: Optional[str] = Header(None),
                           user: Optional[User] = Depends(get_current_user_quiet)):
    if authorization == AGGREGATOR_API_SECRET or (user and VIEW_ALL_ROLES.intersection(user.roles)):
        job_id = batch.submit_aggregator_job(req.branch, req.method, req.args)
        return {"job_id": job_id}
    else:
        raise fastapi.HTTPException(status_code=403, detail="You don't have permission to perform this action")


@router.get("/hermes-meta-analysis")
async def get_metanalyses(user: User = Depends(get_current_user)):
    if check_hermes_admin_perms(user):
        return query.get_meta_analyses(engine)
    else:
        raise fastapi.HTTPException(status_code=403, detail="You need to be a reviewer")


@router.get("/hermes-meta-analysis/{ma_id}")
async def get_metanalysis(ma_id: UUID, user: User = Depends(get_current_user)):
    if check_hermes_admin_perms(user):
        return query.get_meta_analysis(engine, ma_id)
    else:
        raise fastapi.HTTPException(status_code=403, detail="You need to be a reviewer")


@router.delete("/hermes-delete-dataset/{ds_id}", status_code=204)
async def delete_dataset(ds_id: UUID, user: User = Depends(get_current_user)):
    if not check_hermes_admin_perms(user):
        raise fastapi.HTTPException(status_code=403, detail="You don't have permission to perform this action")
    query.delete_hermes_dataset(engine, ds_id)


@router.post("/hermes-meta-analysis")
async def start_metanalysis(req: MetaAnalysisRequest, background: BackgroundTasks,
                            user: Optional[User] = Depends(get_current_user)):
    if not check_hermes_admin_perms(user):
        raise fastapi.HTTPException(status_code=403, detail="You don't have permission to perform this action")

    req.created_by = user.user_name
    ma_id = query.save_meta_analysis(engine, req)
    query.save_phenotype(engine, req.phenotype, get_phenotypes()[req.phenotype]['dichotomous'])
    last_ancestry = None
    for ds in req.datasets:
        ds_name, ancestry = query.get_name_ancestry_for_ds(engine, ds)
        query.save_dataset_name(engine, ds_name, ancestry)
        last_ancestry = ancestry
    paths_to_copy = [query.get_path_for_ds(engine, ds) for ds in req.datasets]
    s3.clear_variants_raw()
    s3.clear_variants_processed()
    s3.clear_meta_analysis()
    s3.clear_variants()
    for path in paths_to_copy:
        path_parts = path.split('/')
        s3.copy_files_for_meta_analysis(f"hermes/{path_parts[1]}/",
                                        f"hermes/variants_raw/GWAS/{path_parts[1]}/{req.phenotype}")
    background.add_task(batch.submit_and_await_job, engine,
                        {
                            'jobName': 'aggregator-web',
                            'jobQueue': 'aggregator-web-api-queue',
                            'jobDefinition': 'aggregator-web-job',
                            'parameters': {
                                'bucket': s3.BASE_BUCKET,
                                'phenotype': req.phenotype,
                                'ancestry': last_ancestry,
                                'guid': str(ma_id),
                                'branch': AGGREGATOR_BRANCH,
                                'method': req.method,
                                'args': '--no-insert-runs --yes --clusters=1',
                            }}, query.update_meta_analysis_log, ma_id.replace('-', ''), is_qc=False)
    return {'meta-analysis-id': ma_id}


@router.get("/hermes-phenotypes")
async def get_hermes_phenotypes() -> dict:
    return {"data": query.get_hermes_phenotypes(engine)}

@router.get("/get-hermes-pre-signed-url")
async def get_hermes_pre_signed_url(request: Request):
    filename = request.headers.get('Filename')
    dataset = request.headers.get('Dataset')
    s3_path = f"hermes/{dataset}/{filename}"
    return s3.generate_presigned_url_with_path(s3_path)


@router.patch("/hermes-rerun-qc/{file_id}")
async def rerun_hermes_qc(request: QCScriptOptions, file_id, background_tasks: BackgroundTasks, user: User = Depends(get_current_user)):
    if not VIEW_ALL_ROLES.intersection(user.roles) and not query.get_file_owner(engine, file_id) == user.user_name:
        raise fastapi.HTTPException(status_code=401, detail='you aren\'t authorized')

    no_dashes_ids = str(file_id).replace('-', '')
    file_upload = query.fetch_file_upload(engine, no_dashes_ids)

    script_options = {k: v for k, v in request.dict().items() if v is not None}

    query.update_file_qc_options(engine, no_dashes_ids, script_options)
    s3_path = f"hermes/{file_upload.dataset_name}/{file_upload.file_name}"
    background_tasks.add_task(batch.submit_and_await_job, engine, {
        'jobName': 'hermes-qc-job',
        'jobQueue': 'hermes-qc-job-queue',
        'jobDefinition': 'hermes-qc-job',
        'parameters': {
            's3-path': f"s3://{s3.BASE_BUCKET}/{s3_path}",
            'file-guid': file_id,
            'col-map': json.dumps(file_upload.metadata["column_map"]),
            'script-options': json.dumps(script_options)
        }}, query.update_file_upload_qc_log, file_id, True)


@router.get("/hermes-past-metadata")
async def get_hermes_past_metadata(user: User = Depends(get_current_user)):
    return query.retrieve_meta_data_mapping(engine, user.user_name)

@router.post("/validate-hermes")
async def validate_hermes_csv(request: QCHermesFileRequest, background_tasks: BackgroundTasks,
                            user: User = Depends(get_current_user)):


    dataset = request.dataset
    filename = request.file_name
    s3_path = f"hermes/{dataset}/{filename}"

    metadata = request.metadata
    validation_errors, file_size = await validate_file(f"s3://{s3.BASE_BUCKET}/{s3_path}", metadata.get('column_map'))
    if validation_errors:
        return {"errors": validation_errors}

    hg38 = metadata.get('referenceGenome') == 'Hg38'

    script_options = {k: v for k, v in request.qc_script_options.dict().items() if v is not None}
    s3.upload_metadata(metadata, f"hermes/{dataset}")
    file_guid = query.save_file_upload_info(engine, dataset, metadata, s3_path, filename, file_size, user.user_name,
                                            script_options,
                                            HermesFileStatus.SUBMITTED_TO_LIFTOVER if hg38 else HermesFileStatus.SUBMITTED_TO_QC)

    if hg38:
        background_tasks.add_task(batch.submit_and_await_job, engine, {
            'jobName': 'liftover-job',
            'jobQueue': 'liftover-job-queue',
            'jobDefinition': 'liftover-job',
            'parameters': {
                's3-path': f"s3://{s3.BASE_BUCKET}/{s3_path}",
                'chromosome-col': metadata["column_map"]['chromosome'],
                'position-col': metadata["column_map"]['position']
            }}, query.update_file_upload_qc_log, file_guid, True)
        # can we do something here?
    else:
        background_tasks.add_task(batch.submit_and_await_job, engine, {
            'jobName': 'hermes-qc-job',
            'jobQueue': 'hermes-qc-job-queue',
            'jobDefinition': 'hermes-qc-job',
            'parameters': {
                's3-path': f"s3://{s3.BASE_BUCKET}/{s3_path}",
                'file-guid': file_guid,
                'col-map': json.dumps(metadata["column_map"]),
                'script-options': json.dumps(script_options)
            }}, query.update_file_upload_qc_log, file_guid, True)

    return {"file_size": file_size, "s3_path": s3_path, "file_id": file_guid}


@router.get("/upload-hermes/{file_id}")
async def fetch_single_file_upload(file_id: UUID, user: User = Depends(get_current_user)):
    if VIEW_ALL_ROLES.intersection(user.roles) or query.get_file_owner(engine, file_id) == user.user_name:
        file_upload = query.fetch_file_upload(engine, str(file_id).replace('-', ''))

        try:
            sample_content = s3.get_file_sample(file_upload.s3_path)

            if sample_content.startswith(b'\x1f\x8b'):
                lines = await file_utils.convert_compressed_bytes_to_list(sample_content)
            else:
                lines = await file_utils.convert_text_bytes_to_list(sample_content)

            df = await file_utils.parse_file(io.StringIO('\n'.join(lines)), file_upload.file_name)
            response_dict = file_upload.dict()
            response_dict['all_columns'] = [column for column in df.columns]
            return response_dict
        except Exception as e:
            logger.exception(f"Error reading headers from file: {e}")
            return file_upload
    else:
        raise fastapi.HTTPException(status_code=401, detail='you aren\'t authorized to view this dataset')


async def check_column_counts(lines):
    delim = '\t'
    header_count = len(lines[0].split(delim))
    for i, line in enumerate(lines[1:], start=2):
        col_count = len(line.rstrip('\n\r').split(delim))
        if col_count != header_count:
            raise fastapi.HTTPException(
                status_code=400,
                detail=(
                    f"Column mismatch on line {i}: "
                    f"expected {header_count} columns, found {col_count}"
                )
            )



@router.get("/upload-hermes")
async def fetch_all_file_uploads(user: User = Depends(get_current_user), statuses: List[str] = Query(None),
                                 limit: Optional[int] = Query(None), offset: Optional[int] = Query(None),
                                 phenotype: Optional[str] = Query(None), uploader: Optional[str] = Query(None)):
    if VIEW_ALL_ROLES.intersection(user.roles):
        return query.fetch_file_uploads(engine, statuses, limit, offset, phenotype, uploader)
    else:
        return query.fetch_file_uploads(engine, statuses, limit, offset, phenotype, user.user_name)


@router.patch("/upload-hermes/{file_id}")
async def update_single_file_upload(file_id: UUID, status: HermesUploadStatus, user: User = Depends(get_current_user)):
    if VIEW_ALL_ROLES.intersection(user.roles) or query.get_file_owner(engine, file_id) == user.user_name:
        query.update_file_qc_status(engine, file_id, status.status)
    else:
        raise fastapi.HTTPException(status_code=401, detail='you aren\'t authorized')


@router.patch("/upload-hermes-metadata/{file_id}")
async def update_single_file_metadata(file_id: UUID, metadata: dict, background_tasks: BackgroundTasks, user: User = Depends(get_current_user)):
    if VIEW_ALL_ROLES.intersection(user.roles) or query.get_file_owner(engine, file_id) == user.user_name:
        no_dashes_id = str(file_id).replace('-', '')
        query.update_file_metadata(engine, no_dashes_id, metadata)
        file_upload = query.fetch_file_upload(engine, no_dashes_id)
        try:
            background_tasks.add_task(batch.submit_and_await_job, engine, {
            'jobName': 'hermes-qc-job',
            'jobQueue': 'hermes-qc-job-queue',
            'jobDefinition': 'hermes-qc-job',
            'parameters': {
                's3-path': f"s3://{s3.BASE_BUCKET}/{file_upload.s3_path}",
                'file-guid': str(file_id),
                'col-map': json.dumps(metadata["column_map"]),
                'script-options': json.dumps(file_upload.qc_script_options)
            }}, query.update_file_upload_qc_log, str(file_id), True)
            query.update_file_qc_status(engine, no_dashes_id, HermesFileStatus.SUBMITTED_TO_QC)
        except Exception as e:
            logger.exception(f"Error submitting QC job: {e}")
            query.update_file_qc_status(engine, str(file_id).replace('-', ''), HermesFileStatus.SUBMISSION_TO_QC_FAILED)
            raise fastapi.HTTPException(status_code=500, detail=f"Error submitting QC job: {str(e)}")
    else:
        raise fastapi.HTTPException(status_code=401, detail='you aren\'t authorized')

@router.post("/uploadfile/{data_set_id}/{dichotomous}/{sample_size}")
async def upload_file_for_phenotype(data_set_id: str, dichotomous: bool, request: Request,
                                    sample_size: int, response: fastapi.Response, cases: int = None,
                                    controls: int = None, user: User = Depends(get_current_user),
                                    phenotype: str = Query(None, title="Phenotype",
                                                           description="Phenotype for file")):
    check_perms(data_set_id, user, "You don't have permission to add files to this dataset")
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


@router.get("/filelist/{data_set_id}")
async def get_file_list(data_set_id: str):
    try:
        ds_uuid = UUID(data_set_id)
    except ValueError:
        raise fastapi.HTTPException(status_code=404, detail=f'Invalid index: {data_set_id}')
    return get_possible_files(ds_uuid)


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
    return await get_s3_file_name_and_obj(s3_path)


async def get_s3_file_name_and_obj(s3_path):
    split = s3_path[5:].split('/')
    bucket = split[0]
    file_name = split[-1]
    file_path = '/'.join(split[1:])
    obj = s3.get_file_obj(file_path, bucket)
    return file_name, obj

@router.get("/hermes-ma/results/{ma_id}")
async def stream_ma(ma_id: str):
    name, obj = await get_s3_file_name_and_obj(f"s3://{s3.BASE_BUCKET}/hermes/ma-results/{ma_id}/combined_data.csv.gz")

    def generator():
        for chunk in iter(lambda: obj['Body'].read(4096), b''):
            yield chunk

    return StreamingResponse(generator(), media_type='application/octet-stream',
                             headers={"Content-Disposition": f"attachment; filename={name}"})



@router.get('/hermes/metadata/{ds_id}')
def get_hermes_metadata(ds_id, user: User = Depends(get_current_user)):
    if VIEW_ALL_ROLES.intersection(user.roles):
        dataset = query.fetch_file_upload(engine, str(ds_id).replace('-', ''))
        try:

            csv = file_utils.convert_json_to_csv(dataset.metadata)
            return StreamingResponse(
                iter([csv.getvalue()]),
                media_type="text/csv",
                headers={"Content-Disposition": f"attachment; filename={dataset.dataset_name}_metadata.csv"}
            )
        except Exception as e:
            raise fastapi.HTTPException(status_code=400, detail=f"JSON conversion error: {str(e)}")
    else:
        raise fastapi.HTTPException(status_code=403, detail="You need to be a reviewer")

@router.get('/hermes/metadata')
def get_all_hermes_metadata(user: User = Depends(get_current_user)):
    if VIEW_ALL_ROLES.intersection(user.roles):
        datasets = query.fetch_file_uploads(engine)
        try:
            csv = file_utils.convert_multiple_datasets_to_csv(datasets)
            return StreamingResponse(
                iter([csv.getvalue()]),
                media_type="text/csv",
                headers={"Content-Disposition": "attachment; filename=all_datasets_metadata.csv"}
            )
        except Exception as e:
            raise fastapi.HTTPException(status_code=400, detail=f"JSON conversion error: {str(e)}")
    else:
        raise fastapi.HTTPException(status_code=403, detail="You need to be a reviewer")

@router.get('/sgc/phenotypes/download')
def download_sgc_phenotypes():
    """Download all SGC phenotypes as a CSV file. This endpoint is public and does not require authentication."""
    phenotypes = query.get_sgc_phenotypes(engine)
    try:
        import csv
        from io import StringIO
        
        output = StringIO()
        if phenotypes:
            # Convert SGCPhenotype objects to dicts and exclude created_at
            phenotype_dicts = [p.dict(exclude={'created_at'}) for p in phenotypes]
            writer = csv.DictWriter(output, fieldnames=phenotype_dicts[0].keys())
            writer.writeheader()
            writer.writerows(phenotype_dicts)
        
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=sgc_phenotypes.csv"}
        )
    except Exception as e:
        logger.exception("Error generating SGC phenotypes CSV")
        raise fastapi.HTTPException(status_code=500, detail=f"Error generating CSV: {str(e)}")

@router.get("/{ft}/{file_id}", name="stream_file")
async def stream_file(file_id: str, ft: FileType):
    no_dash_id = query.shortened_file_id_lookup(file_id, ft.value, engine)
    try:
        if ft == FileType.CS:
            s3_path = query.get_credible_set_file(engine, no_dash_id)
        elif ft == FileType.D:
            s3_path = query.get_phenotype_file(engine, no_dash_id)
    except ValueError:
        raise fastapi.HTTPException(status_code=404, detail=f'Invalid file: {file_id}')
    split = s3_path[5:].split('/')
    bucket = split[0]
    path = '/'.join(split[1:])
    return RedirectResponse(s3.get_signed_url(bucket, path))

@router.get("/hermes/download/{file_id}")
async def download_hermes_file(file_id: UUID, user: User = Depends(get_current_user)):
    if VIEW_ALL_ROLES.intersection(user.roles) or query.get_file_owner(engine, file_id) == user.user_name:
        upload = query.fetch_file_upload(engine, str(file_id).replace('-', ''))
    else:
       raise fastapi.HTTPException(status_code=401, detail='you aren\'t authorized to view this dataset')

    s3_path = upload.s3_path
    return RedirectResponse(s3.get_signed_url(s3.BASE_BUCKET, s3_path))



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
                                            request: Request, response: fastapi.Response,
                                            user: User = Depends(get_current_user)):
    check_perms(query.get_dataset_id_for_phenotype(engine, phenotype_data_set_id), user,
                "You can't upload files to that dataset")
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
async def delete_dataset(data_set_id: str, response: fastapi.Response, user: User = Depends(get_current_user)):
    check_perms(data_set_id, user, "You don't have permission to delete this dataset")
    try:
        query.delete_dataset(engine, data_set_id)
    except Exception as e:
        logger.exception("There was a problem deleting dataset", e)
        response.status_code = 400
        return {"message": f"There was an error deleting the dataset {data_set_id}"}

    return {"message": f"Successfully deleted dataset {data_set_id}"}


@router.delete("/phenotypes/{phenotype_data_set_id}")
async def delete_phenotype(phenotype_data_set_id: str, response: fastapi.Response,
                           user: User = Depends(get_current_user)):
    check_perms(query.get_dataset_id_for_phenotype(engine, phenotype_data_set_id), user,
                "You don't have permission to delete files in this dataset")
    try:
        query.delete_phenotype(engine, phenotype_data_set_id)
    except Exception as e:
        logger.exception("There was a problem deleting phenotype", e)
        response.status_code = 400
        return {"message": f"There was an error deleting the phenotype {phenotype_data_set_id}"}

    return {"message": f"Successfully deleted phenotype {phenotype_data_set_id}"}


@router.post('/studies', response_class=fastapi.responses.ORJSONResponse)
async def save_study(req: Study):
    study_id = query.insert_study(engine, req)
    return SavedStudy(id=study_id, name=req.name, institution=req.institution, created_at=datetime.now())


@router.get('/studies', response_class=fastapi.responses.ORJSONResponse)
async def get_studies():
    return query.get_studies(engine)


@router.post('/datasets', response_class=fastapi.responses.ORJSONResponse)
async def save_dataset(req: DataSet, user: User = Depends(get_current_user)):
    """
    The body of the request contains the information to insert into the records db
    """
    try:
        dataset_id = query.insert_dataset(engine, req, user.id)
        return SavedDataset(id=dataset_id, created_at=datetime.now(), **req.dict())
    except sqlalchemy.exc.IntegrityError:
        raise fastapi.HTTPException(status_code=409, detail='Dataset name already exists')
    except ClientError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))


@router.patch('/datasets', response_class=fastapi.responses.ORJSONResponse)
async def update_dataset(req: SavedDataset, user: User = Depends(get_current_user)):
    check_perms(str(req.id).replace('-', ''), user, "You do not have permission to update this dataset")
    try:
        query.update_dataset(engine, req)
        return fastapi.responses.Response(content=None, status_code=200)
    except sqlalchemy.exc.IntegrityError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))
    except ClientError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))


def check_perms(ds_id: str, user: User, msg: str):
    if not VIEW_ALL_ROLES.intersection(user.roles):
        ds_owner = query.get_data_set_owner(engine, ds_id)
        if ds_owner != user.id:
            raise fastapi.HTTPException(status_code=401, detail=msg)




@router.get('/is-logged-in')
def is_logged_in(user: User = Depends(get_current_user)):
    if user:
        return user
    else:
        raise fastapi.HTTPException(status_code=401, detail='Not logged in')


def log_user_in(response: Response, user: User, token: str):
    query.log_user_in(engine, user)
    response.set_cookie(key=AUTH_COOKIE_NAME, httponly=True,
                        value=token,
                        domain='.kpndataregistry.org', samesite='strict',
                        secure=os.getenv('USE_HTTPS') == 'true')

@router.post('/login')
def login(response: Response, creds: UserCredentials):
    user = query.get_user(engine, creds)
    if user:
        token = get_encoded_jwt_data(user)
        user.api_token = token
        log_user_in(response, user, token)
        return {'status': 'success', 'user': user}
    else:
        raise fastapi.HTTPException(status_code=401, detail='Invalid username or password')


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
