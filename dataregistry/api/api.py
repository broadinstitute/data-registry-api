import io
import json
import logging
from uuid import UUID

import fastapi
import requests
import sqlalchemy
import xmltodict
from botocore.exceptions import ClientError
from fastapi import UploadFile
from starlette.responses import StreamingResponse

from dataregistry.api import query, s3
from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api.model import DataSet, Study, SavedDatasetInfo, SavedDataset
from dataregistry.pub_ids import PubIdType, infer_id_type

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
    abstract = article_meta.get('Abstract')
    return {"title": article_meta.get('ArticleTitle', ''),
            "abstract": abstract.get('AbstractText', '') if abstract else ''}


@router.post("/uploadfile/{data_set_id}/{phenotype}/{dichotomous}/{sample_size}")
async def upload_file_for_phenotype(data_set_id: str, phenotype: str, dichotomous: bool, file: UploadFile,
                                    sample_size: int, response: fastapi.Response, cases: int = None,
                                    controls: int = None):
    filename = file.filename
    try:
        saved_dataset = query.get_dataset(engine, UUID(data_set_id))
        file_path = f"{saved_dataset.name}/{phenotype}"
        await multipart_upload_to_s3(file, file_path)
        pd_id = query.insert_phenotype_data_set(engine, data_set_id, phenotype,
                                                f"s3://{s3.BASE_BUCKET}/{file_path}/{filename}", dichotomous,
                                                sample_size, cases, controls, filename)
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
    return get_possible_files(ds, ds_uuid)


@router.get("/files/{data_set_id}/{filename}", name="stream_file")
async def stream_file(data_set_id: str, filename: str, filepath: str):
    try:
        ds_uuid = UUID(data_set_id)
        ds = query.get_dataset(engine, ds_uuid)
        if not ds.publicly_available:
            raise fastapi.HTTPException(status_code=403, detail=f'{data_set_id} is not publicly available')
    except ValueError:
        raise fastapi.HTTPException(status_code=404, detail=f'Invalid index: {data_set_id}')
    available_files = [file['path'] for file in get_possible_files(ds, ds_uuid)]
    if filepath not in available_files:
        raise fastapi.HTTPException(status_code=404, detail=f'File {filepath} not found')

    obj = s3.get_file_obj(filepath)

    def generator():
        for chunk in iter(lambda: obj['Body'].read(4096), b''):
            yield chunk

    return StreamingResponse(generator(), media_type='application/octet-stream')


def get_possible_files(ds, ds_uuid):
    available_files = []
    phenos = query.get_phenotypes_for_dataset(engine, ds_uuid)
    for pheno in phenos:
        available_files.append({'path': f"{ds.name}/{pheno.phenotype}/{pheno.file_name}", 'name': pheno.file_name,
                                'phenotype': pheno.phenotype, 'created_at': pheno.created_at, 'type': 'data set'})
    if phenos:
        credible_sets = query.get_credible_sets_for_dataset(engine, [pheno.id for pheno in phenos])
        for cs in credible_sets:
            available_files.append({'path': f"credible_sets/{str(cs.phenotype_data_set_id).replace('-', '')}/{cs.file_name}",
                                    'name': cs.file_name, 'phenotype': cs.phenotype, 'type': 'credible set',
                                    'created_at': cs.created_at})
    return available_files


@router.post("/crediblesetupload/{phenotype_data_set_id}/{credible_set_name}")
async def upload_credible_set_for_phenotype(phenotype_data_set_id: str, credible_set_name: str,
                                            file: UploadFile, response: fastapi.Response):
    try:
        file_path = f"credible_sets/{phenotype_data_set_id}"
        await multipart_upload_to_s3(file, file_path)
        cs_id = query.insert_credible_set(engine, phenotype_data_set_id, file_path, credible_set_name, file.filename)
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
    # read and put 50 mb at a time--is that too small?
    while contents := await file.read(1024 * 1024 * 50):
        upload_part_response = s3.put_bytes(file_path, file.filename, contents, upload, part_number)
        parts.append({
            'PartNumber': part_number,
            'ETag': upload_part_response['ETag']
        })
        part_number = part_number + 1
    s3.finalize_upload(file_path, file.filename, parts, upload)


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
