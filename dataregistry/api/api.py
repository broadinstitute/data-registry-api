import logging

import fastapi
import sqlalchemy
from botocore.exceptions import ClientError
from fastapi import UploadFile
from fastapi.security.api_key import APIKeyHeader
from starlette.status import HTTP_403_FORBIDDEN

from dataregistry.api import query, s3
from dataregistry.api.config import APP_CONFIG
from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api.model import RecordRequest

router = fastapi.APIRouter()


# get root logger
logger = logging.getLogger(__name__)
# connect to database
engine = DataRegistryReadWriteDB().get_engine()
api_key_header = APIKeyHeader(name="access_token", auto_error=False)
valid_api_key = APP_CONFIG['apiKey']

logger.info("Starting API")


async def get_api_key(request_api_key: str = fastapi.Security(api_key_header)):
    if request_api_key == valid_api_key:
        return api_key_header
    else:
        raise fastapi.HTTPException(
            status_code=HTTP_403_FORBIDDEN, detail="Could not validate API KEY"
        )


@router.get('/records', response_class=fastapi.responses.ORJSONResponse, dependencies=[fastapi.Depends(get_api_key)])
async def api_records():
    try:
        records = query.get_all_records(engine)
        return [record.to_json() for record in records]
    except ValueError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))


@router.get('/records/{index}', response_class=fastapi.responses.ORJSONResponse,
            dependencies=[fastapi.Depends(get_api_key)])
async def api_records(index: int):
    try:
        record = query.get_record(engine, index)
        return record.to_json()
    except KeyError:
        raise fastapi.HTTPException(status_code=400, detail=f'Invalid index: {index}')
    except ValueError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))


@router.post("/uploadfile/{recordid}", dependencies=[fastapi.Depends(get_api_key)])
async def upload_file_for_record(record_name: str, file: UploadFile):
    try:
        upload = s3.initiate_multi_part(record_name, file.filename)
        part_number = 1
        parts = []
        while contents := await file.read(1024 * 1024 * 5):
            upload_part_response = s3.put_bytes(record_name, file.filename, contents, upload, part_number)
            parts.append({
                'PartNumber': part_number,
                'ETag': upload_part_response['ETag']
            })
            part_number = part_number + 1
        s3.finalize_upload(record_name, file.filename, parts, upload)
    except Exception as e:
        logger.exception("There was a problem uploading file", e)
        return {"message": "There was an error uploading the file"}
    finally:
        await file.close()

    return {"message": f"Successfully uploaded {file.filename}"}


@router.post('/records', response_class=fastapi.responses.ORJSONResponse, dependencies=[fastapi.Depends(get_api_key)])
async def api_record_post(req: RecordRequest):
    """
    The body of the request contains the information to insert into the records db
    """
    try:
        s3_record_id = query.insert_record(engine, req)

        return {
            'name': req.name,
            's3_record_id': s3_record_id
        }
    except sqlalchemy.exc.IntegrityError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))
    except ClientError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))


@router.delete('/records/{index}', response_class=fastapi.responses.ORJSONResponse,
               dependencies=[fastapi.Depends(get_api_key)])
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
