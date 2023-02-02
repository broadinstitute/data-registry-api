from botocore.exceptions import ClientError
from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api import query
import fastapi
import sqlalchemy

# create flask app; this will load .env
router = fastapi.APIRouter()

# connect to database
engine = DataRegistryReadWriteDB().get_engine()


@router.get('/records', response_class=fastapi.responses.ORJSONResponse)
async def api_records():
    try:
        records = query.get_all_records(engine)
        return [record.to_json() for record in records]
    except ValueError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))


@router.get('/records/{index}', response_class=fastapi.responses.ORJSONResponse)
async def api_records(index: int):
    try:
        record = query.get_record(engine, index)
        return record.to_json()
    except KeyError:
        raise fastapi.HTTPException(status_code=400, detail=f'Invalid index: {index}')
    except ValueError as e:
        raise fastapi.HTTPException(status_code=400, detail=str(e))


@router.post('/records', response_class=fastapi.responses.ORJSONResponse)
async def api_record_post(req: fastapi.Request):
    """
    The body of the request contains the information to insert into the records db
    """
    try:
        record_info = await req.json()
        s3_record_id = query.insert_record(engine, record_info)

        return {
            'name': record_info['name'],
            's3_record_id': s3_record_id
        }
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
