import os

import fastapi
import httpx
import boto3
from typing import Optional
from fastapi import UploadFile, Form, Depends, Header
from fastapi.responses import StreamingResponse

from dataregistry.api import s3, query
from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api.model import CALRFile, User

router = fastapi.APIRouter()
engine = DataRegistryReadWriteDB().get_engine()

USER_SERVICE_URL = os.getenv('USER_SERVICE_URL', 'https://users.kpndataregistry.org')


async def get_calr_user(authorization: Optional[str] = Header(None)):
    """Validate CALR user token against the user service."""
    if not authorization:
        raise fastapi.HTTPException(status_code=401, detail='Authorization header required')

    schema, _, token = authorization.partition(' ')
    if schema.lower() != 'bearer' or not token:
        raise fastapi.HTTPException(status_code=401, detail='Bearer token required')

    calr_user_group = os.getenv('CALR_USER_GROUP', 'calr')

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{USER_SERVICE_URL}/api/auth/verify/",
                params={"group": calr_user_group},
                headers={"Authorization": f"Bearer {token}"}
            )
            if response.status_code == 200:
                user_data = response.json()
                user = user_data.get('user')
                return User(
                    id=user.get('id'),
                    user_name=user.get('username'),
                    email=user.get('email'),
                    roles=user.get('roles', []),
                    permissions=user.get('permissions', [])
                )
            else:
                raise fastapi.HTTPException(status_code=401, detail='Invalid token')
    except httpx.RequestError:
        raise fastapi.HTTPException(status_code=503, detail='User service unavailable')


@router.post("/calr/files")
async def upload_calr_file(
    file: UploadFile,
    name: str = Form(...),
    user: User = Depends(get_calr_user)
):
    """
    Upload a calorimetry data file.
    The file is stored in S3 and metadata is saved to the database.
    """
    try:
        # Read file content
        file_content = await file.read()
        file_size = len(file_content)

        # Generate S3 path
        import uuid
        file_id = str(uuid.uuid4()).replace('-', '')
        s3_key = f"calr/{user.user_name}/{file_id}/{file.filename}"

        # Upload to S3
        s3_client = boto3.client('s3', region_name=s3.S3_REGION)
        bucket = s3.BASE_BUCKET

        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=file_content,
            ContentType=file.content_type or 'application/octet-stream'
        )

        # Create database record
        calr_file = CALRFile(
            id=file_id,
            name=name,
            file_name=file.filename,
            file_size=file_size,
            s3_path=s3_key,
            uploaded_by=user.user_name
        )

        saved_file_id = query.insert_calr_file(engine, calr_file)

        return {
            "message": "File uploaded successfully",
            "file_id": saved_file_id,
            "name": name,
            "file_name": file.filename,
            "file_size": file_size,
            "uploaded_by": user.user_name
        }

    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"Error uploading file: {str(e)}")


@router.get("/calr/files")
async def list_calr_files(user: User = Depends(get_calr_user)):
    """
    List all CALR files uploaded by the current user.
    Users can only see their own files.
    """
    try:
        files = query.get_calr_files_by_user(engine, user.user_name)
        return files
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"Error retrieving files: {str(e)}")


@router.get("/calr/files/{file_id}")
async def download_calr_file(file_id: str, user: User = Depends(get_calr_user)):
    """
    Download a CALR file by streaming its content from S3.
    Users can only download their own files.
    Returns the file content as a streaming response.
    """
    try:
        # Get file info from database
        file_info = query.get_calr_file_by_id(engine, file_id)
        if not file_info:
            raise fastapi.HTTPException(status_code=404, detail="File not found")

        # Check ownership
        if file_info['uploaded_by'] != user.user_name:
            raise fastapi.HTTPException(
                status_code=403,
                detail="You can only download your own files"
            )

        # Stream file from S3
        s3_client = boto3.client('s3', region_name=s3.S3_REGION)
        bucket = s3.BASE_BUCKET
        s3_key = file_info['s3_path']

        try:
            s3_response = s3_client.get_object(Bucket=bucket, Key=s3_key)
        except s3_client.exceptions.NoSuchKey:
            raise fastapi.HTTPException(status_code=404, detail="File not found in storage")

        # Determine content type based on file extension
        file_name = file_info['file_name']
        content_type = 'text/csv'
        if file_name.endswith('.tsv') or file_name.endswith('.txt'):
            content_type = 'text/tab-separated-values'

        def stream_s3_file():
            """Generator to stream S3 file content."""
            for chunk in s3_response['Body'].iter_chunks(chunk_size=8192):
                yield chunk

        return StreamingResponse(
            stream_s3_file(),
            media_type=content_type,
            headers={
                "Content-Disposition": f'attachment; filename="{file_name}"',
                "Content-Length": str(file_info['file_size'])
            }
        )

    except fastapi.HTTPException:
        raise
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"Error downloading file: {str(e)}")


@router.get("/calr/files/{file_id}/info")
async def get_calr_file_info(file_id: str, user: User = Depends(get_calr_user)):
    """
    Get metadata for a CALR file without downloading it.
    Users can only see info for their own files.
    """
    try:
        file_info = query.get_calr_file_by_id(engine, file_id)
        if not file_info:
            raise fastapi.HTTPException(status_code=404, detail="File not found")

        # Check ownership
        if file_info['uploaded_by'] != user.user_name:
            raise fastapi.HTTPException(
                status_code=403,
                detail="You can only view your own files"
            )

        return file_info

    except fastapi.HTTPException:
        raise
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"Error retrieving file info: {str(e)}")


@router.delete("/calr/files/{file_id}")
async def delete_calr_file(file_id: str, user: User = Depends(get_calr_user)):
    """
    Delete a CALR file.
    Users can only delete their own files.
    Removes both the S3 object and the database record.
    """
    try:
        # Get file info from database
        file_info = query.get_calr_file_by_id(engine, file_id)
        if not file_info:
            raise fastapi.HTTPException(status_code=404, detail="File not found")

        # Check ownership
        if file_info['uploaded_by'] != user.user_name:
            raise fastapi.HTTPException(
                status_code=403,
                detail="You can only delete your own files"
            )

        # Delete from S3
        s3_client = boto3.client('s3', region_name=s3.S3_REGION)
        bucket = s3.BASE_BUCKET
        s3_key = file_info['s3_path']

        try:
            s3_client.delete_object(Bucket=bucket, Key=s3_key)
        except Exception as s3_error:
            # Log but don't fail if S3 delete fails
            pass

        # Delete from database
        deleted = query.delete_calr_file(engine, file_id)
        if not deleted:
            raise fastapi.HTTPException(status_code=404, detail="File not found")

        return {"message": "File deleted successfully", "file_id": file_id}

    except fastapi.HTTPException:
        raise
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"Error deleting file: {str(e)}")
