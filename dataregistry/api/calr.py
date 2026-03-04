import io
import os
import uuid
import tempfile
from pathlib import Path
from typing import List, Optional

import fastapi
import httpx
import boto3
from fastapi import UploadFile, Form, Depends, Header
from fastapi.responses import StreamingResponse

from dataregistry.api import s3, query
from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api.model import CALRFile, CALRSubmission, User

# Import CalR conversion functions
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from calr.loaders import detect_format, load_cal_file
from calr.oxymax_loader import load_oxymax_file, convert_oxymax
from calr.sable_loader import load_sable_file, convert_sable
from calr.tse_loader import load_tse_file, convert_tse

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


def _upload_file_to_s3(file_content: bytes, s3_key: str, content_type: str):
    """Upload file bytes to S3."""
    s3_client = boto3.client('s3', region_name=s3.S3_REGION)
    s3_client.put_object(
        Bucket=s3.BASE_BUCKET,
        Key=s3_key,
        Body=file_content,
        ContentType=content_type or 'application/octet-stream'
    )


@router.post("/calr/files")
async def upload_calr_files(
    standard_file: UploadFile,
    session_file: UploadFile,
    name: str = Form(...),
    description: str = Form(''),
    public: bool = Form(False),
    user: User = Depends(get_calr_user)
):
    """
    Upload a paired CALR submission (standard format file + session file).
    Creates one submission record and two file records.
    """
    try:
        submission_id = str(uuid.uuid4()).replace('-', '')

        # Create submission record
        submission = CALRSubmission(
            id=submission_id,
            name=name,
            description=description or None,
            public=public,
            uploaded_by=user.user_name
        )
        saved_sub_id = query.insert_calr_submission(engine, submission)

        file_ids = {}
        for file_type, upload_file in [('standard', standard_file), ('session', session_file)]:
            content = await upload_file.read()
            file_size = len(content)
            file_id = str(uuid.uuid4()).replace('-', '')
            s3_key = f"calr/{user.user_name}/{saved_sub_id}/{file_type}/{upload_file.filename}"

            _upload_file_to_s3(content, s3_key, upload_file.content_type)

            calr_file = CALRFile(
                id=file_id,
                submission_id=saved_sub_id,
                file_type=file_type,
                file_name=upload_file.filename,
                file_size=file_size,
                s3_path=s3_key,
            )
            saved_file_id = query.insert_calr_file(engine, calr_file)
            file_ids[file_type] = {
                'file_id': saved_file_id,
                'file_name': upload_file.filename,
                'file_size': file_size,
            }

        return {
            "message": "Submission uploaded successfully",
            "submission_id": saved_sub_id,
            "name": name,
            "public": public,
            "files": file_ids,
            "uploaded_by": user.user_name
        }

    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"Error uploading files: {str(e)}")


@router.get("/calr/files")
async def list_calr_submissions(user: User = Depends(get_calr_user)):
    """
    List all CALR submissions for the authenticated user.
    Returns submissions with nested file info.
    """
    try:
        return query.get_calr_submissions_by_user(engine, user.user_name)
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"Error retrieving submissions: {str(e)}")


@router.get("/calr/public")
async def list_public_calr_submissions():
    """
    List all public CALR submissions.
    No authentication required. Returns id, name, description, uploaded_at,
    and file IDs/types for each public submission.
    """
    try:
        return query.get_public_calr_submissions(engine)
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"Error retrieving public submissions: {str(e)}")


@router.get("/calr/files/{file_id}")
async def download_calr_file(file_id: str):
    """
    Download a CALR file by streaming its content from S3.
    Only files belonging to public submissions are accessible.
    No authentication required.
    """
    try:
        file_info = query.get_calr_file_by_id(engine, file_id)
        if not file_info or not file_info['public']:
            raise fastapi.HTTPException(status_code=404, detail="File not found")

        s3_client = boto3.client('s3', region_name=s3.S3_REGION)

        try:
            s3_response = s3_client.get_object(Bucket=s3.BASE_BUCKET, Key=file_info['s3_path'])
        except s3_client.exceptions.NoSuchKey:
            raise fastapi.HTTPException(status_code=404, detail="File not found in storage")

        file_name = file_info['file_name']
        content_type = 'text/csv'
        if file_name.endswith('.tsv') or file_name.endswith('.txt'):
            content_type = 'text/tab-separated-values'

        def stream_s3_file():
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
async def get_calr_file_info(file_id: str):
    """
    Get metadata for a public CALR file without downloading it.
    No authentication required. Returns 404 for non-public files.
    """
    try:
        file_info = query.get_calr_file_by_id(engine, file_id)
        if not file_info or not file_info['public']:
            raise fastapi.HTTPException(status_code=404, detail="File not found")

        # Don't expose s3_path in public info
        return {
            'id': file_info['id'],
            'submission_id': file_info['submission_id'],
            'file_type': file_info['file_type'],
            'file_name': file_info['file_name'],
            'file_size': file_info['file_size'],
            'uploaded_at': file_info['uploaded_at'],
        }

    except fastapi.HTTPException:
        raise
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"Error retrieving file info: {str(e)}")


@router.post("/calr/convert")
async def convert_calr_files(files: List[UploadFile]):
    """
    Convert one or more calorimetry files to standard CalR format.

    Accepts files in vendor formats (Oxymax/CLAMS, TSE, Sable) and streams
    back a single CSV in standard CalR format. No authentication required.

    For Oxymax/CLAMS, multiple files are supported (one per cage) and will
    be combined into a single output. For TSE and Sable, only the first
    file is processed.

    Returns the converted data as a streaming CSV response.
    """
    temp_paths = []
    try:
        if not files:
            raise fastapi.HTTPException(
                status_code=400, detail="At least one file is required"
            )

        # Save all uploaded files to temp directory
        for upload_file in files:
            content = await upload_file.read()
            with tempfile.NamedTemporaryFile(
                mode='wb', delete=False, suffix='.csv'
            ) as tmp:
                tmp.write(content)
                temp_paths.append(tmp.name)

        # Detect format from first file
        try:
            detected_format = detect_format(temp_paths[0])
        except ValueError as e:
            raise fastapi.HTTPException(
                status_code=400,
                detail=f"Unrecognized file format: {str(e)}"
            )

        # Convert based on detected format
        import pandas as pd

        if detected_format == 'oxymax':
            # Oxymax supports multiple files (one per cage)
            raw_data_list = [load_oxymax_file(p) for p in temp_paths]
            converted_df = convert_oxymax(raw_data_list)

        elif detected_format == 'tse':
            raw_data = load_tse_file(temp_paths[0])
            converted_df = convert_tse(raw_data)

        elif detected_format == 'sable':
            raw_data = load_sable_file(temp_paths[0])
            converted_df = convert_sable(raw_data)

        elif detected_format == 'calr':
            # Already standard format, just return it
            converted_df = pd.read_csv(temp_paths[0])

        else:
            raise fastapi.HTTPException(
                status_code=400,
                detail=f"Unsupported format: {detected_format}"
            )

        # Stream back as CSV
        csv_buffer = io.StringIO()
        converted_df.to_csv(csv_buffer, index=False)
        csv_bytes = csv_buffer.getvalue().encode('utf-8')

        return StreamingResponse(
            io.BytesIO(csv_bytes),
            media_type='text/csv',
            headers={
                "Content-Disposition": 'attachment; filename="calr_converted.csv"',
                "Content-Length": str(len(csv_bytes)),
            }
        )

    except fastapi.HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise fastapi.HTTPException(
            status_code=500,
            detail=f"Error converting file: {str(e)}"
        )
    finally:
        for p in temp_paths:
            try:
                os.unlink(p)
            except Exception:
                pass


@router.delete("/calr/files/{submission_id}")
async def delete_calr_submission(submission_id: str, user: User = Depends(get_calr_user)):
    """
    Delete a CALR submission and both of its files.
    Users can only delete their own submissions.
    Removes S3 objects and database records.
    """
    try:
        # Get all files for this submission to check ownership and get S3 paths
        files = query.get_calr_files_by_submission(engine, submission_id)
        if not files:
            raise fastapi.HTTPException(status_code=404, detail="Submission not found")

        # Verify ownership by checking one of the files (they share the same submission)
        file_info = query.get_calr_file_by_id(engine, files[0]['id'])
        if not file_info or file_info['uploaded_by'] != user.user_name:
            raise fastapi.HTTPException(
                status_code=403,
                detail="You can only delete your own submissions"
            )

        # Delete S3 objects
        s3_client = boto3.client('s3', region_name=s3.S3_REGION)
        for f in files:
            try:
                s3_client.delete_object(Bucket=s3.BASE_BUCKET, Key=f['s3_path'])
            except Exception:
                pass  # Best-effort S3 cleanup

        # Delete submission and files from database
        deleted = query.delete_calr_submission(engine, submission_id)
        if not deleted:
            raise fastapi.HTTPException(status_code=404, detail="Submission not found")

        return {"message": "Submission deleted successfully", "submission_id": submission_id}

    except fastapi.HTTPException:
        raise
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"Error deleting submission: {str(e)}")
