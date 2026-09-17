import io
import os
import re
import zipfile
from datetime import datetime
from typing import Optional
from uuid import UUID

import boto3
import fastapi
import httpx
from fastapi import UploadFile, File, Depends, Header
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, EmailStr

from dataregistry.api import query
from dataregistry.api import peg_validation
from dataregistry.api import s3
from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api.model import User

router = fastapi.APIRouter()
engine = DataRegistryReadWriteDB().get_engine()

USER_SERVICE_URL = os.getenv('USER_SERVICE_URL', 'https://users.kpndataregistry.org')
PEG_USER_TOKEN = os.getenv('PEG_USER_TOKEN')


_SAFE_FILENAME = re.compile(r'^[\w\-\.]+$')


def _validate_filename(filename: Optional[str]) -> None:
    """Reject filenames that could escape the intended S3 prefix."""
    if not filename or not _SAFE_FILENAME.match(filename) or '..' in filename:
        raise fastapi.HTTPException(
            status_code=400,
            detail="Invalid filename: must contain only alphanumeric characters, hyphens, underscores, or single dots"
        )


def check_review_permissions(user: User):
    """Check if user has PEG review permissions."""
    return user.permissions and "peg-review-data" in user.permissions


async def get_peg_user(authorization: Optional[str] = Header(None)):
    """Verify PEG user authentication via Bearer token."""
    if not authorization:
        raise fastapi.HTTPException(status_code=401, detail='Authorization header required')
    
    schema, _, token = authorization.partition(' ')
    if schema.lower() != 'bearer' or not token:
        raise fastapi.HTTPException(status_code=401, detail='Bearer token required')
    
    peg_user_group = os.getenv('PEG_USER_GROUP', 'peg')
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{USER_SERVICE_URL}/api/auth/verify/",
                params={"group": peg_user_group},
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
                    permissions=user.get('permissions', []),
                    first_name=None,
                    last_name=None,
                    avatar=None,
                    is_active=None,
                    groups=None,
                    is_internal=None,
                    api_token=None,
                )
            else:
                raise fastapi.HTTPException(status_code=401, detail='Invalid token')
    except httpx.RequestError:
        raise fastapi.HTTPException(status_code=503, detail='User service unavailable')


PEG_UPLOAD_FIELDS = (
    ("peg_list", "peg_list"),
    ("peg_matrix", "peg_matrix"),
    ("peg_metadata", "peg_metadata"),
)

DEFAULT_CONTENT_TYPES = {
    "peg_list": "text/tab-separated-values",
    "peg_matrix": "text/tab-separated-values",
    "peg_metadata": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
}


async def _read_peg_uploads(peg_list: UploadFile, peg_matrix: UploadFile, peg_metadata: UploadFile) -> dict:
    """Validate filenames and read all three uploads into memory.

    Returns {field_name: (filename, bytes, content_type)}.
    """
    uploads = {}
    for field_name, upload in (("peg_list", peg_list), ("peg_matrix", peg_matrix), ("peg_metadata", peg_metadata)):
        _validate_filename(upload.filename)
        uploads[field_name] = (upload.filename, await upload.read(), upload.content_type)
    return uploads


async def _validate_uploads(uploads: dict) -> dict:
    """Run the toolkit off the event loop; it takes a few seconds on real data."""
    return await run_in_threadpool(
        peg_validation.validate_peg_files,
        uploads["peg_list"][1], uploads["peg_list"][0],
        uploads["peg_matrix"][1], uploads["peg_matrix"][0],
        uploads["peg_metadata"][1], uploads["peg_metadata"][0],
    )


@router.post("/peg/validate-files")
async def validate_peg_files_dry_run(
    peg_list: UploadFile = File(...),
    peg_matrix: UploadFile = File(...),
    peg_metadata: UploadFile = File(...),
    user: User = Depends(get_peg_user),
):
    """Validate a PEG submission without storing anything. Always 200; see report.status."""
    uploads = await _read_peg_uploads(peg_list, peg_matrix, peg_metadata)
    return await _validate_uploads(uploads)


class PEGStudyMetadata(BaseModel):
    """Metadata for a PEG study"""
    study_author: str
    phenotype: str
    gwas_source: str
    gwas_source_type: str
    published: str
    publication_ref: Optional[str] = None
    phenotype_is_custom: bool = False
    mondo_id: Optional[str] = None


class PEGStudy(BaseModel):
    """PEG Study response model"""
    id: UUID
    accession_id: str
    name: str
    created_by: str
    created_at: datetime
    updated_at: Optional[datetime] = None
    metadata: PEGStudyMetadata


class PEGFile(BaseModel):
    """PEG file record"""
    id: UUID
    study_id: UUID
    file_type: str  # 'peg_list' or 'peg_matrix'
    file_name: str
    file_path: str
    file_size: int
    uploaded_at: datetime


class CreatePEGStudyRequest(BaseModel):
    """Request to create a new PEG study"""
    name: str
    metadata: PEGStudyMetadata


class PEGNewUserRequest(BaseModel):
    """Public self-service signup request"""
    user_name: EmailStr
    password: str


@router.get("/peg/is-logged-in")
async def peg_is_logged_in(user: User = Depends(get_peg_user)):
    """Check if the user is logged in to PEG."""
    return user


@router.get("/peg/users")
async def list_peg_users(user: User = Depends(get_peg_user)):
    """
    List all PEG users from the user service.
    Requires 'peg-review-data' permission.
    """
    if not check_review_permissions(user):
        raise fastapi.HTTPException(
            status_code=403,
            detail="You need 'peg-review-data' permission to list users"
        )

    if not PEG_USER_TOKEN:
        raise fastapi.HTTPException(status_code=503, detail="User listing is not configured")

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{USER_SERVICE_URL}/api/auth/list-users/",
                params={"token": PEG_USER_TOKEN}
            )
            if response.status_code == 200:
                return response.json()
            try:
                detail = response.json()
            except Exception:
                detail = response.text
            raise fastapi.HTTPException(status_code=response.status_code, detail=detail)
    except httpx.RequestError:
        raise fastapi.HTTPException(status_code=503, detail="User service unavailable")


@router.post("/peg/create-user", status_code=201)
async def create_peg_user(request: PEGNewUserRequest):
    """
    Self-service registration for PEG users.
    Creates a new account in the user service. No authentication required.
    """
    if not PEG_USER_TOKEN:
        raise fastapi.HTTPException(status_code=503, detail="User creation is not configured")

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{USER_SERVICE_URL}/api/auth/create-user/",
                data={
                    "token": PEG_USER_TOKEN,
                    "username": request.user_name,
                    "email": request.user_name,
                    "password": request.password,
                }
            )
            if response.status_code in (200, 201):
                return {
                    "message": "Account created successfully",
                    "username": request.user_name,
                    "existing_user": False,
                }

            # The user service is shared across tenants/groups and usernames are
            # globally unique, so an "already exists" rejection often just means
            # the account isn't in this token's group yet. Fall back to adding the
            # existing user to the token's group/roles (mirrors copy_*_to_prod.py).
            # Note: the existing account's password is left unchanged.
            if response.status_code == 400 and "already exists" in response.text.lower():
                add_response = await client.post(
                    f"{USER_SERVICE_URL}/api/auth/add-user-to-group/",
                    data={"token": PEG_USER_TOKEN, "username": request.user_name},
                )
                if add_response.status_code == 200:
                    return {
                        "message": "Existing account granted access",
                        "username": request.user_name,
                        "existing_user": True,
                    }
                try:
                    detail = add_response.json()
                except Exception:
                    detail = add_response.text
                raise fastapi.HTTPException(status_code=add_response.status_code, detail=detail)

            try:
                detail = response.json()
            except Exception:
                detail = response.text
            raise fastapi.HTTPException(status_code=response.status_code, detail=detail)
    except httpx.RequestError:
        raise fastapi.HTTPException(status_code=503, detail="User service unavailable")


@router.post("/peg/studies")
async def create_peg_study(request: CreatePEGStudyRequest, user: User = Depends(get_peg_user)):
    """Create a new PEG study"""
    try:
        result = query.create_peg_study(
            engine=engine,
            name=request.name,
            created_by=user.user_name,
            metadata=request.metadata.dict()
        )

        return {
            "id": result['id'],
            "accession_id": result['accession_id'],
            "message": "PEG study created successfully"
        }
    except Exception as e:
        # Check for duplicate key error
        if "Duplicate entry" in str(e) and "idx_unique_name" in str(e):
            raise fastapi.HTTPException(
                status_code=400,
                detail=f"A study with the name '{request.name}' already exists. Please choose a different name."
            )
        raise


@router.get("/peg/studies")
async def list_peg_studies(user: User = Depends(get_peg_user)):
    """List PEG studies.
    - Users with 'peg-review-data' permission can see all studies
    - Other users can only see studies they created
    """
    try:
        if check_review_permissions(user):
            # Reviewer can see all studies
            studies = query.get_peg_studies(engine, created_by=None)
        else:
            # Regular user can only see their own studies
            studies = query.get_peg_studies(engine, created_by=user.user_name)
        
        return studies
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"Error retrieving studies: {str(e)}")


@router.get("/peg/studies/{study_id}")
async def get_peg_study(study_id: UUID, user: User = Depends(get_peg_user)):
    """Get a specific PEG study.
    - Users with 'peg-review-data' permission can see any study
    - Other users can only see studies they created
    """
    try:
        study = query.get_peg_study(engine, study_id)
        if not study:
            raise fastapi.HTTPException(status_code=404, detail="Study not found")
        
        # Check permissions: either the user owns the study or has review permissions
        if not (study['created_by'] == user.user_name or check_review_permissions(user)):
            raise fastapi.HTTPException(
                status_code=403,
                detail="You can only view studies you created"
            )
        
        return study
    except fastapi.HTTPException:
        raise
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"Error retrieving study: {str(e)}")


@router.patch("/peg/studies/{study_id}")
async def update_peg_study(study_id: UUID, request: CreatePEGStudyRequest, user: User = Depends(get_peg_user)):
    """Update a PEG study's metadata.
    - Users can only update studies they created
    - Users with 'peg-review-data' permission can update any study
    """
    try:
        study = query.get_peg_study(engine, study_id)
        if not study:
            raise fastapi.HTTPException(status_code=404, detail="Study not found")
        
        # Check permissions: either the user owns the study or has review permissions
        if not (study['created_by'] == user.user_name or check_review_permissions(user)):
            raise fastapi.HTTPException(
                status_code=403,
                detail="You can only update studies you created"
            )
        
        query.update_peg_study(
            engine=engine,
            study_id=study_id,
            name=request.name,
            metadata=request.metadata.dict()
        )
        return {"message": "PEG study updated successfully"}
    except fastapi.HTTPException:
        raise
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"Error updating study: {str(e)}")


@router.delete("/peg/studies/{study_id}")
async def delete_peg_study(study_id: UUID, user: User = Depends(get_peg_user)):
    """Delete a PEG study and all its files.
    - Only users with 'peg-review-data' permission can delete studies
    """
    try:
        # Check permissions: only reviewers can delete studies
        if not check_review_permissions(user):
            raise fastapi.HTTPException(
                status_code=403,
                detail="Only reviewers can delete studies"
            )
        
        study = query.get_peg_study(engine, study_id)
        if not study:
            raise fastapi.HTTPException(status_code=404, detail="Study not found")
        
        query.delete_peg_study(engine, study_id)
        return {"message": "PEG study deleted successfully"}
    except fastapi.HTTPException:
        raise
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"Error deleting study: {str(e)}")


@router.post("/peg/studies/{study_id}/files")
async def upload_peg_files(
    study_id: UUID,
    peg_list: UploadFile = File(...),
    peg_matrix: UploadFile = File(...),
    peg_metadata: UploadFile = File(...),
    user: User = Depends(get_peg_user),
):
    """Validate and store a complete PEG submission (list, matrix, metadata).

    Errors → 422 with the report, nothing stored. Success → all three files
    replace any previous files on the study; the report is returned so
    warnings still reach the user.
    """
    study = query.get_peg_study(engine, study_id)
    if not study:
        raise fastapi.HTTPException(status_code=404, detail="Study not found")
    if not (study['created_by'] == user.user_name or check_review_permissions(user)):
        raise fastapi.HTTPException(status_code=403, detail="You can only upload files to studies you created")

    uploads = await _read_peg_uploads(peg_list, peg_matrix, peg_metadata)
    report = await _validate_uploads(uploads)
    if report["status"] == "error":
        return JSONResponse(status_code=422, content={"detail": "PEG files failed validation", "report": report})

    s3_client = boto3.client('s3', region_name=s3.S3_REGION)
    s3_prefix = f"s3://{s3.BASE_BUCKET}/"
    previous = query.get_peg_files(engine, study_id)

    new_keys = {}
    for field_name, file_type in PEG_UPLOAD_FIELDS:
        filename, contents, content_type = uploads[field_name]
        key = f"peg/{study_id}/{file_type}/{filename}"
        s3_client.put_object(
            Bucket=s3.BASE_BUCKET,
            Key=key,
            Body=contents,
            ContentType=content_type or DEFAULT_CONTENT_TYPES[file_type],
        )
        new_keys[field_name] = key

    for old in previous:
        query.delete_peg_file(engine, old['id'])
        old_key = old['file_path'][len(s3_prefix):] if old['file_path'].startswith(s3_prefix) else None
        if old_key and old_key not in new_keys.values():
            try:
                s3_client.delete_object(Bucket=s3.BASE_BUCKET, Key=old_key)
            except Exception:
                pass  # best-effort cleanup; the DB row is already gone

    saved = []
    for field_name, file_type in PEG_UPLOAD_FIELDS:
        filename, contents, _ = uploads[field_name]
        file_id = query.create_peg_file(
            engine=engine,
            study_id=study_id,
            file_type=file_type,
            file_name=filename or '',
            file_path=f"{s3_prefix}{new_keys[field_name]}",
            file_size=len(contents),
        )
        saved.append({
            "id": file_id,
            "study_id": str(study_id),
            "file_type": file_type,
            "file_name": filename,
            "file_path": f"{s3_prefix}{new_keys[field_name]}",
            "file_size": len(contents),
        })

    return {"files": saved, "report": report}


@router.get("/peg/studies/{study_id}/files")
async def get_peg_files(study_id: UUID, user: User = Depends(get_peg_user)):
    """Get all files for a PEG study"""
    study = query.get_peg_study(engine, study_id)
    if not study:
        raise fastapi.HTTPException(status_code=404, detail="Study not found")
    if not (study['created_by'] == user.user_name or check_review_permissions(user)):
        raise fastapi.HTTPException(status_code=403, detail="You can only view files for studies you created")
    files = query.get_peg_files(engine, study_id)
    return files


@router.get("/peg/studies/{study_id}/download")
async def download_peg_study_zip(study_id: UUID, user: User = Depends(get_peg_user)):
    """Download all files for a PEG study as a single zip archive."""
    study = query.get_peg_study(engine, study_id)
    if not study:
        raise fastapi.HTTPException(status_code=404, detail="Study not found")
    if not (study['created_by'] == user.user_name or check_review_permissions(user)):
        raise fastapi.HTTPException(status_code=403, detail="You can only download files from studies you created")

    files = query.get_peg_files(engine, study_id)
    if not files:
        raise fastapi.HTTPException(status_code=404, detail="No files have been uploaded for this study")

    s3_prefix = f"s3://{s3.BASE_BUCKET}/"
    s3_client = boto3.client('s3', region_name=s3.S3_REGION)
    buffer = io.BytesIO()

    with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        used_names = set()
        for file_info in files:
            s3_path = file_info['file_path']
            if not s3_path.startswith(s3_prefix):
                continue
            key = s3_path[len(s3_prefix):]
            obj = s3_client.get_object(Bucket=s3.BASE_BUCKET, Key=key)
            arcname = file_info['file_name']
            if arcname in used_names:
                stem, dot, ext = arcname.rpartition('.')
                arcname = f"{file_info['file_type']}_{arcname}" if not stem else f"{stem}_{file_info['file_type']}{dot}{ext}"
            used_names.add(arcname)
            zf.writestr(arcname, obj['Body'].read())

    buffer.seek(0)
    archive_name = f"{study['accession_id']}.zip"
    return StreamingResponse(
        buffer,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{archive_name}"'},
    )


@router.get("/peg/files/{file_id}")
async def download_peg_file(file_id: UUID, user: User = Depends(get_peg_user)):
    """Get download info for a PEG file (returns presigned S3 URL)"""
    try:
        # Get file info
        file_info = query.get_peg_file(engine, file_id)
        if not file_info:
            raise fastapi.HTTPException(status_code=404, detail="File not found")

        # Check ownership via parent study
        study = query.get_peg_study(engine, file_info['study_id'])
        if not study:
            raise fastapi.HTTPException(status_code=404, detail="Study not found")
        if not (study['created_by'] == user.user_name or check_review_permissions(user)):
            raise fastapi.HTTPException(status_code=403, detail="You can only download files from studies you created")

        # Get S3 path and create presigned URL
        s3_full_path = file_info['file_path']
        s3_path = s3_full_path.replace(f"s3://{s3.BASE_BUCKET}/", "")
        presigned_url = s3.get_signed_url(s3.BASE_BUCKET, s3_path)

        return {
            "presigned_url": presigned_url,
            "file_name": file_info['file_name'],
            "file_size": file_info['file_size']
        }
    except fastapi.HTTPException:
        raise
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"Error downloading file: {str(e)}")


@router.delete("/peg/files/{file_id}")
async def delete_peg_file(file_id: UUID, user: User = Depends(get_peg_user)):
    """Delete a PEG file"""
    file_info = query.get_peg_file(engine, file_id)
    if not file_info:
        raise fastapi.HTTPException(status_code=404, detail="File not found")
    study = query.get_peg_study(engine, file_info['study_id'])
    if not study:
        raise fastapi.HTTPException(status_code=404, detail="Study not found")
    if not (study['created_by'] == user.user_name or check_review_permissions(user)):
        raise fastapi.HTTPException(status_code=403, detail="You can only delete files from studies you created")
    query.delete_peg_file(engine, file_id)
    return {"message": "PEG file deleted successfully"}
