import io
import os
from datetime import datetime
from typing import Dict, List, Optional
from uuid import UUID

import boto3
import fastapi
import httpx
import pandas as pd
from fastapi import UploadFile, File, Depends, Header
from pydantic import BaseModel

from dataregistry.api import query
from dataregistry.api import s3
from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api.model import User

router = fastapi.APIRouter()
engine = DataRegistryReadWriteDB().get_engine()

USER_SERVICE_URL = os.getenv('USER_SERVICE_URL', 'https://users.kpndataregistry.org')


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
                    permissions=user.get('permissions', [])
                )
            else:
                raise fastapi.HTTPException(status_code=401, detail='Invalid token')
    except httpx.RequestError:
        raise fastapi.HTTPException(status_code=503, detail='User service unavailable')


class PEGStudyMetadata(BaseModel):
    """Metadata for a PEG study"""
    study_author: str


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


@router.get("/peg/is-logged-in")
async def peg_is_logged_in(user: User = Depends(get_peg_user)):
    """Check if the user is logged in to PEG."""
    if user:
        return user
    else:
        raise fastapi.HTTPException(status_code=401, detail='Not logged in')


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


@router.post("/peg/studies/{study_id}/peg-list")
async def upload_peg_list(study_id: UUID, file: UploadFile = File(...)):
    """Upload PEG list TSV file"""
    # Verify study exists
    study = query.get_peg_study(engine, study_id)
    if not study:
        raise fastapi.HTTPException(status_code=404, detail="Study not found")
    
    # Read and validate file
    try:
        contents = await file.read()
        df = pd.read_csv(io.BytesIO(contents), sep='\t')

        # Upload to S3 using boto3
        s3_path = f"peg/{study_id}/peg_list/{file.filename}"
        s3_client = boto3.client('s3', region_name=s3.S3_REGION)

        s3_client.put_object(
            Bucket=s3.BASE_BUCKET,
            Key=s3_path,
            Body=contents,
            ContentType=file.content_type or 'application/octet-stream'
        )

        # Save file record
        file_id = query.create_peg_file(
            engine=engine,
            study_id=study_id,
            file_type="peg_list",
            file_name=file.filename,
            file_path=f"s3://{s3.BASE_BUCKET}/{s3_path}",
            file_size=len(contents)
        )

        return {"id": file_id, "message": "PEG list uploaded successfully"}

    except pd.errors.ParserError:
        raise fastapi.HTTPException(status_code=400, detail="Invalid TSV format")
    except fastapi.HTTPException:
        raise
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")


@router.post("/peg/studies/{study_id}/peg-matrix")
async def upload_peg_matrix(study_id: UUID, file: UploadFile = File(...)):
    """Upload PEG matrix TSV file"""
    # Verify study exists
    study = query.get_peg_study(engine, study_id)
    if not study:
        raise fastapi.HTTPException(status_code=404, detail="Study not found")
    
    # Read and validate file
    try:
        contents = await file.read()
        df = pd.read_csv(io.BytesIO(contents), sep='\t')

        # Upload to S3 using boto3
        s3_path = f"peg/{study_id}/peg_matrix/{file.filename}"
        s3_client = boto3.client('s3', region_name=s3.S3_REGION)

        s3_client.put_object(
            Bucket=s3.BASE_BUCKET,
            Key=s3_path,
            Body=contents,
            ContentType=file.content_type or 'application/octet-stream'
        )

        # Save file record
        file_id = query.create_peg_file(
            engine=engine,
            study_id=study_id,
            file_type="peg_matrix",
            file_name=file.filename,
            file_path=f"s3://{s3.BASE_BUCKET}/{s3_path}",
            file_size=len(contents)
        )

        return {"id": file_id, "message": "PEG matrix uploaded successfully"}

    except pd.errors.ParserError:
        raise fastapi.HTTPException(status_code=400, detail="Invalid TSV format")
    except fastapi.HTTPException:
        raise
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")


@router.post("/peg/studies/{study_id}/peg-metadata")
async def upload_peg_metadata(study_id: UUID, file: UploadFile = File(...)):
    """Upload PEG metadata XLSX file with multiple sheets"""
    # Verify study exists
    study = query.get_peg_study(engine, study_id)
    if not study:
        raise fastapi.HTTPException(status_code=404, detail="Study not found")
    
    # Read and validate file
    try:
        contents = await file.read()
        
        # Only accept XLSX files
        if not file.filename.endswith('.xlsx'):
            raise fastapi.HTTPException(
                status_code=400, 
                detail="Invalid file format. Please upload an .xlsx file."
            )
        
        # Read the Excel file and check for required sheets
        xl_file = pd.ExcelFile(io.BytesIO(contents))
        
        # Expected sheets from the template
        expected_sheets = [
            'Dataset_description',
            'Genomic_identifier',
            'Evidence',
            'Integration',
            'source',
            'method'
        ]
        
        # Check if all required sheets are present
        missing_sheets = [sheet for sheet in expected_sheets if sheet not in xl_file.sheet_names]
        if missing_sheets:
            raise fastapi.HTTPException(
                status_code=400,
                detail=f"Missing required sheets: {', '.join(missing_sheets)}. Please use the provided template."
            )
        
        # Basic validation: ensure sheets have data
        for sheet_name in expected_sheets:
            df = pd.read_excel(xl_file, sheet_name=sheet_name)
            if len(df) == 0:
                raise fastapi.HTTPException(
                    status_code=400,
                    detail=f"Sheet '{sheet_name}' is empty. Please provide data for all sheets."
                )

        # Upload to S3 using boto3
        s3_path = f"peg/{study_id}/peg_metadata/{file.filename}"
        s3_client = boto3.client('s3', region_name=s3.S3_REGION)

        s3_client.put_object(
            Bucket=s3.BASE_BUCKET,
            Key=s3_path,
            Body=contents,
            ContentType=file.content_type or 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

        # Save file record
        file_id = query.create_peg_file(
            engine=engine,
            study_id=study_id,
            file_type="peg_metadata",
            file_name=file.filename,
            file_path=f"s3://{s3.BASE_BUCKET}/{s3_path}",
            file_size=len(contents)
        )

        return {"id": file_id, "message": "PEG metadata uploaded successfully"}

    except fastapi.HTTPException:
        raise
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")


@router.get("/peg/studies/{study_id}/files")
async def get_peg_files(study_id: UUID):
    """Get all files for a PEG study"""
    files = query.get_peg_files(engine, study_id)
    return files


@router.get("/peg/files/{file_id}")
async def download_peg_file(file_id: UUID):
    """Get download info for a PEG file (returns presigned S3 URL)"""
    try:
        # Get file info
        file_info = query.get_peg_file(engine, file_id)
        if not file_info:
            raise fastapi.HTTPException(status_code=404, detail="File not found")

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
async def delete_peg_file(file_id: UUID):
    """Delete a PEG file"""
    query.delete_peg_file(engine, file_id)
    return {"message": "PEG file deleted successfully"}
