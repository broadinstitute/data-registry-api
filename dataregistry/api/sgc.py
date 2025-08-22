import json
import os

import fastapi
import pandas as pd
import io
from typing import Dict, List, Optional
from fastapi import UploadFile, Body, Query, Form, Depends, Header
from pydantic import BaseModel
from starlette.requests import Request

import httpx

from dataregistry.api import file_utils, s3, query
from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api.model import SGCPhenotype, SGCCohort, SGCCohortFile, User
from dataregistry.api.api import get_current_user

router = fastapi.APIRouter()
engine = DataRegistryReadWriteDB().get_engine()

USER_SERVICE_URL = os.getenv('USER_SERVICE_URL', 'https://users.kpndataregistry.org')

def check_review_permissions(user: User):
    return user.permissions and "sgc-review-data" in user.permissions


def get_valid_phenotype_codes() -> set:
    """Get set of valid phenotype codes from the database."""
    phenotypes = query.get_sgc_phenotypes(engine)
    return {phenotype.phenotype_code for phenotype in phenotypes}

async def get_sgc_user(authorization: Optional[str] = Header(None)):
    if not authorization:
        raise fastapi.HTTPException(status_code=401, detail='Authorization header required')
    
    schema, _, token = authorization.partition(' ')
    if schema.lower() != 'bearer' or not token:
        raise fastapi.HTTPException(status_code=401, detail='Bearer token required')
    
    sgc_user_group = os.getenv('SGC_USER_GROUP', 'sgc')
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{USER_SERVICE_URL}/api/auth/verify/",
                params={"group": sgc_user_group},
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


class SGCCasesControlsMapping(BaseModel):
    phenotype_column: str
    cases_column: str
    controls_column: str


class SGCCoOccurrenceMapping(BaseModel):
    phenotype1_column: str
    phenotype2_column: str
    num_individuals_column: str




def validate_sgc_cases_controls(df: pd.DataFrame, header_mapping: Dict[str, str], is_sample: bool = True) -> Optional[str]:
    required_cols = [header_mapping['phenotype'],
                    header_mapping['cases'],
                    header_mapping['controls']]
    
    # Check required columns exist
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        return f"Missing required columns: {missing_cols}"
    
    phenotype_col = header_mapping['phenotype']
    cases_col = header_mapping['cases']
    controls_col = header_mapping['controls']
    
    errors = []
    
    # Check phenotype column
    if df[phenotype_col].isna().any():
        errors.append(f"Column '{phenotype_col}' contains empty values")
    
    # Validate phenotype codes against database
    valid_phenotype_codes = get_valid_phenotype_codes()
    invalid_phenotypes = []
    for phenotype in df[phenotype_col].dropna():
        if phenotype not in valid_phenotype_codes:
            invalid_phenotypes.append(phenotype)
    
    if invalid_phenotypes:
        sample_invalid = invalid_phenotypes[:5]  # Show first 5
        error_msg = f"Invalid phenotype codes: {sample_invalid}"
        if len(invalid_phenotypes) > 5:
            error_msg += f" (and {len(invalid_phenotypes) - 5} more)"
        errors.append(error_msg)
    
    # Check for duplicate phenotypes
    duplicates = df[phenotype_col].value_counts()
    duplicates = duplicates[duplicates > 1]
    if not duplicates.empty:
        dup_list = duplicates.index.tolist()[:5]  # Show first 5
        error_msg = f"Duplicate phenotypes found: {dup_list}"
        if len(duplicates) > 5:
            error_msg += f" (and {len(duplicates) - 5} more)"
        errors.append(error_msg)
    
    # Validate cases column (positive integers)
    if df[cases_col].isna().any():
        errors.append(f"Column '{cases_col}' contains empty values")
    else:
        try:
            cases_numeric = pd.to_numeric(df[cases_col], errors='coerce')
            if cases_numeric.isna().any():
                errors.append(f"Column '{cases_col}' contains non-numeric values")
            elif (cases_numeric <= 0).any():
                errors.append(f"Column '{cases_col}' must contain only positive integers")
            elif not cases_numeric.equals(cases_numeric.astype(int)):
                errors.append(f"Column '{cases_col}' must contain integers, not decimals")
        except Exception:
            errors.append(f"Column '{cases_col}' validation failed")
    
    # Validate controls column (positive integers)
    if df[controls_col].isna().any():
        errors.append(f"Column '{controls_col}' contains empty values")
    else:
        try:
            controls_numeric = pd.to_numeric(df[controls_col], errors='coerce')
            if controls_numeric.isna().any():
                errors.append(f"Column '{controls_col}' contains non-numeric values")
            elif (controls_numeric <= 0).any():
                errors.append(f"Column '{controls_col}' must contain only positive integers")
            elif not controls_numeric.equals(controls_numeric.astype(int)):
                errors.append(f"Column '{controls_col}' must contain integers, not decimals")
        except Exception:
            errors.append(f"Column '{controls_col}' validation failed")
    
    return "; ".join(errors) if errors else None


def validate_sgc_co_occurrence(df: pd.DataFrame, header_mapping: Dict[str, str], is_sample: bool = True) -> Optional[str]:
    required_cols = [header_mapping['phenotype1_column'],
                    header_mapping['phenotype2_column'], 
                    header_mapping['num_individuals_column']]
    
    # Check required columns exist
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        return f"Missing required columns: {missing_cols}"
    
    phenotype1_col = header_mapping['phenotype1_column']
    phenotype2_col = header_mapping['phenotype2_column']
    num_individuals_col = header_mapping['num_individuals_column']
    
    errors = []
    
    # Check phenotype1 column
    if df[phenotype1_col].isna().any():
        errors.append(f"Column '{phenotype1_col}' contains empty values")
    
    # Check phenotype2 column
    if df[phenotype2_col].isna().any():
        errors.append(f"Column '{phenotype2_col}' contains empty values")
    
    # Validate phenotype codes against database
    valid_phenotype_codes = get_valid_phenotype_codes()
    
    # Validate phenotype1 codes
    invalid_phenotypes1 = []
    for phenotype in df[phenotype1_col].dropna():
        if phenotype not in valid_phenotype_codes:
            invalid_phenotypes1.append(phenotype)
    
    if invalid_phenotypes1:
        sample_invalid = invalid_phenotypes1[:5]
        error_msg = f"Invalid phenotype codes in {phenotype1_col}: {sample_invalid}"
        if len(invalid_phenotypes1) > 5:
            error_msg += f" (and {len(invalid_phenotypes1) - 5} more)"
        errors.append(error_msg)
    
    # Validate phenotype2 codes
    invalid_phenotypes2 = []
    for phenotype in df[phenotype2_col].dropna():
        if phenotype not in valid_phenotype_codes:
            invalid_phenotypes2.append(phenotype)
    
    if invalid_phenotypes2:
        sample_invalid = invalid_phenotypes2[:5]
        error_msg = f"Invalid phenotype codes in {phenotype2_col}: {sample_invalid}"
        if len(invalid_phenotypes2) > 5:
            error_msg += f" (and {len(invalid_phenotypes2) - 5} more)"
        errors.append(error_msg)
    
    # Check for duplicate phenotype pairs
    df_pairs = df[[phenotype1_col, phenotype2_col]].copy()
    # Create a standardized pair representation (sorted order to catch A,B and B,A as duplicates)
    df_pairs['pair'] = df_pairs.apply(lambda row: tuple(sorted([row[phenotype1_col], row[phenotype2_col]])), axis=1)
    duplicates = df_pairs['pair'].value_counts()
    duplicates = duplicates[duplicates > 1]
    if not duplicates.empty:
        dup_list = [f"({pair[0]}, {pair[1]})" for pair in duplicates.index.tolist()[:5]]
        error_msg = f"Duplicate phenotype pairs found: {dup_list}"
        if len(duplicates) > 5:
            error_msg += f" (and {len(duplicates) - 5} more)"
        errors.append(error_msg)
    
    # Validate num_individuals column (positive integers)
    if df[num_individuals_col].isna().any():
        errors.append(f"Column '{num_individuals_col}' contains empty values")
    else:
        try:
            num_numeric = pd.to_numeric(df[num_individuals_col], errors='coerce')
            if num_numeric.isna().any():
                errors.append(f"Column '{num_individuals_col}' contains non-numeric values")
            elif (num_numeric <= 0).any():
                errors.append(f"Column '{num_individuals_col}' must contain only positive integers")
            elif not num_numeric.equals(num_numeric.astype(int)):
                errors.append(f"Column '{num_individuals_col}' must contain integers, not decimals")
        except Exception:
            errors.append(f"Column '{num_individuals_col}' validation failed")
    
    return "; ".join(errors) if errors else None


@router.post("/sgc-preview-cases-controls")
async def preview_sgc_cases_controls(file: UploadFile, header_mapping: SGCCasesControlsMapping = Body(...)):
    contents = await file.read(100)
    await file.seek(0)

    if contents.startswith(b'\x1f\x8b'):
        sample_lines = await file_utils.get_compressed_sample(file)
    else:
        sample_lines = await file_utils.get_text_sample(file)

    df = await file_utils.parse_file(io.StringIO('\n'.join(sample_lines)), file.filename)
    
    # Validate using cases/controls rules
    error_message = validate_sgc_cases_controls(df, header_mapping.dict(), is_sample=True)
    
    if error_message:
        raise fastapi.HTTPException(
            status_code=400,
            detail={
                "message": "File does not meet SGC cases/controls format requirements",
                "errors": error_message
            }
        )
    
    return {
        "valid": True,
        "columns": list(df.columns),
        "sample_row_count": len(sample_lines) - 1,
        "validation_type": "cases_controls",
        "header_mapping": header_mapping.dict(),
        "message": "File format is valid for SGC cases/controls requirements"
    }


@router.post("/sgc-preview-co-occurrence")
async def preview_sgc_co_occurrence(file: UploadFile, header_mapping: SGCCoOccurrenceMapping = Body(...)):
    contents = await file.read(100)
    await file.seek(0)

    if contents.startswith(b'\x1f\x8b'):
        sample_lines = await file_utils.get_compressed_sample(file)
    else:
        sample_lines = await file_utils.get_text_sample(file)

    df = await file_utils.parse_file(io.StringIO('\n'.join(sample_lines)), file.filename)
    
    # Validate using co-occurrence rules
    error_message = validate_sgc_co_occurrence(df, header_mapping.dict(), is_sample=True)
    
    if error_message:
        raise fastapi.HTTPException(
            status_code=400,
            detail={
                "message": "File does not meet SGC co-occurrence format requirements",
                "errors": error_message
            }
        )
    
    return {
        "valid": True,
        "columns": list(df.columns),
        "sample_row_count": len(sample_lines) - 1,
        "validation_type": "co_occurrence",
        "header_mapping": header_mapping.dict(),
        "message": "File format is valid for SGC co-occurrence requirements"
    }


@router.post("/sgc-validate-s3-cases-controls")
async def validate_s3_cases_controls(
    s3_path: str = Body(..., description="S3 path to file (e.g., s3://bucket/key)"),
    header_mapping: SGCCasesControlsMapping = Body(...)
):
    return {
        "message": "S3 validation not yet implemented",
        "s3_path": s3_path,
        "validation_type": "cases_controls"
    }


@router.post("/sgc-validate-s3-co-occurrence") 
async def validate_s3_co_occurrence(
    s3_path: str = Body(..., description="S3 path to file (e.g., s3://bucket/key)"),
    header_mapping: SGCCoOccurrenceMapping = Body(...)
):
    """
    Validate full SGC co-occurrence file from S3.
    """
    # Read file from S3 and create DataFrame
    # This would need to be implemented based on your S3 access patterns
    # For now, returning a placeholder
    return {
        "message": "S3 validation not yet implemented", 
        "s3_path": s3_path,
        "validation_type": "co_occurrence"
    }


@router.get("/sgc/phenotypes")
async def get_all_sgc_phenotypes(user: User = Depends(get_sgc_user)):
    return query.get_sgc_phenotypes(engine)


@router.post("/sgc/phenotypes")
async def create_sgc_phenotype(phenotype_code: str = Body(...), description: str = Body(...), user: User = Depends(get_sgc_user)):
    if not check_review_permissions(user):
        raise fastapi.HTTPException(status_code=403, detail="You need sgc-review-data permission to add phenotypes")
    
    try:
        query.insert_sgc_phenotype(engine, phenotype_code, description)
        return {"message": "Phenotype created successfully", "phenotype_code": phenotype_code}
    except Exception as e:
        if "Duplicate entry" in str(e):
            raise fastapi.HTTPException(status_code=409, detail=f"Phenotype code '{phenotype_code}' already exists")
        raise fastapi.HTTPException(status_code=500, detail=f"Error creating phenotype: {str(e)}")


@router.delete("/sgc/phenotypes/{phenotype_code}")
async def delete_sgc_phenotype(phenotype_code: str, user: User = Depends(get_sgc_user)):
    if not check_review_permissions(user):
        raise fastapi.HTTPException(status_code=403, detail="You need sgc-review-data permission to delete phenotypes")
    
    deleted = query.delete_sgc_phenotype(engine, phenotype_code)
    if not deleted:
        raise fastapi.HTTPException(status_code=404, detail=f"Phenotype code '{phenotype_code}' not found")
    return {"message": f"Phenotype '{phenotype_code}' deleted successfully"}


@router.post("/sgc/cohorts")
async def upsert_sgc_cohort(cohort: SGCCohort, user: User = Depends(get_sgc_user)):
    try:
        # Set uploaded_by to current user if not provided
        if not cohort.uploaded_by:
            cohort.uploaded_by = user.user_name
            
        cohort_id = query.upsert_sgc_cohort(engine, cohort)
        return {
            "message": "Cohort saved successfully",
            "cohort_id": cohort_id,
            "name": cohort.name,
            "uploaded_by": cohort.uploaded_by
        }
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"Error saving cohort: {str(e)}")


@router.post("/sgc/cohort-files")
async def upload_and_create_sgc_cohort_file(
    file: UploadFile,
    cohort_id: str = Form(...),
    file_type: str = Form(...),
    validation_type: str = Form(...),
    column_mapping: str = Form(...),
    user: User = Depends(get_sgc_user)
):
    """
    Combined endpoint that validates file, uploads to S3, and creates cohort file record.
    """
    try:
        # Parse the column mapping JSON
        mapping = json.loads(column_mapping)
        
        # Read and validate file content
        content = await file.read()
        df = await file_utils.parse_file(io.StringIO(content.decode('utf-8')), file.filename)
        
        # Validate file content
        if validation_type == "cases_controls":
            error_message = validate_sgc_cases_controls(df, mapping, is_sample=False)
        elif validation_type == "cooccurrence":
            error_message = validate_sgc_co_occurrence(df, mapping, is_sample=False)
        else:
            raise fastapi.HTTPException(
                status_code=400,
                detail="validation_type must be 'cases_controls' or 'cooccurrence'"
            )
        
        if error_message:
            raise fastapi.HTTPException(detail=error_message, status_code=400)
        
        # Upload to S3
        s3_path = f"sgc/{cohort_id}/{file.filename}"
        await file.seek(0)  # Reset file pointer
        file_content = await file.read()
        
        # Upload to S3 using boto3
        import boto3
        s3_client = boto3.client('s3', region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'))
        bucket = os.getenv('S3_BUCKET', 'dig-data-registry-qa')
        
        s3_client.put_object(
            Bucket=bucket,
            Key=s3_path,
            Body=file_content,
            ContentType=file.content_type or 'application/octet-stream'
        )
        
        # Create cohort file record
        cohort_file = SGCCohortFile(
            cohort_id=cohort_id,
            file_type=file_type,
            file_path=f"s3://{bucket}/{s3_path}",
            file_name=file.filename,
            file_size=len(file_content)
        )
        
        file_id = query.insert_sgc_cohort_file(engine, cohort_file)
        
        return {
            "message": "File validated, uploaded, and saved successfully",
            "file_id": file_id,
            "cohort_id": cohort_id,
            "file_type": file_type,
            "file_name": file.filename,
            "file_path": cohort_file.file_path,
            "validation_type": validation_type,
            "file_size": cohort_file.file_size
        }
        
    except json.JSONDecodeError:
        raise fastapi.HTTPException(status_code=400, detail="Invalid column_mapping JSON")
    except fastapi.HTTPException:
        raise
    except Exception as e:
        if "Duplicate entry" in str(e):
            raise fastapi.HTTPException(
                status_code=409, 
                detail=f"A file of type '{file_type}' already exists for this cohort. Delete the existing file first."
            )
        elif "Cannot add or update a child row" in str(e) or "foreign key constraint fails" in str(e):
            raise fastapi.HTTPException(
                status_code=400,
                detail="Invalid cohort_id: cohort does not exist"
            )
        raise fastapi.HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")


@router.get("/sgc/cohorts")
async def get_sgc_cohorts(user: User = Depends(get_sgc_user)):
    """
    Get SGC cohorts with their associated files.
    - Users with 'sgc-review-data' permission can see all cohorts
    - Other users can only see cohorts they uploaded
    """
    try:
        # Check if user has review permissions to see all cohorts
        if check_review_permissions(user):
            # Reviewer can see all cohorts
            cohorts = query.get_sgc_cohorts_with_files(engine, uploaded_by=None)
        else:
            # Regular user can only see their own cohorts
            cohorts = query.get_sgc_cohorts_with_files(engine, uploaded_by=user.user_name)
        
        return cohorts
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"Error retrieving cohorts: {str(e)}")


@router.delete("/sgc/cohort-files/{file_id}")
async def delete_sgc_cohort_file(file_id: str, user: User = Depends(get_sgc_user)):
    """
    Delete an SGC cohort file.
    - Users can delete files from cohorts they uploaded
    - Users with 'sgc-review-data' permission can delete any file
    """
    try:
        # Get the owner of the file
        file_owner = query.get_sgc_cohort_file_owner(engine, file_id)
        if not file_owner:
            raise fastapi.HTTPException(status_code=404, detail="File not found")
        
        # Check permissions: either the user owns the file or has review permissions
        if not (file_owner == user.user_name or check_review_permissions(user)):
            raise fastapi.HTTPException(
                status_code=403, 
                detail="You can only delete files from cohorts you uploaded"
            )
        
        # Delete the file
        deleted = query.delete_sgc_cohort_file(engine, file_id)
        if not deleted:
            raise fastapi.HTTPException(status_code=404, detail="File not found")
        
        return {"message": "File deleted successfully", "file_id": file_id}
    except fastapi.HTTPException:
        raise
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"Error deleting file: {str(e)}")


