import io
from datetime import datetime
from typing import Dict, List, Optional
from uuid import UUID

import boto3
import fastapi
import pandas as pd
from fastapi import UploadFile, File
from pydantic import BaseModel

from dataregistry.api import query
from dataregistry.api import s3
from dataregistry.api.db import DataRegistryReadWriteDB

router = fastapi.APIRouter()
engine = DataRegistryReadWriteDB().get_engine()

# Expected column names for validation
PEG_LIST_COLUMNS = [
    'rsID', 'Gene', 'GWAS', 'FM', 'PROX', 'FUNC', 'QTL', 
    'PHEWAS', 'GENEBASE', 'PERTUB', 'DB', 'Author_conclusion'
]

PEG_MATRIX_COLUMNS = [
    'rsID', 'Locus_name', 'Locus_number', 'Gene_symbol', 'GWAS_pvalue', 
    'GWAS_beta', 'FM_PPA', 'FM_FGWAS_Most_enriched_tissue', 'PROX', 
    'FUNC_VEP_consequence', 'QTL_eQTL_gtex_pvalue', 'QTL_eQTL_gtex_slope', 
    'QTL_eQTL_gtex_tissue', 'PHEWAS_ukbb_diseases', 'GENEBASE_rare5_SKATO', 
    'PERTURB_mouse_phenotype', 'PERTURB_mouse_model', 'DB_ClinVar', 
    'INT_PoPS_score', 'INT_PoPS_feature1', 'INT_author_conclusion'
]


class PEGStudyMetadata(BaseModel):
    """Metadata for a PEG study"""
    # Dataset Description
    peg_source: str
    gwas_source: str
    trait_description: str
    trait_ontology_id: Optional[str] = None
    
    # Genomic Identifier
    variant_type: str
    genome_build: str
    variant_information: Optional[str] = None
    gene_information: Optional[str] = None
    
    # Evidence streams and integration (stored as JSON)
    evidence_streams: Optional[List[Dict]] = None
    integration_analyses: Optional[List[Dict]] = None


class PEGStudy(BaseModel):
    """PEG Study response model"""
    id: UUID
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


def validate_tsv_columns(df: pd.DataFrame, expected_columns: List[str], file_type: str) -> Optional[str]:
    """Validate that TSV has the expected columns

    Returns:
        Error message if validation fails, None if successful
    """
    actual_columns = df.columns.tolist()

    # Check for missing required columns (extra columns are allowed and will be ignored)
    missing_cols = [col for col in expected_columns if col not in actual_columns]

    if missing_cols:
        return f"{file_type} validation failed: Missing required columns: {', '.join(missing_cols)}"

    return None


def validate_peg_list(df: pd.DataFrame) -> Optional[str]:
    """Validate PEG list file contents
    
    Returns:
        Error message if validation fails, None if successful
    """
    # Check column names
    column_error = validate_tsv_columns(df, PEG_LIST_COLUMNS, "PEG List")
    if column_error:
        return column_error
    
    errors = []
    
    # Check for empty values in required columns
    if df['rsID'].isna().any():
        errors.append("rsID column contains empty values")
    if df['Gene'].isna().any():
        errors.append("Gene column contains empty values")
    
    # Check Author_conclusion is numeric
    try:
        pd.to_numeric(df['Author_conclusion'], errors='raise')
    except (ValueError, TypeError):
        errors.append("Author_conclusion column must contain numeric values")
    
    if errors:
        return "PEG List validation failed: " + "; ".join(errors)
    
    return None


def validate_peg_matrix(df: pd.DataFrame) -> Optional[str]:
    """Validate PEG matrix file contents
    
    Returns:
        Error message if validation fails, None if successful
    """
    # Check column names
    column_error = validate_tsv_columns(df, PEG_MATRIX_COLUMNS, "PEG Matrix")
    if column_error:
        return column_error
    
    errors = []
    
    # Check for empty values in required identifier columns
    required_id_cols = ['rsID', 'Locus_name', 'Locus_number', 'Gene_symbol']
    for col in required_id_cols:
        if df[col].isna().any():
            errors.append(f"{col} column contains empty values")
    
    # Validate numeric columns (NA values are allowed, but other non-numeric values are not)
    numeric_cols = ['GWAS_pvalue', 'GWAS_beta', 'QTL_eQTL_gtex_pvalue',
                    'QTL_eQTL_gtex_slope', 'INT_PoPS_score']
    for col in numeric_cols:
        if col in df.columns and not df[col].isna().all():
            # Count original NA values
            original_na_count = df[col].isna().sum()
            # Convert to numeric, coercing invalid values to NaN
            converted = pd.to_numeric(df[col], errors='coerce')
            # Count NAs after conversion
            converted_na_count = converted.isna().sum()
            # If more NAs after conversion, we had non-numeric non-NA values
            if converted_na_count > original_na_count:
                errors.append(f"{col} column contains non-numeric values")
    
    if errors:
        return "PEG Matrix validation failed: " + "; ".join(errors)
    
    return None


@router.post("/peg/studies")
async def create_peg_study(request: CreatePEGStudyRequest):
    """Create a new PEG study"""
    try:
        study_id = query.create_peg_study(
            engine=engine,
            name=request.name,
            created_by="anonymous",  # TODO: Add auth later
            metadata=request.metadata.dict()
        )

        return {"id": study_id, "message": "PEG study created successfully"}
    except Exception as e:
        # Check for duplicate key error
        if "Duplicate entry" in str(e) and "idx_unique_name" in str(e):
            raise fastapi.HTTPException(
                status_code=400,
                detail=f"A study with the name '{request.name}' already exists. Please choose a different name."
            )
        raise


@router.get("/peg/studies")
async def list_peg_studies():
    """List all PEG studies"""
    studies = query.get_peg_studies(engine)
    return studies


@router.get("/peg/studies/{study_id}")
async def get_peg_study(study_id: UUID):
    """Get a specific PEG study"""
    study = query.get_peg_study(engine, study_id)
    if not study:
        raise fastapi.HTTPException(status_code=404, detail="Study not found")
    return study


@router.patch("/peg/studies/{study_id}")
async def update_peg_study(study_id: UUID, request: CreatePEGStudyRequest):
    """Update a PEG study's metadata"""
    query.update_peg_study(
        engine=engine,
        study_id=study_id,
        name=request.name,
        metadata=request.metadata.dict()
    )
    return {"message": "PEG study updated successfully"}


@router.delete("/peg/studies/{study_id}")
async def delete_peg_study(study_id: UUID):
    """Delete a PEG study and all its files"""
    query.delete_peg_study(engine, study_id)
    return {"message": "PEG study deleted successfully"}


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
        
        # Validate
        validation_error = validate_peg_list(df)
        if validation_error:
            raise fastapi.HTTPException(status_code=400, detail=validation_error)
        
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
        
        # Validate
        validation_error = validate_peg_matrix(df)
        if validation_error:
            raise fastapi.HTTPException(status_code=400, detail=validation_error)
        
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
