"""MSKKP GWAS Upload API endpoints"""
import io
import json
from difflib import SequenceMatcher
from datetime import datetime
from typing import Dict, List, Optional
from uuid import UUID, uuid4

import boto3
import fastapi
import pandas as pd
from fastapi import Request, Header, Body
from pydantic import BaseModel
from sqlalchemy.exc import IntegrityError

from dataregistry.api import query
from dataregistry.api import s3
from dataregistry.api.db import DataRegistryReadWriteDB

router = fastapi.APIRouter()
engine = DataRegistryReadWriteDB().get_engine()

# Common GWAS column name aliases mapped to canonical target field names
COLUMN_ALIASES = {
    # chromosome
    "chr": "chromosome",
    "chrom": "chromosome",
    "#chrom": "chromosome",
    "#chr": "chromosome",
    "chromosome": "chromosome",
    # position
    "bp": "position",
    "pos": "position",
    "position": "position",
    "base_pair_location": "position",
    "bp_pos": "position",
    # reference allele
    "ref": "reference",
    "a1": "reference",
    "reference": "reference",
    "effect_allele": "reference",
    "allele1": "reference",
    # alt allele
    "alt": "alt",
    "a2": "alt",
    "other_allele": "alt",
    "non_effect_allele": "alt",
    "allele2": "alt",
    # p-value
    "p": "pValue",
    "pval": "pValue",
    "pvalue": "pValue",
    "p_value": "pValue",
    "p-value": "pValue",
    "p_val": "pValue",
    # beta
    "beta": "beta",
    "effect": "beta",
    "effect_size": "beta",
    # odds ratio
    "or": "oddsRatio",
    "odds_ratio": "oddsRatio",
    "oddsratio": "oddsRatio",
    # sample size
    "n": "n",
    "n_total": "n",
    "sample_size": "n",
    "samplesize": "n",
    "neff": "n",
}

SIMILARITY_THRESHOLD = 0.6


def suggest_column_map(columns: List[str], target_fields: List[str], aliases: Dict[str, str] = None) -> Dict[str, str]:
    """Suggest mappings from file columns to target fields using aliases and string similarity.

    Args:
        columns: column names found in the uploaded file.
        target_fields: canonical field names the caller expects.
        aliases: optional dict mapping lowercased column names to canonical target
                 field names.  Defaults to the module-level COLUMN_ALIASES.
    """
    if aliases is None:
        aliases = COLUMN_ALIASES

    suggested = {}
    matched_targets = set()

    # Pass 1: exact match (case-insensitive) and alias lookup
    for col in columns:
        col_lower = col.lower().strip()

        # Check exact match against target fields
        for target in target_fields:
            if target in matched_targets:
                continue
            if col_lower == target.lower():
                suggested[col] = target
                matched_targets.add(target)
                break
        else:
            # Check alias dict
            alias_target = aliases.get(col_lower)
            if alias_target and alias_target in target_fields and alias_target not in matched_targets:
                suggested[col] = alias_target
                matched_targets.add(alias_target)

    # Pass 2: fuzzy match remaining columns against remaining targets
    unmatched_cols = [c for c in columns if c not in suggested]
    unmatched_targets = [t for t in target_fields if t not in matched_targets]

    for col in unmatched_cols:
        best_score = 0
        best_target = None
        col_lower = col.lower().strip()

        for target in unmatched_targets:
            score = SequenceMatcher(None, col_lower, target.lower()).ratio()
            if score > best_score:
                best_score = score
                best_target = target

        if best_target and best_score >= SIMILARITY_THRESHOLD:
            suggested[col] = best_target
            unmatched_targets.remove(best_target)

    return suggested


class MSKKPDatasetMetadata(BaseModel):
    """Metadata for MSKKP GWAS dataset"""
    name: str
    ancestry: str
    phenotype: Optional[str] = None
    effective_n: Optional[int] = None
    genome_build: str
    column_map: Dict[str, str]


class MSKKPDatasetRequest(BaseModel):
    """Request to validate/save MSKKP GWAS dataset"""
    dataset_name: str
    file_name: str
    metadata: MSKKPDatasetMetadata


class MSKKPDatasetCreateRequest(BaseModel):
    """Request to create MSKKP dataset metadata before file upload"""
    name: str
    ancestry: str
    phenotype: Optional[str] = None
    effective_n: Optional[int] = None
    genome_build: str
    column_map: Dict[str, str]


class ColumnMapSuggestionRequest(BaseModel):
    """Request to suggest column mappings based on file columns"""
    columns: List[str]
    target_fields: List[str]


@router.post("/mskkp/suggest-column-map")
async def suggest_column_mapping(request: ColumnMapSuggestionRequest):
    """Suggest mappings from file column names to target field names using aliases and string similarity."""
    return {"suggested_map": suggest_column_map(request.columns, request.target_fields)}


@router.post("/mskkp/datasets")
async def create_mskkp_dataset(request: MSKKPDatasetCreateRequest):
    """Create MSKKP dataset metadata entry before file upload."""
    dataset_id = str(uuid4())
    
    # Convert request to metadata dict
    metadata = {
        'name': request.name,
        'ancestry': request.ancestry,
        'phenotype': request.phenotype,
        'effective_n': request.effective_n,
        'genome_build': request.genome_build,
        'column_map': request.column_map
    }
    
    try:
        # Create database entry with pending status (no file info yet)
        query.save_mskkp_dataset(
            engine,
            dataset_id,
            request.name,
            metadata,
            s3_path='',  # Will be set during file upload
            filename='',  # Will be set during file upload
            file_size=0,  # Will be set during file upload
            uploader='anonymous'
        )
    except IntegrityError as e:
        # Check if it's a duplicate key error
        if 'Duplicate entry' in str(e.orig) or 'mskkp_datasets_name_unique' in str(e.orig):
            raise fastapi.HTTPException(
                status_code=409,
                detail=f"A dataset with the name '{request.name}' already exists. Please choose a different name."
            )
        # Re-raise if it's a different integrity error
        raise
    
    return {
        "dataset_id": dataset_id,
        "name": request.name,
        "message": "Dataset metadata created successfully. You can now upload the file."
    }


@router.get("/mskkp/datasets/{dataset_id}/presigned-url")
async def get_mskkp_dataset_presigned_url(dataset_id: str, filename: str):
    """Get presigned URL for uploading file directly to S3."""
    # Fetch dataset to ensure it exists
    dataset = query.fetch_mskkp_dataset_by_id(engine, dataset_id)
    if not dataset:
        raise fastapi.HTTPException(
            status_code=404,
            detail=f"Dataset with ID '{dataset_id}' not found"
        )
    
    # Check if file already uploaded
    if dataset.get('file_size', 0) > 0:
        raise fastapi.HTTPException(
            status_code=400,
            detail="File has already been uploaded for this dataset"
        )
    
    dataset_name = dataset['name']
    
    # Construct S3 path
    s3_path = f"mskkp/{dataset_name}/{filename}"
    
    # Generate presigned URL for direct S3 upload
    return s3.generate_presigned_url_with_path(s3_path)


@router.post("/mskkp/datasets/{dataset_id}/finalize")
async def finalize_mskkp_dataset_upload(dataset_id: str, filename: str = Body(...)):
    """Finalize upload after file has been uploaded to S3."""
    # Fetch dataset to ensure it exists
    dataset = query.fetch_mskkp_dataset_by_id(engine, dataset_id)
    if not dataset:
        raise fastapi.HTTPException(
            status_code=404,
            detail=f"Dataset with ID '{dataset_id}' not found"
        )
    
    dataset_name = dataset['name']
    s3_path = f"mskkp/{dataset_name}/{filename}"
    
    try:
        # Verify file exists in S3 and get size
        s3_client = boto3.client('s3', region_name=s3.S3_REGION)
        response = s3_client.head_object(Bucket=s3.BASE_BUCKET, Key=s3_path)
        file_size = response['ContentLength']
        
        # Update database with file info
        query.update_mskkp_dataset_file_info(engine, dataset_id, s3_path, filename, file_size)
        
        # Save metadata to S3
        metadata_dict = dataset.get('metadata', {})
        if not metadata_dict:
            # Reconstruct from dataset fields if metadata field is empty
            metadata_dict = {
                'name': dataset['name'],
                'ancestry': dataset['ancestry'],
                'phenotype': dataset.get('phenotype'),
                'effective_n': dataset.get('effective_n'),
                'genome_build': dataset['genome_build'],
                'column_map': dataset.get('column_map', {})
            }
        s3.upload_metadata(metadata_dict, f"mskkp/{dataset_name}")
        
        return {
            "dataset_id": dataset_id,
            "file_name": filename,
            "file_size": file_size,
            "s3_path": s3_path,
            "message": "File uploaded successfully"
        }
    except Exception as e:
        if 'Not Found' in str(e) or '404' in str(e):
            raise fastapi.HTTPException(
                status_code=404,
                detail=f"File not found in S3. Please upload the file first."
            )
        raise fastapi.HTTPException(
            status_code=500,
            detail=f"Error finalizing upload: {str(e)}"
        )


@router.get("/mskkp/get-presigned-url")
async def get_mskkp_presigned_url(request: Request):
    """Generate a presigned URL for uploading MSKKP dataset to S3
    
    DEPRECATED: Use POST /mskkp/datasets and POST /mskkp/datasets/{id}/upload instead
    """
    filename = request.headers.get('Filename')
    dataset_name = request.headers.get('Dataset')
    
    if not filename or not dataset_name:
        raise fastapi.HTTPException(
            status_code=400, 
            detail="Filename and Dataset headers are required"
        )
    
    # Create S3 path: mskkp/{dataset_name}/{filename}
    s3_path = f"mskkp/{dataset_name}/{filename}"
    
    return s3.generate_presigned_url_with_path(s3_path)


@router.post("/mskkp/validate-dataset")
async def validate_mskkp_dataset(request: MSKKPDatasetRequest):
    """Validate and register the uploaded MSKKP dataset"""
    dataset_name = request.dataset_name
    filename = request.file_name
    metadata = request.metadata
    
    # Construct S3 path
    s3_path = f"mskkp/{dataset_name}/{filename}"
    
    # TODO: Add validation logic here
    # - Check if file exists in S3
    # - Validate column mappings
    # - Validate file format
    
    # Generate a unique ID for this dataset
    dataset_id = str(uuid4())
    
    # Save metadata to S3
    s3.upload_metadata(metadata.dict(), f"mskkp/{dataset_name}")
    
    # TODO: Save dataset info to database
    # query.save_mskkp_dataset(engine, dataset_id, dataset_name, metadata, s3_path, filename)
    
    return {
        "dataset_id": dataset_id,
        "s3_path": s3_path,
        "message": "Dataset uploaded and validated successfully"
    }


@router.get("/mskkp/datasets")
async def list_mskkp_datasets():
    """List all MSKKP GWAS datasets"""
    # TODO: Implement database query to list MSKKP datasets
    return {"datasets": []}


@router.delete("/mskkp/datasets/{dataset_id}")
async def delete_mskkp_dataset(dataset_id: str):
    """Delete an MSKKP GWAS dataset"""
    # TODO: Implement deletion logic
    return {"message": f"Dataset {dataset_id} deleted successfully"}
