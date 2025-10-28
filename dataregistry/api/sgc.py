import json
import os

import fastapi
import pandas as pd
import io
from typing import Dict, List, Optional, Tuple
from fastapi import UploadFile, Body, Query, Form, Depends, Header
from pydantic import BaseModel
from starlette.requests import Request

import httpx

from dataregistry.api import file_utils, s3, query
from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api.model import SGCPhenotype, SGCCohort, SGCCohortFile, SGCCasesControlsMetadata, SGCCoOccurrenceMetadata, SGCPhenotypeCaseTotals, SGCPhenotypeCaseCountsBySex, User, NewUserRequest
from dataregistry.api.api import get_current_user

router = fastapi.APIRouter()
engine = DataRegistryReadWriteDB().get_engine()

USER_SERVICE_URL = os.getenv('USER_SERVICE_URL', 'https://users.kpndataregistry.org')
UPLOADER_TOKEN = os.getenv('SGC_UPLOADER_TOKEN')
REVIEWER_TOKEN = os.getenv('SGC_REVIEWER_TOKEN')

def check_review_permissions(user: User):
    return user.permissions and "sgc-review-data" in user.permissions


def check_add_user_permissions(user: User):
    return user.permissions and "sgc-add-user" in user.permissions


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




def validate_sgc_cases_controls(df: pd.DataFrame, header_mapping: Dict[str, str]) -> Optional[str]:
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
        error_msg = f"Invalid phenotype codes: {invalid_phenotypes}"
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


def validate_sgc_co_occurrence(df: pd.DataFrame, header_mapping: Dict[str, str]) -> Optional[str]:
    required_cols = [header_mapping['phenotype1'],
                    header_mapping['phenotype2'],
                    header_mapping['cooccurrence_count']]
    
    # Check required columns exist
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        return f"Missing required columns: {missing_cols}"
    
    phenotype1_col = header_mapping['phenotype1']
    phenotype2_col = header_mapping['phenotype2']
    num_individuals_col = header_mapping['cooccurrence_count']
    
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
        error_msg = f"Invalid phenotype codes in {phenotype1_col}: {invalid_phenotypes1}"
        errors.append(error_msg)
    
    # Validate phenotype2 codes
    invalid_phenotypes2 = []
    for phenotype in df[phenotype2_col].dropna():
        if phenotype not in valid_phenotype_codes:
            invalid_phenotypes2.append(phenotype)
    
    if invalid_phenotypes2:
        error_msg = f"Invalid phenotype codes in {phenotype2_col}: {invalid_phenotypes2}"
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


def extract_cases_controls_metadata(df: pd.DataFrame, header_mapping: Dict[str, str]) -> SGCCasesControlsMetadata:
    """Extract metadata from a cases/controls file."""
    phenotype_col = header_mapping['phenotype']
    cases_col = header_mapping['cases']
    controls_col = header_mapping['controls']

    # Get distinct phenotypes
    distinct_phenotypes = df[phenotype_col].dropna().unique().tolist()

    # Calculate totals
    total_cases = pd.to_numeric(df[cases_col], errors='coerce').sum()
    total_controls = pd.to_numeric(df[controls_col], errors='coerce').sum()

    # Extract per-phenotype counts
    phenotype_counts = {}
    for _, row in df.iterrows():
        phenotype = row[phenotype_col]
        if pd.notna(phenotype):
            cases_count = pd.to_numeric(row[cases_col], errors='coerce')
            controls_count = pd.to_numeric(row[controls_col], errors='coerce')

            if not pd.isna(cases_count) and not pd.isna(controls_count):
                phenotype_counts[phenotype] = {
                    "cases": int(cases_count),
                    "controls": int(controls_count)
                }

    return SGCCasesControlsMetadata(
        file_id=None,  # Will be set when file is created
        distinct_phenotypes=distinct_phenotypes,
        total_cases=int(total_cases) if not pd.isna(total_cases) else 0,
        total_controls=int(total_controls) if not pd.isna(total_controls) else 0,
        phenotype_counts=phenotype_counts
    )


def extract_cooccurrence_metadata(df: pd.DataFrame, header_mapping: Dict[str, str]) -> SGCCoOccurrenceMetadata:
    """Extract metadata from a co-occurrence file."""
    phenotype1_col = header_mapping['phenotype1']
    phenotype2_col = header_mapping['phenotype2']
    cooccurrence_count_col = header_mapping['cooccurrence_count']

    # Get all distinct phenotypes from both columns
    phenotypes1 = set(df[phenotype1_col].dropna().unique())
    phenotypes2 = set(df[phenotype2_col].dropna().unique())
    distinct_phenotypes = sorted(phenotypes1.union(phenotypes2))

    # Calculate totals
    total_pairs = len(df)
    total_cooccurrence_count = pd.to_numeric(df[cooccurrence_count_col], errors='coerce').sum()

    # Extract per-phenotype-pair counts
    phenotype_pair_counts = {}
    for _, row in df.iterrows():
        phenotype1 = row[phenotype1_col]
        phenotype2 = row[phenotype2_col]
        cooccur_count = row[cooccurrence_count_col]

        if pd.notna(phenotype1) and pd.notna(phenotype2) and pd.notna(cooccur_count):
            # Create standardized pair key (sorted order)
            pair_key = '|'.join(sorted([str(phenotype1), str(phenotype2)]))
            cooccur_count_int = pd.to_numeric(cooccur_count, errors='coerce')
            if not pd.isna(cooccur_count_int):
                phenotype_pair_counts[pair_key] = int(cooccur_count_int)

    return SGCCoOccurrenceMetadata(
        file_id=None,  # Will be set when file is created
        distinct_phenotypes=distinct_phenotypes,
        total_pairs=total_pairs,
        total_cooccurrence_count=int(total_cooccurrence_count) if not pd.isna(total_cooccurrence_count) else 0,
        phenotype_pair_counts=phenotype_pair_counts
    )


def extract_cooccurrence_phenotypes(df: pd.DataFrame, header_mapping: Dict[str, str]) -> List[str]:
    """Extract distinct phenotypes from a co-occurrence file."""
    phenotype1_col = header_mapping['phenotype1']
    phenotype2_col = header_mapping['phenotype2']
    
    # Get all distinct phenotypes from both columns
    phenotypes1 = set(df[phenotype1_col].dropna().unique())
    phenotypes2 = set(df[phenotype2_col].dropna().unique())
    return sorted(phenotypes1.union(phenotypes2))


def derive_both_cases_controls_metadata(male_metadata: dict, female_metadata: dict) -> SGCCasesControlsMetadata:
    """Derive 'both' cases/controls metadata by combining male and female data."""
    # Combine distinct phenotypes from both files
    male_phenotypes = set(male_metadata.get('distinct_phenotypes', []))
    female_phenotypes = set(female_metadata.get('distinct_phenotypes', []))
    distinct_phenotypes = sorted(male_phenotypes.union(female_phenotypes))
    
    # Sum totals
    total_cases = male_metadata.get('total_cases', 0) + female_metadata.get('total_cases', 0)
    total_controls = male_metadata.get('total_controls', 0) + female_metadata.get('total_controls', 0)
    
    # Combine phenotype-specific counts
    male_phenotype_counts = male_metadata.get('phenotype_counts', {})
    female_phenotype_counts = female_metadata.get('phenotype_counts', {})
    
    combined_phenotype_counts = {}
    for phenotype in distinct_phenotypes:
        male_data = male_phenotype_counts.get(phenotype, {'cases': 0, 'controls': 0})
        female_data = female_phenotype_counts.get(phenotype, {'cases': 0, 'controls': 0})
        
        combined_phenotype_counts[phenotype] = {
            'cases': male_data.get('cases', 0) + female_data.get('cases', 0),
            'controls': male_data.get('controls', 0) + female_data.get('controls', 0)
        }
    
    return SGCCasesControlsMetadata(
        file_id=None,  # Will be set when file is created
        distinct_phenotypes=distinct_phenotypes,
        total_cases=total_cases,
        total_controls=total_controls,
        phenotype_counts=combined_phenotype_counts
    )


def derive_both_cooccurrence_metadata(male_metadata: dict, female_metadata: dict) -> SGCCoOccurrenceMetadata:
    """Derive 'both' co-occurrence metadata by combining male and female data."""
    # Combine distinct phenotypes from both files
    male_phenotypes = set(male_metadata.get('distinct_phenotypes', []))
    female_phenotypes = set(female_metadata.get('distinct_phenotypes', []))
    distinct_phenotypes = sorted(male_phenotypes.union(female_phenotypes))
    
    # Sum totals
    total_pairs = male_metadata.get('total_pairs', 0) + female_metadata.get('total_pairs', 0)
    total_cooccurrence_count = male_metadata.get('total_cooccurrence_count', 0) + female_metadata.get('total_cooccurrence_count', 0)
    
    # Combine phenotype pair counts
    male_pair_counts = male_metadata.get('phenotype_pair_counts', {})
    female_pair_counts = female_metadata.get('phenotype_pair_counts', {})
    
    combined_pair_counts = {}
    all_pairs = set(male_pair_counts.keys()).union(set(female_pair_counts.keys()))
    
    for pair_key in all_pairs:
        male_count = male_pair_counts.get(pair_key, 0)
        female_count = female_pair_counts.get(pair_key, 0)
        combined_pair_counts[pair_key] = male_count + female_count
    
    return SGCCoOccurrenceMetadata(
        file_id=None,  # Will be set when file is created
        distinct_phenotypes=distinct_phenotypes,
        total_pairs=total_pairs,
        total_cooccurrence_count=total_cooccurrence_count,
        phenotype_pair_counts=combined_pair_counts
    )


async def combine_two_files(male_file_path: str, female_file_path: str, male_mapping: dict = None, female_mapping: dict = None) -> Tuple[str, str]:
    male_key = male_file_path.replace(f"s3://{s3.BASE_BUCKET}/", "")
    female_key = female_file_path.replace(f"s3://{s3.BASE_BUCKET}/", "")
    
    male_ext = male_key.split('.')[-1].lower()
    female_ext = female_key.split('.')[-1].lower()
    
    # Determine separator for each file based on its extension
    if male_ext in ['tsv', 'txt']:
        male_separator = '\t'
    elif male_ext == 'csv':
        male_separator = ','
    else:
        male_separator = ','
    
    if female_ext in ['tsv', 'txt']:
        female_separator = '\t'
    elif female_ext == 'csv':
        female_separator = ','
    else:
        female_separator = ','
    
    try:
        male_response = s3.get_file_obj(male_key, s3.BASE_BUCKET)
        male_content = male_response['Body'].read().decode('utf-8')
        
        female_response = s3.get_file_obj(female_key, s3.BASE_BUCKET)
        female_content = female_response['Body'].read().decode('utf-8')
        
        male_df = pd.read_csv(io.StringIO(male_content), sep=male_separator)
        female_df = pd.read_csv(io.StringIO(female_content), sep=female_separator)
        
        # Standardize column names using the mappings if provided
        if male_mapping and female_mapping:
            # For cases_controls files, expect: phenotype, cases, controls
            # For cooccurrence files, expect: phenotype1, phenotype2, cooccurrence_count
            if 'phenotype' in male_mapping and 'cases' in male_mapping:
                # Cases/controls mapping
                male_df = male_df.rename(columns={
                    male_mapping['phenotype']: 'phenotype',
                    male_mapping['cases']: 'cases', 
                    male_mapping['controls']: 'controls'
                })
                female_df = female_df.rename(columns={
                    female_mapping['phenotype']: 'phenotype',
                    female_mapping['cases']: 'cases',
                    female_mapping['controls']: 'controls'
                })
            elif 'phenotype1' in male_mapping and 'cooccurrence_count' in male_mapping:
                # Co-occurrence mapping  
                male_df = male_df.rename(columns={
                    male_mapping['phenotype1']: 'phenotype1',
                    male_mapping['phenotype2']: 'phenotype2',
                    male_mapping['cooccurrence_count']: 'cooccurrence_count'
                })
                female_df = female_df.rename(columns={
                    female_mapping['phenotype1']: 'phenotype1', 
                    female_mapping['phenotype2']: 'phenotype2',
                    female_mapping['cooccurrence_count']: 'cooccurrence_count'
                })
        
        combined_df = pd.concat([male_df, female_df], ignore_index=True)
        combined_content = combined_df.to_csv(sep='\t', index=False)
        
        return combined_content, 'tsv'
        
    except Exception as e:
        raise Exception(f"Error combining files {male_key} and {female_key}: {str(e)}")


async def generate_both_file_from_male_female(
    cohort_id: str,
    files_by_type: dict,
    file_type_prefix: str  # "cases_controls" or "cooccurrence"
) -> None:
    """Generate a 'both' file by combining male and female files of the same type."""
    
    male_type = f"{file_type_prefix}_male"
    female_type = f"{file_type_prefix}_female"
    both_type = f"{file_type_prefix}_both"
    
    # Check if both male and female files exist
    if male_type not in files_by_type or female_type not in files_by_type:
        return
    
    # Check if 'both' file already exists and delete it
    if both_type in files_by_type:
        both_file_id = files_by_type[both_type]['file_id']
        query.delete_sgc_cases_controls_metadata(engine, both_file_id)
        query.delete_sgc_cooccurrence_metadata(engine, both_file_id)
        query.delete_sgc_cohort_file(engine, both_file_id)
    
    # Combine the actual file contents
    male_file_path = files_by_type[male_type]['file_path']
    female_file_path = files_by_type[female_type]['file_path']
    male_mapping = files_by_type[male_type].get('column_mapping')
    female_mapping = files_by_type[female_type].get('column_mapping')
    combined_content, file_ext = await combine_two_files(male_file_path, female_file_path, male_mapping, female_mapping)
    
    # Always output as TSV regardless of input formats
    content_type = 'text/tab-separated-values'
    separator = '\t'
    file_ext = 'tsv'
    
    # Upload combined file to S3 using existing utilities
    s3_path = f"sgc/{cohort_id}/{both_type}/combined_{both_type}.{file_ext}"
    bucket = s3.BASE_BUCKET
    
    # Use boto3 directly since s3 module doesn't have a put_object wrapper
    import boto3
    s3_client = boto3.client('s3', region_name=s3.S3_REGION)
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_path,
        Body=combined_content.encode('utf-8'),
        ContentType=content_type
    )
    
    # Create cohort file record for the combined file
    cohort_file = SGCCohortFile(
        cohort_id=cohort_id,
        file_type=both_type,
        file_path=f"s3://{bucket}/{s3_path}",
        file_name=f"combined_{both_type}.{file_ext}",
        file_size=len(combined_content.encode('utf-8'))
    )
    
    both_file_id = query.insert_sgc_cohort_file(engine, cohort_file)
    
    # Parse combined content and extract metadata
    df = pd.read_csv(io.StringIO(combined_content), sep=separator)
    
    if file_type_prefix == "cases_controls":
        # Assume standard column structure: phenotype, cases, controls
        header_mapping = {'phenotype': df.columns[0], 'cases': df.columns[1], 'controls': df.columns[2]}
        both_metadata = extract_cases_controls_metadata(df, header_mapping)
        both_metadata.file_id = both_file_id
        query.insert_sgc_cases_controls_metadata(engine, both_metadata)
    elif file_type_prefix == "cooccurrence":
        # Assume standard column structure: phenotype1, phenotype2, cooccurrence_count
        header_mapping = {'phenotype1': df.columns[0], 'phenotype2': df.columns[1], 'cooccurrence_count': df.columns[2]}
        both_metadata = extract_cooccurrence_metadata(df, header_mapping)
        both_metadata.file_id = both_file_id
        query.insert_sgc_cooccurrence_metadata(engine, both_metadata)


def validate_cases_controls_file_consistency(cohort_id: str) -> Optional[str]:
    try:
        cohort_files = query.get_sgc_cohort_by_id(engine, cohort_id)
        if not cohort_files:
            return "cases/controls check: Cohort not found"
        
        files_by_type = {}
        metadata_by_type = {}
        
        for file_data in cohort_files:
            if file_data.get('file_type') and file_data['file_type'].startswith('cases_controls'):
                file_type = file_data['file_type']
                files_by_type[file_type] = file_data
                
                if file_data.get('file_id'):
                    try:
                        cc_metadata = query.get_sgc_cases_controls_metadata(engine, file_data['file_id'])
                        if cc_metadata:
                            metadata_by_type[file_type] = cc_metadata[0] if isinstance(cc_metadata, list) else cc_metadata
                    except Exception:
                        pass
        
        required_types = {'cases_controls_male', 'cases_controls_female', 'cases_controls_both'}
        available_types = set(files_by_type.keys())
        missing_types = required_types - available_types
        
        if missing_types:
            return f"cases/controls check: Missing required file types: {sorted(list(missing_types))}"
        

        return None
        
    except Exception as e:
        return f"cases/controls check: Error during validation - {str(e)}"


def validate_cooccurrence_file_consistency(cohort_id: str) -> Optional[str]:
    try:
        cohort_files = query.get_sgc_cohort_by_id(engine, cohort_id)
        if not cohort_files:
            return "co-occurrence check: Cohort not found"

        files_by_type = {}
        metadata_by_type = {}

        for file_data in cohort_files:
            if file_data.get('file_type') and file_data['file_type'].startswith('cooccurrence'):
                file_type = file_data['file_type']
                files_by_type[file_type] = file_data

                if file_data.get('file_id'):
                    try:
                        cooccur_metadata = query.get_sgc_cooccurrence_metadata(engine, file_data['file_id'])
                        if cooccur_metadata:
                            metadata_by_type[file_type] = cooccur_metadata[0] if isinstance(cooccur_metadata, list) else cooccur_metadata
                    except Exception:
                        pass

        required_types = {'cooccurrence_male', 'cooccurrence_female', 'cooccurrence_both'}
        available_types = set(files_by_type.keys())
        missing_types = required_types - available_types

        if missing_types:
            return f"co-occurrence check: Missing required file types: {sorted(list(missing_types))}"
        

        return None

    except Exception as e:
        return f"co-occurrence check: Error during validation - {str(e)}"


def validate_cohort_cross_file_consistency(cohort_id: str) -> Optional[str]:
    try:
        cohort_files = query.get_sgc_cohort_by_id(engine, cohort_id)
        if not cohort_files:
            return "co-occurrence + cases/controls check: Cohort not found"

        cases_controls_metadata = {}
        cooccurrence_metadata = {}

        for file_data in cohort_files:
            file_type = file_data.get('file_type')
            file_id = file_data.get('file_id')

            if file_type and file_type.startswith('cases_controls'):
                if file_id:
                    try:
                        cc_metadata = query.get_sgc_cases_controls_metadata(engine, file_id)
                        if cc_metadata:
                            cases_controls_metadata[file_type] = cc_metadata[0] if isinstance(cc_metadata, list) else cc_metadata
                    except Exception:
                        pass

            elif file_type and file_type.startswith('cooccurrence'):
                if file_id:
                    try:
                        cooccur_metadata = query.get_sgc_cooccurrence_metadata(engine, file_id)
                        if cooccur_metadata:
                            cooccurrence_metadata[file_type] = cooccur_metadata[0] if isinstance(cooccur_metadata, list) else cooccur_metadata
                    except Exception:
                        pass

        file_type_mappings = {
            'cooccurrence_male': 'cases_controls_male',
            'cooccurrence_female': 'cases_controls_female',
            'cooccurrence_both': 'cases_controls_both'
        }

        for cooccur_type, cases_type in file_type_mappings.items():
            if cooccur_type in cooccurrence_metadata and cases_type in cases_controls_metadata:
                cooccur_phenotypes = set(cooccurrence_metadata[cooccur_type].get('distinct_phenotypes', []))
                cases_phenotypes = set(cases_controls_metadata[cases_type].get('distinct_phenotypes', []))

                missing_phenotypes = cooccur_phenotypes - cases_phenotypes
                if missing_phenotypes:
                    sample_missing = sorted(list(missing_phenotypes))[:5]
                    error_msg = f"co-occurrence + cases/controls check: {cooccur_type} file references phenotypes not found in {cases_type} file: {sample_missing}"
                    if len(missing_phenotypes) > 5:
                        error_msg += f" (and {len(missing_phenotypes) - 5} more)"
                    return error_msg
                
                # Validate that co-occurrence counts don't exceed cases counts
                cooccur_meta = cooccurrence_metadata[cooccur_type]
                cases_meta = cases_controls_metadata[cases_type]
                
                phenotype_pair_counts = cooccur_meta.get('phenotype_pair_counts', {})
                phenotype_counts = cases_meta.get('phenotype_counts', {})
                
                violations = []
                
                for pair_key, cooccur_count in phenotype_pair_counts.items():
                    phenotypes = pair_key.split('|')
                    if len(phenotypes) != 2:
                        continue
                    
                    phenotype1, phenotype2 = phenotypes
                    
                    cases1 = phenotype_counts.get(phenotype1, {}).get('cases', 0)
                    cases2 = phenotype_counts.get(phenotype2, {}).get('cases', 0)
                    
                    # Co-occurrence count cannot exceed the minimum of the two cases counts
                    min_cases = min(cases1, cases2) if cases1 > 0 and cases2 > 0 else 0
                    
                    if min_cases > 0 and cooccur_count > min_cases:
                        violations.append(
                            f"({phenotype1}, {phenotype2}): cooccur={cooccur_count} > min(cases={cases1}, cases={cases2})"
                        )
                
                if violations:
                    sample_violations = violations[:5]
                    error_msg = f"co-occurrence + cases/controls check: {cooccur_type} has counts exceeding cases: {sample_violations}"
                    if len(violations) > 5:
                        error_msg += f" (and {len(violations) - 5} more)"
                    return error_msg

        return None

    except Exception as e:
        return f"co-occurrence + cases/controls check: Error during validation - {str(e)}"




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
    error_message = validate_sgc_cases_controls(df, header_mapping.dict())
    
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
    error_message = validate_sgc_co_occurrence(df, header_mapping.dict())
    
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
    from sqlalchemy.exc import IntegrityError
    
    try:
        # Set uploaded_by to current user if not provided
        if not cohort.uploaded_by:
            cohort.uploaded_by = user.user_name
            
        cohort_id = query.upsert_sgc_cohort(engine, cohort)
        
        # Determine if this was a create or update based on whether ID was provided
        was_update = cohort.id is not None
        
        # If this is an update, reset validation status since metadata may have changed
        if was_update:
            query.update_sgc_cohort_validation_status(engine, cohort_id, False)
        
        if was_update:
            # For updates, return the full cohort data (same as get_sgc_cohort_by_id)
            cohort_data = query.get_sgc_cohort_by_id(engine, cohort_id)
            if cohort_data:
                return cohort_data
            else:
                # Fallback in case the cohort wasn't found after update
                raise fastapi.HTTPException(status_code=500, detail="Cohort update succeeded but could not retrieve updated data")
        else:
            # For new cohorts, return simple confirmation payload
            return {
                "message": "Cohort created successfully",
                "cohort_id": cohort_id,
                "name": cohort.name,
                "uploaded_by": cohort.uploaded_by
            }
    except IntegrityError as e:
        if "unique_cohort_name_uploader" in str(e) or "Duplicate entry" in str(e):
            raise fastapi.HTTPException(
                status_code=409, 
                detail=f"A cohort named '{cohort.name}' already exists for user '{cohort.uploaded_by}'"
            )
        raise fastapi.HTTPException(status_code=500, detail=f"Database error: {str(e)}")
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
        mapping = json.loads(column_mapping)
        warnings = []
        
        content = await file.read()
        df = await file_utils.parse_file(io.StringIO(content.decode('utf-8')), file.filename)
        
        error_message = None
        if validation_type == "cases_controls" or file_type.startswith("cases_controls"):
            error_message = validate_sgc_cases_controls(df, mapping)
        elif validation_type == "cooccurrence" or file_type.startswith("cooccurrence"):
            error_message = validate_sgc_co_occurrence(df, mapping)

        if error_message:
            raise fastapi.HTTPException(detail=error_message, status_code=400)
        
        # Add gender-specific sample size validation for cases/controls files
        if file_type.startswith("cases_controls"):
            # Get cohort info to validate against sample sizes
            cohort_info = query.get_sgc_cohort_by_id(engine, cohort_id)
            if not cohort_info:
                raise fastapi.HTTPException(status_code=400, detail="Invalid cohort_id: cohort does not exist")

            cohort = cohort_info[0]  # Get first row which contains cohort data
            
            cases_col = mapping.get('cases')
            controls_col = mapping.get('controls')
            if cases_col and controls_col:
                if file_type == "cases_controls_male":
                    expected_total = cohort['number_of_males']
                elif file_type == "cases_controls_female":
                    expected_total = cohort['number_of_females']
                elif file_type == "cases_controls_both":
                    expected_total = cohort['total_sample_size']
                else:
                    expected_total = None
                
                if expected_total is not None:
                    cases_values = pd.to_numeric(df[cases_col], errors='coerce')
                    controls_values = pd.to_numeric(df[controls_col], errors='coerce')
                    row_totals = cases_values + controls_values
                    
                    # Check for blocking errors first
                    blocking_errors = []
                    warning_rows = []
                    
                    for idx, (cases, controls, row_total) in enumerate(zip(cases_values, controls_values, row_totals)):
                        if pd.notna(cases) and pd.notna(controls) and pd.notna(row_total):
                            phenotype = df.iloc[idx][mapping.get('phenotype', 'phenotype')]
                            
                            # Blocking error: cases + controls > expected subjects
                            if int(row_total) > expected_total:
                                blocking_errors.append(f"{phenotype}: {int(row_total)} > {expected_total}")
                            
                            # Warning: cases + controls != expected subjects (but not over)
                            elif int(row_total) != expected_total:
                                warning_rows.append(f"{int(row_total)} < {expected_total} for {phenotype}")
                    
                    # Raise blocking errors immediately
                    if blocking_errors:
                        sample_errors = blocking_errors[:5]
                        error_msg = f"Validation failed for {file_type.split('_')[-1]} subjects. Errors: {sample_errors}"
                        if len(blocking_errors) > 5:
                            error_msg += f" (and {len(blocking_errors) - 5} more)"
                        raise fastapi.HTTPException(status_code=400, detail=error_msg)
                    
                    # Add warnings for non-matching totals
                    if warning_rows:
                        sample_warnings = warning_rows[:5]
                        warning_msg = f"Cases+controls less than expected subjects: {sample_warnings}"
                        if len(warning_rows) > 5:
                            warning_msg += f" (and {len(warning_rows) - 5} more)"
                        warnings.append(warning_msg)

        # Add validation for co-occurrence files
        if file_type.startswith("cooccurrence"):
            # Get cohort info to validate against sample sizes
            cohort_info = query.get_sgc_cohort_by_id(engine, cohort_id)
            if not cohort_info:
                raise fastapi.HTTPException(status_code=400, detail="Invalid cohort_id: cohort does not exist")

            cohort = cohort_info[0]  # Get first row which contains cohort data

            # Validate that the file type is one of the three supported co-occurrence types
            valid_cooccurrence_types = {"cooccurrence_male", "cooccurrence_female", "cooccurrence_both"}
            if file_type not in valid_cooccurrence_types:
                raise fastapi.HTTPException(
                    status_code=400,
                    detail=f"Invalid co-occurrence file type: {file_type}. Must be one of: {valid_cooccurrence_types}"
                )

            # Validate co-occurrence counts against cohort sample sizes
            cooccurrence_count_col = mapping.get('cooccurrence_count')
            if cooccurrence_count_col:
                # Check that individual co-occurrence counts don't exceed the relevant sample size
                cooccurrence_counts = pd.to_numeric(df[cooccurrence_count_col], errors='coerce')
                max_cooccurrence = cooccurrence_counts.max() if not cooccurrence_counts.isna().all() else 0

                # Determine the maximum allowed count based on file type
                if file_type == "cooccurrence_male":
                    max_allowed = cohort['number_of_males']
                    if max_cooccurrence > max_allowed:
                        raise fastapi.HTTPException(
                            status_code=400,
                            detail=f"Male co-occurrence file contains counts ({max_cooccurrence}) exceeding cohort male count ({max_allowed})"
                        )
                elif file_type == "cooccurrence_female":
                    max_allowed = cohort['number_of_females']
                    if max_cooccurrence > max_allowed:
                        raise fastapi.HTTPException(
                            status_code=400,
                            detail=f"Female co-occurrence file contains counts ({max_cooccurrence}) exceeding cohort female count ({max_allowed})"
                        )
                elif file_type == "cooccurrence_both":
                    max_allowed = cohort['total_sample_size']
                    if max_cooccurrence > max_allowed:
                        raise fastapi.HTTPException(
                            status_code=400,
                            detail=f"Combined co-occurrence file contains counts ({max_cooccurrence}) exceeding cohort total sample size ({max_allowed})"
                        )

        # Upload to S3
        s3_path = f"sgc/{cohort_id}/{file_type}/{file.filename}"
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
            file_size=len(file_content),
            column_mapping=mapping
        )

        file_id = query.insert_sgc_cohort_file(engine, cohort_file)

        # Reset validation status since a new file was added
        query.update_sgc_cohort_validation_status(engine, cohort_id, False)

        # Extract and store metadata based on file type
        if validation_type == "cases_controls" or file_type.startswith("cases_controls"):
            metadata = extract_cases_controls_metadata(df, mapping)
            metadata.file_id = file_id
            query.insert_sgc_cases_controls_metadata(engine, metadata)
        elif validation_type == "cooccurrence" or file_type.startswith("cooccurrence"):
            metadata = extract_cooccurrence_metadata(df, mapping)
            metadata.file_id = file_id
            query.insert_sgc_cooccurrence_metadata(engine, metadata)
        
        response = {
            "message": "File validated, uploaded, and saved successfully",
            "file_id": file_id,
            "cohort_id": cohort_id,
            "file_type": file_type,
            "file_name": file.filename,
            "file_path": cohort_file.file_path,
            "validation_type": validation_type,
            "file_size": cohort_file.file_size
        }
        
        if warnings:
            response["warnings"] = warnings
            response["message"] = "File uploaded successfully with warnings"
        
        return response
        
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


@router.get("/sgc/cohorts/{cohort_id}")
async def get_sgc_cohort_by_id(cohort_id: str, user: User = Depends(get_sgc_user)):
    """
    Get a single SGC cohort by ID with its associated files.
    - Users with 'sgc-review-data' permission can see any cohort
    - Other users can only see cohorts they uploaded
    """
    try:
        cohort_data = query.get_sgc_cohort_by_id(engine, cohort_id)
        if not cohort_data:
            raise fastapi.HTTPException(status_code=404, detail="Cohort not found")
        
        # Check permissions: either the user owns the cohort or has review permissions
        cohort_owner = cohort_data[0]['uploaded_by']  # First row has cohort info
        if not (cohort_owner == user.user_name or check_review_permissions(user)):
            raise fastapi.HTTPException(
                status_code=403, 
                detail="You can only view cohorts you uploaded"
            )
        
        return cohort_data
    except fastapi.HTTPException:
        raise
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"Error retrieving cohort: {str(e)}")


@router.delete("/sgc/cohorts/{cohort_id}")
async def delete_sgc_cohort(cohort_id: str, user: User = Depends(get_sgc_user)):
    """
    Delete an SGC cohort and all associated files.
    - Only users with 'sgc-review-data' permission can delete cohorts
    """
    try:
        # Check permissions: only reviewers can delete cohorts
        if not check_review_permissions(user):
            raise fastapi.HTTPException(
                status_code=403, 
                detail="Only reviewers can delete cohorts"
            )
        
        # Check if cohort exists
        cohort_data = query.get_sgc_cohort_by_id(engine, cohort_id)
        if not cohort_data:
            raise fastapi.HTTPException(status_code=404, detail="Cohort not found")
        
        # Delete the cohort (CASCADE will handle associated files and metadata)
        deleted = query.delete_sgc_cohort(engine, cohort_id)
        if not deleted:
            raise fastapi.HTTPException(status_code=404, detail="Cohort not found")
        
        return {"message": "Cohort deleted successfully", "cohort_id": cohort_id}
    except fastapi.HTTPException:
        raise
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"Error deleting cohort: {str(e)}")


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
        
        # Delete associated metadata first (foreign key will handle cascade, but let's be explicit)
        # Try to delete both types of metadata (only one will exist per file)
        query.delete_sgc_cases_controls_metadata(engine, file_id)
        query.delete_sgc_cooccurrence_metadata(engine, file_id)
        
        # Get cohort_id before deleting the file
        file_info = query.get_sgc_cohort_file_by_id(engine, file_id)
        cohort_id = file_info['cohort_id'] if file_info else None
        
        # Delete the file
        deleted = query.delete_sgc_cohort_file(engine, file_id)
        if not deleted:
            raise fastapi.HTTPException(status_code=404, detail="File not found")
        
        # Reset validation status since files have changed
        if cohort_id:
            query.update_sgc_cohort_validation_status(engine, cohort_id, False)
        
        return {"message": "File deleted successfully", "file_id": file_id}
    except fastapi.HTTPException:
        raise
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"Error deleting file: {str(e)}")


@router.post("/sgc/cohorts/{cohort_id}/validate-all-consistency")
async def validate_sgc_cohort_all_consistency(cohort_id: str, user: User = Depends(get_sgc_user)):
    try:
        cohort_data = query.get_sgc_cohort_by_id(engine, cohort_id)
        if not cohort_data:
            raise fastapi.HTTPException(status_code=404, detail="Cohort not found")

        cohort_owner = cohort_data[0]['uploaded_by']
        if not (cohort_owner == user.user_name or check_review_permissions(user)):
            raise fastapi.HTTPException(
                status_code=403,
                detail="You can only validate cohorts you uploaded"
            )

        # Generate 'both' files by combining male and female files
        files_by_type = {}
        for file_data in cohort_data:
            if file_data.get('file_type'):
                file_type = file_data['file_type']
                files_by_type[file_type] = file_data

        # Generate 'both' cases/controls file if male and female files exist
        await generate_both_file_from_male_female(cohort_id, files_by_type, "cases_controls")

        # Generate 'both' co-occurrence file if male and female files exist  
        await generate_both_file_from_male_female(cohort_id, files_by_type, "cooccurrence")

        # Now run validation checks (which will now include the generated 'both' files)
        cases_controls_error = validate_cases_controls_file_consistency(cohort_id)
        if cases_controls_error:
            raise fastapi.HTTPException(status_code=400, detail=cases_controls_error)

        cooccurrence_error = validate_cooccurrence_file_consistency(cohort_id)
        if cooccurrence_error:
            raise fastapi.HTTPException(status_code=400, detail=cooccurrence_error)

        cross_file_error = validate_cohort_cross_file_consistency(cohort_id)
        if cross_file_error:
            raise fastapi.HTTPException(status_code=400, detail=cross_file_error)

        # All validations passed, update the validation status
        query.update_sgc_cohort_validation_status(engine, cohort_id, True)

        return {
            "message": "Cohort validated successfully - 'both' files generated by combining male/female files and all consistency checks passed", 
            "validation_status": True
        }

    except fastapi.HTTPException:
        raise
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"Error during validation: {str(e)}")




@router.get("/sgc/cohort-files/{file_id}")
async def download_sgc_cohort_file(file_id: str, user: User = Depends(get_sgc_user)):
    """
    Download an SGC cohort file.
    - Users can download files from cohorts they uploaded
    - Users with 'sgc-review-data' permission can download any file
    Returns a redirect to a presigned S3 URL for the file download.
    """
    try:
        # Get the file information
        file_info = query.get_sgc_cohort_file_by_id(engine, file_id)
        if not file_info:
            raise fastapi.HTTPException(status_code=404, detail="File not found")
        
        # Get the owner of the file for permission checking
        file_owner = query.get_sgc_cohort_file_owner(engine, file_id)
        if not file_owner:
            raise fastapi.HTTPException(status_code=404, detail="File not found")
        
        # Check permissions: either the user owns the file or has review permissions
        if not (file_owner == user.user_name or check_review_permissions(user)):
            raise fastapi.HTTPException(
                status_code=403, 
                detail="You can only download files from cohorts you uploaded"
            )
        
        # Get the S3 path and create a presigned URL
        s3_full_path = file_info['file_path']
        # Strip s3://bucket/ prefix to get just the key
        s3_path = s3_full_path.replace(f"s3://{s3.BASE_BUCKET}/", "")
        presigned_url = s3.get_signed_url(s3.BASE_BUCKET, s3_path)
        
        # Return the presigned URL in response payload
        return {
            "presigned_url": presigned_url,
            "file_name": file_info['file_name'],
            "file_size": file_info['file_size']
        }
        
    except fastapi.HTTPException:
        raise
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"Error downloading file: {str(e)}")


@router.get("/sgc/users")
async def get_all_sgc_users(user: User = Depends(get_sgc_user)):
    """
    Get all SGC users from the dig-user-service.
    Requires 'sgc-review-data' permission.
    """
    if not check_review_permissions(user):
        raise fastapi.HTTPException(
            status_code=403,
            detail="You need 'sgc-review-data' permission to list users"
        )

    token = UPLOADER_TOKEN or REVIEWER_TOKEN

    if not token:
        raise fastapi.HTTPException(
            status_code=500,
            detail="No token configured for user service access"
        )

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{USER_SERVICE_URL}/api/auth/list-users/",
                params={"token": token}
            )

            if response.status_code == 200:
                return response.json()
            else:
                try:
                    error_detail = response.json()
                except:
                    error_detail = response.text

                raise fastapi.HTTPException(
                    status_code=response.status_code,
                    detail=f"Failed to retrieve users: {error_detail}"
                )

    except httpx.RequestError as e:
        raise fastapi.HTTPException(
            status_code=503,
            detail=f"User service unavailable: {str(e)}"
        )
    except Exception as e:
        raise fastapi.HTTPException(
            status_code=500,
            detail=f"Error retrieving users: {str(e)}"
        )


@router.post("/sgc/create-user")
async def create_sgc_user(request: NewUserRequest, user: User = Depends(get_sgc_user)):
    """
    Create a new SGC user via the dig-user-service.
    Requires 'sgc-add-user' permission.
    """
    # Check permissions
    if not check_add_user_permissions(user):
        raise fastapi.HTTPException(
            status_code=403,
            detail="You need 'sgc-add-user' permission to create users"
        )

    # Get the appropriate token based on user type
    token = None
    if request.user_type == 'uploader':
        token = UPLOADER_TOKEN
    elif request.user_type == 'reviewer':
        token = REVIEWER_TOKEN
    else:
        raise fastapi.HTTPException(
            status_code=400,
            detail=f"Invalid user_type: {request.user_type}. Must be 'uploader' or 'reviewer'"
        )

    if not token:
        raise fastapi.HTTPException(
            status_code=500,
            detail=f"No token configured for user_type: {request.user_type}"
        )

    try:
        # Prepare the request data for dig-user-service
        user_data = {
            "token": token,
            "username": request.user_name,
            "password": request.password
        }

        # Add optional fields if provided
        if request.first_name:
            user_data["first_name"] = request.first_name
        if request.last_name:
            user_data["last_name"] = request.last_name
        if request.user_name:  # Use username as email if not provided separately
            user_data["email"] = request.user_name

        # Make the request to dig-user-service
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{USER_SERVICE_URL}/api/auth/create-user/",
                data=user_data  # Use form data as specified in the API
            )

            if response.status_code == 200 or response.status_code == 201:
                return {
                    "message": "User created successfully",
                    "username": request.user_name,
                    "user_type": request.user_type
                }
            else:
                # Try to get error details from response
                try:
                    error_detail = response.json()
                except:
                    error_detail = response.text

                raise fastapi.HTTPException(
                    status_code=response.status_code,
                    detail=f"Failed to create user: {error_detail}"
                )

    except httpx.RequestError as e:
        raise fastapi.HTTPException(
            status_code=503,
            detail=f"User service unavailable: {str(e)}"
        )
    except Exception as e:
        raise fastapi.HTTPException(
            status_code=500,
            detail=f"Error creating user: {str(e)}"
        )


@router.get("/sgc/phenotype-case-totals", response_model=List[SGCPhenotypeCaseTotals])
async def get_sgc_phenotype_case_totals_endpoint(user: User = Depends(get_sgc_user)):
    """
    Get total cases and controls across all SGC cohorts aggregated by phenotype.
    Returns statistics showing how many cases/controls exist for each phenotype across all cohorts.
    """
    try:
        results = query.get_sgc_phenotype_case_totals(engine)
        return results
    except Exception as e:
        raise fastapi.HTTPException(
            status_code=500,
            detail=f"Error retrieving phenotype case totals: {str(e)}"
        )


@router.get("/sgc/phenotype-case-counts-by-sex", response_model=List[SGCPhenotypeCaseCountsBySex])
async def get_sgc_phenotype_case_counts_by_sex_endpoint(user: User = Depends(get_sgc_user)):
    """
    Get case and control counts by phenotype broken down by sex across all SGC cohorts.
    Returns statistics showing how many male/female/both cases and controls exist for each phenotype.
    Requires 'sgc-review-data' permission.
    """
    if not check_review_permissions(user):
        raise fastapi.HTTPException(
            status_code=403,
            detail="You need 'sgc-review-data' permission to access phenotype case counts by sex"
        )
    
    try:
        results = query.get_sgc_phenotype_case_counts_by_sex(engine)
        return results
    except Exception as e:
        raise fastapi.HTTPException(
            status_code=500,
            detail=f"Error retrieving phenotype case counts by sex: {str(e)}"
        )


