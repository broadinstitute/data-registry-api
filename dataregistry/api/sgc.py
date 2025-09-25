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
from dataregistry.api.model import SGCPhenotype, SGCCohort, SGCCohortFile, SGCCasesControlsMetadata, SGCCoOccurrenceMetadata, User, NewUserRequest
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


def validate_cases_controls_file_consistency(cohort_id: str) -> Dict[str, any]:
    """
    Validate consistency across the three cases/controls files for a cohort.
    Returns validation results with errors and warnings.
    """
    errors = []
    warnings = []
    
    try:
        # Get all files for this cohort
        cohort_files = query.get_sgc_cohort_by_id(engine, cohort_id)
        if not cohort_files:
            return {"errors": ["Cohort not found"], "warnings": [], "files_found": {}}
        
        # Group cases_controls files by type and get their metadata
        files_by_type = {}
        metadata_by_type = {}
        
        for file_data in cohort_files:
            if file_data.get('file_type') and file_data['file_type'].startswith('cases_controls'):
                file_type = file_data['file_type']
                files_by_type[file_type] = file_data
                
                # Get metadata for this file
                if file_data.get('file_id'):
                    try:
                        cc_metadata = query.get_sgc_cases_controls_metadata(engine, file_data['file_id'])
                        if cc_metadata:
                            metadata_by_type[file_type] = cc_metadata[0] if isinstance(cc_metadata, list) else cc_metadata
                    except Exception as e:
                        warnings.append(f"Could not retrieve metadata for {file_type}: {str(e)}")
        
        # Check which file types we have
        required_types = {'cases_controls_male', 'cases_controls_female', 'cases_controls_both'}
        available_types = set(files_by_type.keys())
        missing_types = required_types - available_types
        
        if missing_types:
            return {
                "errors": [],
                "warnings": [f"Missing file types for complete validation: {sorted(list(missing_types))}"],
                "files_found": list(available_types)
            }
        
        # Get cohort info for reference
        cohort_info = cohort_files[0]  # First row contains cohort data
        
        # Validate phenotype consistency
        male_phenotypes = set(metadata_by_type['cases_controls_male'].get('distinct_phenotypes', []))
        female_phenotypes = set(metadata_by_type['cases_controls_female'].get('distinct_phenotypes', []))
        both_phenotypes = set(metadata_by_type['cases_controls_both'].get('distinct_phenotypes', []))
        
        # Combined phenotypes from male + female should match 'both' phenotypes
        combined_phenotypes = male_phenotypes.union(female_phenotypes)
        if combined_phenotypes != both_phenotypes:
            missing_from_both = combined_phenotypes - both_phenotypes
            extra_in_both = both_phenotypes - combined_phenotypes
            
            if missing_from_both:
                sample_missing = sorted(list(missing_from_both))[:5]
                error_msg = f"Phenotypes in male/female files but missing from 'both' file: {sample_missing}"
                if len(missing_from_both) > 5:
                    error_msg += f" (and {len(missing_from_both) - 5} more)"
                errors.append(error_msg)
            
            if extra_in_both:
                sample_extra = sorted(list(extra_in_both))[:5] 
                error_msg = f"Extra phenotypes in 'both' file not found in male/female files: {sample_extra}"
                if len(extra_in_both) > 5:
                    error_msg += f" (and {len(extra_in_both) - 5} more)"
                errors.append(error_msg)
        
        # Validate total counts consistency
        male_total = metadata_by_type['cases_controls_male'].get('total_cases', 0) + metadata_by_type['cases_controls_male'].get('total_controls', 0)
        female_total = metadata_by_type['cases_controls_female'].get('total_cases', 0) + metadata_by_type['cases_controls_female'].get('total_controls', 0)
        both_total = metadata_by_type['cases_controls_both'].get('total_cases', 0) + metadata_by_type['cases_controls_both'].get('total_controls', 0)
        
        combined_total = male_total + female_total
        if combined_total != both_total:
            errors.append(f"Combined male + female totals ({combined_total}) does not equal 'both' file total ({both_total})")
        
        # Additional validation: Check individual file totals match cohort expectations
        cohort_male_count = cohort_info.get('number_of_males', 0)
        cohort_female_count = cohort_info.get('number_of_females', 0)
        cohort_total_count = cohort_info.get('total_sample_size', 0)
        
        if male_total != cohort_male_count:
            errors.append(f"Male file total ({male_total}) does not match cohort male count ({cohort_male_count})")
        
        if female_total != cohort_female_count:
            errors.append(f"Female file total ({female_total}) does not match cohort female count ({cohort_female_count})")
        
        if both_total != cohort_total_count:
            errors.append(f"Both file total ({both_total}) does not match cohort total sample size ({cohort_total_count})")

        # Per-phenotype count validation: male + female should equal both for each phenotype
        if all(file_type in metadata_by_type for file_type in ['cases_controls_male', 'cases_controls_female', 'cases_controls_both']):
            male_phenotype_counts = metadata_by_type['cases_controls_male'].get('phenotype_counts', {})
            female_phenotype_counts = metadata_by_type['cases_controls_female'].get('phenotype_counts', {})
            both_phenotype_counts = metadata_by_type['cases_controls_both'].get('phenotype_counts', {})

            # Get all phenotypes that appear in the both file
            for phenotype in both_phenotype_counts:
                both_cases = both_phenotype_counts[phenotype].get('cases', 0)
                both_controls = both_phenotype_counts[phenotype].get('controls', 0)

                # Calculate expected totals from male + female files
                male_cases = male_phenotype_counts.get(phenotype, {}).get('cases', 0)
                male_controls = male_phenotype_counts.get(phenotype, {}).get('controls', 0)
                female_cases = female_phenotype_counts.get(phenotype, {}).get('cases', 0)
                female_controls = female_phenotype_counts.get(phenotype, {}).get('controls', 0)

                expected_cases = male_cases + female_cases
                expected_controls = male_controls + female_controls

                # Check if totals match
                if both_cases != expected_cases:
                    errors.append(f"Phenotype '{phenotype}': Both file cases ({both_cases}) != Male + Female cases ({expected_cases})")

                if both_controls != expected_controls:
                    errors.append(f"Phenotype '{phenotype}': Both file controls ({both_controls}) != Male + Female controls ({expected_controls})")

        return {
            "errors": errors,
            "warnings": warnings,
            "files_found": list(available_types),
            "validation_summary": {
                "male_total": male_total,
                "female_total": female_total,
                "both_total": both_total,
                "combined_total": combined_total,
                "cohort_male_count": cohort_male_count,
                "cohort_female_count": cohort_female_count,
                "cohort_total_count": cohort_total_count
            }
        }
        
    except Exception as e:
        return {
            "errors": [f"Error during cross-file validation: {str(e)}"],
            "warnings": [],
            "files_found": list(files_by_type.keys()) if 'files_by_type' in locals() else []
        }


def validate_cooccurrence_file_consistency(cohort_id: str) -> Dict[str, any]:
    """
    Validate consistency across the three co-occurrence files for a cohort.
    Returns validation results with errors and warnings.
    """
    errors = []
    warnings = []

    try:
        # Get all files for this cohort
        cohort_files = query.get_sgc_cohort_by_id(engine, cohort_id)
        if not cohort_files:
            return {"errors": ["Cohort not found"], "warnings": [], "files_found": {}}

        # Group co-occurrence files by type and get their metadata
        files_by_type = {}
        metadata_by_type = {}

        for file_data in cohort_files:
            if file_data.get('file_type') and file_data['file_type'].startswith('cooccurrence'):
                file_type = file_data['file_type']
                files_by_type[file_type] = file_data

                # Get metadata for this file
                if file_data.get('file_id'):
                    try:
                        cooccur_metadata = query.get_sgc_cooccurrence_metadata(engine, file_data['file_id'])
                        if cooccur_metadata:
                            metadata_by_type[file_type] = cooccur_metadata[0] if isinstance(cooccur_metadata, list) else cooccur_metadata
                    except Exception as e:
                        warnings.append(f"Could not retrieve metadata for {file_type}: {str(e)}")

        # Check which file types we have
        required_types = {'cooccurrence_male', 'cooccurrence_female', 'cooccurrence_both'}
        available_types = set(files_by_type.keys())
        missing_types = required_types - available_types

        if missing_types:
            return {
                "errors": [],
                "warnings": [f"Missing file types for complete validation: {sorted(list(missing_types))}"],
                "files_found": list(available_types)
            }

        # Get cohort info for reference
        cohort_info = cohort_files[0]  # First row contains cohort data

        # Validate phenotype consistency across co-occurrence files
        male_phenotypes = set(metadata_by_type['cooccurrence_male'].get('distinct_phenotypes', []))
        female_phenotypes = set(metadata_by_type['cooccurrence_female'].get('distinct_phenotypes', []))
        both_phenotypes = set(metadata_by_type['cooccurrence_both'].get('distinct_phenotypes', []))

        # Combined phenotypes from male + female should match 'both' phenotypes
        combined_phenotypes = male_phenotypes.union(female_phenotypes)
        if combined_phenotypes != both_phenotypes:
            missing_from_both = combined_phenotypes - both_phenotypes
            extra_in_both = both_phenotypes - combined_phenotypes

            if missing_from_both:
                sample_missing = sorted(list(missing_from_both))[:5]
                error_msg = f"Phenotypes in male/female co-occurrence files but missing from 'both' file: {sample_missing}"
                if len(missing_from_both) > 5:
                    error_msg += f" (and {len(missing_from_both) - 5} more)"
                errors.append(error_msg)

            if extra_in_both:
                sample_extra = sorted(list(extra_in_both))[:5]
                error_msg = f"Extra phenotypes in 'both' co-occurrence file not found in male/female files: {sample_extra}"
                if len(extra_in_both) > 5:
                    error_msg += f" (and {len(extra_in_both) - 5} more)"
                errors.append(error_msg)

        # Validate pair count consistency
        male_pairs = metadata_by_type['cooccurrence_male'].get('total_pairs', 0)
        female_pairs = metadata_by_type['cooccurrence_female'].get('total_pairs', 0)
        both_pairs = metadata_by_type['cooccurrence_both'].get('total_pairs', 0)

        # The 'both' file should have at least as many pairs as the combined unique pairs from male + female
        # (It might have more due to cross-gender phenotype pairs)
        combined_pairs = male_pairs + female_pairs
        if both_pairs < combined_pairs:
            warnings.append(f"Combined co-occurrence pairs count ({both_pairs}) is less than sum of male + female pairs ({combined_pairs}). This may indicate missing cross-gender pairs.")

        # Validate co-occurrence count totals
        male_total_count = metadata_by_type['cooccurrence_male'].get('total_cooccurrence_count', 0)
        female_total_count = metadata_by_type['cooccurrence_female'].get('total_cooccurrence_count', 0)
        both_total_count = metadata_by_type['cooccurrence_both'].get('total_cooccurrence_count', 0)

        combined_count = male_total_count + female_total_count
        if both_total_count != combined_count:
            warnings.append(f"Combined male + female co-occurrence counts ({combined_count}) does not equal 'both' file total ({both_total_count})")

        # Per-phenotype-pair count validation: male + female should equal both for each phenotype pair
        if all(file_type in metadata_by_type for file_type in ['cooccurrence_male', 'cooccurrence_female', 'cooccurrence_both']):
            male_pair_counts = metadata_by_type['cooccurrence_male'].get('phenotype_pair_counts', {})
            female_pair_counts = metadata_by_type['cooccurrence_female'].get('phenotype_pair_counts', {})
            both_pair_counts = metadata_by_type['cooccurrence_both'].get('phenotype_pair_counts', {})

            # Get all phenotype pairs that appear in the both file
            for pair_key in both_pair_counts:
                both_count = both_pair_counts[pair_key]

                # Calculate expected total from male + female files
                male_count = male_pair_counts.get(pair_key, 0)
                female_count = female_pair_counts.get(pair_key, 0)
                expected_count = male_count + female_count

                # Check if counts match
                if both_count != expected_count:
                    phenotypes = pair_key.split('|')
                    pair_display = f"({phenotypes[0]}, {phenotypes[1]})"
                    errors.append(f"Phenotype pair {pair_display}: Both file count ({both_count}) != Male + Female counts ({expected_count})")

        return {
            "errors": errors,
            "warnings": warnings,
            "files_found": list(available_types),
            "validation_summary": {
                "male_pairs": male_pairs,
                "female_pairs": female_pairs,
                "both_pairs": both_pairs,
                "combined_pairs": combined_pairs,
                "male_total_count": male_total_count,
                "female_total_count": female_total_count,
                "both_total_count": both_total_count,
                "combined_count": combined_count
            }
        }

    except Exception as e:
        return {
            "errors": [f"Error during co-occurrence cross-file validation: {str(e)}"],
            "warnings": [],
            "files_found": list(files_by_type.keys()) if 'files_by_type' in locals() else []
        }


def validate_cohort_cross_file_consistency(cohort_id: str) -> Dict[str, any]:
    """
    Validate consistency between co-occurrence files and their corresponding cases/controls files.
    Ensures co-occurrence files only reference phenotypes that exist in cases/controls files.
    Returns validation results with errors and warnings.
    """
    errors = []
    warnings = []

    try:
        # Get all files for this cohort
        cohort_files = query.get_sgc_cohort_by_id(engine, cohort_id)
        if not cohort_files:
            return {"errors": ["Cohort not found"], "warnings": [], "files_found": {}}

        # Group files by type and get their metadata
        cases_controls_files = {}
        cooccurrence_files = {}
        cases_controls_metadata = {}
        cooccurrence_metadata = {}

        for file_data in cohort_files:
            file_type = file_data.get('file_type')
            file_id = file_data.get('file_id')

            if file_type and file_type.startswith('cases_controls'):
                cases_controls_files[file_type] = file_data
                if file_id:
                    try:
                        cc_metadata = query.get_sgc_cases_controls_metadata(engine, file_id)
                        if cc_metadata:
                            cases_controls_metadata[file_type] = cc_metadata[0] if isinstance(cc_metadata, list) else cc_metadata
                    except Exception as e:
                        warnings.append(f"Could not retrieve cases/controls metadata for {file_type}: {str(e)}")

            elif file_type and file_type.startswith('cooccurrence'):
                cooccurrence_files[file_type] = file_data
                if file_id:
                    try:
                        cooccur_metadata = query.get_sgc_cooccurrence_metadata(engine, file_id)
                        if cooccur_metadata:
                            cooccurrence_metadata[file_type] = cooccur_metadata[0] if isinstance(cooccur_metadata, list) else cooccur_metadata
                    except Exception as e:
                        warnings.append(f"Could not retrieve co-occurrence metadata for {file_type}: {str(e)}")

        # Define file type mappings (co-occurrence -> cases/controls)
        file_type_mappings = {
            'cooccurrence_male': 'cases_controls_male',
            'cooccurrence_female': 'cases_controls_female',
            'cooccurrence_both': 'cases_controls_both'
        }

        # Validate each co-occurrence file against its corresponding cases/controls file
        validation_results = {}

        for cooccur_type, cases_type in file_type_mappings.items():
            if cooccur_type in cooccurrence_metadata and cases_type in cases_controls_metadata:
                cooccur_phenotypes = set(cooccurrence_metadata[cooccur_type].get('distinct_phenotypes', []))
                cases_phenotypes = set(cases_controls_metadata[cases_type].get('distinct_phenotypes', []))

                # Check for phenotypes in co-occurrence that don't exist in cases/controls
                missing_phenotypes = cooccur_phenotypes - cases_phenotypes
                if missing_phenotypes:
                    sample_missing = sorted(list(missing_phenotypes))[:5]
                    error_msg = f"{cooccur_type} file references phenotypes not found in {cases_type} file: {sample_missing}"
                    if len(missing_phenotypes) > 5:
                        error_msg += f" (and {len(missing_phenotypes) - 5} more)"
                    errors.append(error_msg)

                validation_results[f"{cooccur_type}_vs_{cases_type}"] = {
                    "cooccurrence_phenotypes": len(cooccur_phenotypes),
                    "cases_controls_phenotypes": len(cases_phenotypes),
                    "invalid_phenotypes_in_cooccurrence": len(missing_phenotypes)
                }

        return {
            "errors": errors,
            "warnings": warnings,
            "files_found": {
                "cases_controls": list(cases_controls_files.keys()),
                "cooccurrence": list(cooccurrence_files.keys())
            },
            "validation_results": validation_results
        }

    except Exception as e:
        return {
            "errors": [f"Error during cross-file validation: {str(e)}"],
            "warnings": [],
            "files_found": {}
        }


def generate_validation_warnings(cohort: SGCCohort, cases_controls_metadata: Optional[SGCCasesControlsMetadata],
                                cooccurrence_phenotypes: Optional[List[str]] = None,
                                cases_controls_df: Optional[pd.DataFrame] = None,
                                cooccurrence_df: Optional[pd.DataFrame] = None,
                                cases_controls_mapping: Optional[Dict[str, str]] = None,
                                cooccurrence_mapping: Optional[Dict[str, str]] = None) -> List[str]:
    """
    Generate warnings for SGC cohort files based on cohort metadata and file contents.
    These are non-blocking warnings that don't prevent file upload.
    """
    warnings = []
    
    if not cases_controls_metadata:
        return warnings  # No cases/controls file to validate against
    
    # 1. Check cases + controls vs total sample size
    file_total = cases_controls_metadata.total_cases + cases_controls_metadata.total_controls
    if file_total != cohort.total_sample_size:
        warnings.append(
            f"Cases + Controls ({file_total}) does not equal total cohort sample size ({cohort.total_sample_size})"
        )
    
    # 2. Check Female + Male counts vs total cases/controls
    cohort_total_gender = cohort.number_of_males + cohort.number_of_females
    if cohort_total_gender != cohort.total_sample_size:
        warnings.append(
            f"Male count ({cohort.number_of_males}) + Female count ({cohort.number_of_females}) = {cohort_total_gender} "
            f"does not equal total sample size ({cohort.total_sample_size})"
        )
    
    if cooccurrence_metadata and cooccurrence_df is not None and cases_controls_df is not None:
        # 3. Check for co-occurrence phenotypes not in cases/controls file
        cases_phenotypes = set(cases_controls_metadata.distinct_phenotypes)
        cooccurrence_phenotypes = set(cooccurrence_metadata.distinct_phenotypes)
        missing_from_cases = cooccurrence_phenotypes - cases_phenotypes
        
        if missing_from_cases:
            missing_list = sorted(list(missing_from_cases))[:5]  # Show first 5
            warning_msg = f"Co-occurrence phenotypes not found in cases/controls file: {missing_list}"
            if len(missing_from_cases) > 5:
                warning_msg += f" (and {len(missing_from_cases) - 5} more)"
            warnings.append(warning_msg)
        
        # 4. Check co-occurrence numbers are smaller than case numbers for relevant phenotypes
        if cases_controls_mapping and cooccurrence_mapping:
            cases_col = cases_controls_mapping['cases']
            phenotype_col = cases_controls_mapping['phenotype'] 
            cooccur_col = cooccurrence_mapping['cooccurrence_count']
            phenotype1_col = cooccurrence_mapping['phenotype1']
            phenotype2_col = cooccurrence_mapping['phenotype2']
            
            # Create a lookup of phenotype -> case count
            case_counts = {}
            for _, row in cases_controls_df.iterrows():
                phenotype = row[phenotype_col]
                cases = pd.to_numeric(row[cases_col], errors='coerce')
                if not pd.isna(cases):
                    case_counts[phenotype] = int(cases)
            
            # Check each co-occurrence against individual case counts
            violations = []
            for _, row in cooccurrence_df.iterrows():
                phenotype1 = row[phenotype1_col]
                phenotype2 = row[phenotype2_col]
                cooccur_count = pd.to_numeric(row[cooccur_col], errors='coerce')
                
                if not pd.isna(cooccur_count) and int(cooccur_count) > 0:
                    cooccur_count = int(cooccur_count)
                    # Check against both phenotypes' case counts
                    for phenotype in [phenotype1, phenotype2]:
                        if phenotype in case_counts:
                            if cooccur_count > case_counts[phenotype]:
                                violations.append(f"({phenotype1}, {phenotype2}): {cooccur_count} > {phenotype} cases ({case_counts[phenotype]})")
            
            if violations:
                sample_violations = violations[:3]  # Show first 3
                warning_msg = f"Co-occurrence counts exceed individual case counts: {sample_violations}"
                if len(violations) > 3:
                    warning_msg += f" (and {len(violations) - 3} more)"
                warnings.append(warning_msg)
        
        # 5. Check for missing phenotypes (in cases/controls but not co-occurrence)
        missing_from_cooccurrence = cases_phenotypes - cooccurrence_phenotypes
        if missing_from_cooccurrence:
            missing_list = sorted(list(missing_from_cooccurrence))[:5]
            warning_msg = f"Phenotypes missing from co-occurrence file: {missing_list}"
            if len(missing_from_cooccurrence) > 5:
                warning_msg += f" (and {len(missing_from_cooccurrence) - 5} more)"
            warnings.append(warning_msg)
        
        # 6. Check for missing phenotype pairs from co-occurrence file
        # Generate all possible pairs from cases/controls phenotypes
        cases_phenotype_list = sorted(cases_phenotypes)
        all_possible_pairs = set()
        for i in range(len(cases_phenotype_list)):
            for j in range(i + 1, len(cases_phenotype_list)):
                pair = tuple(sorted([cases_phenotype_list[i], cases_phenotype_list[j]]))
                all_possible_pairs.add(pair)
        
        # Get actual pairs from co-occurrence file
        if cooccurrence_mapping:
            phenotype1_col = cooccurrence_mapping['phenotype1']
            phenotype2_col = cooccurrence_mapping['phenotype2']
            actual_pairs = set()
            for _, row in cooccurrence_df.iterrows():
                pair = tuple(sorted([row[phenotype1_col], row[phenotype2_col]]))
                actual_pairs.add(pair)
            
            missing_pairs = all_possible_pairs - actual_pairs
            if missing_pairs:
                missing_list = [f"({p[0]}, {p[1]})" for p in sorted(missing_pairs)[:5]]
                warning_msg = f"Missing phenotype pairs from co-occurrence file: {missing_list}"
                if len(missing_pairs) > 5:
                    warning_msg += f" (and {len(missing_pairs) - 5} more)"
                warnings.append(warning_msg)
    
    elif cooccurrence_metadata:
        # Basic checks without access to full dataframe data
        cases_phenotypes = set(cases_controls_metadata.distinct_phenotypes)
        cooccurrence_phenotypes = set(cooccurrence_metadata.distinct_phenotypes)
        missing_from_cases = cooccurrence_phenotypes - cases_phenotypes
        
        if missing_from_cases:
            missing_list = sorted(list(missing_from_cases))[:5]
            warning_msg = f"Co-occurrence phenotypes not found in cases/controls file: {missing_list}"
            if len(missing_from_cases) > 5:
                warning_msg += f" (and {len(missing_from_cases) - 5} more)"
            warnings.append(warning_msg)
            
        missing_from_cooccurrence = cases_phenotypes - cooccurrence_phenotypes
        if missing_from_cooccurrence:
            missing_list = sorted(list(missing_from_cooccurrence))[:5]
            warning_msg = f"Phenotypes missing from co-occurrence file: {missing_list}"
            if len(missing_from_cooccurrence) > 5:
                warning_msg += f" (and {len(missing_from_cooccurrence) - 5} more)"
            warnings.append(warning_msg)
    
    else:
        # Flag that co-occurrence file is missing entirely
        warnings.append("Co-occurrence file not provided")
    
    return warnings


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
        message = "Cohort updated successfully" if was_update else "Cohort created successfully"
        
        return {
            "message": message,
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
            
            # Calculate total cases + controls from the file
            cases_col = mapping.get('cases')
            controls_col = mapping.get('controls')
            if cases_col and controls_col:
                total_cases = pd.to_numeric(df[cases_col], errors='coerce').sum()
                total_controls = pd.to_numeric(df[controls_col], errors='coerce').sum()
                file_total = int(total_cases + total_controls) if not (pd.isna(total_cases) or pd.isna(total_controls)) else 0
                
                # Validate against expected sample sizes based on file type
                if file_type == "cases_controls_male":
                    expected_total = cohort['number_of_males']
                    if file_total != expected_total:
                        raise fastapi.HTTPException(
                            status_code=400,
                            detail=f"Male cases/controls file total ({file_total}) does not match cohort male count ({expected_total})"
                        )
                elif file_type == "cases_controls_female":
                    expected_total = cohort['number_of_females']
                    if file_total != expected_total:
                        raise fastapi.HTTPException(
                            status_code=400,
                            detail=f"Female cases/controls file total ({file_total}) does not match cohort female count ({expected_total})"
                        )
                elif file_type == "cases_controls_both":
                    expected_total = cohort['total_sample_size']
                    if file_total != expected_total:
                        raise fastapi.HTTPException(
                            status_code=400,
                            detail=f"Combined cases/controls file total ({file_total}) does not match cohort total sample size ({expected_total})"
                        )

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
            file_size=len(file_content)
        )

        file_id = query.insert_sgc_cohort_file(engine, cohort_file)

        # Extract and store metadata based on file type
        if validation_type == "cases_controls" or file_type.startswith("cases_controls"):
            metadata = extract_cases_controls_metadata(df, mapping)
            metadata.file_id = file_id
            query.insert_sgc_cases_controls_metadata(engine, metadata)
        elif validation_type == "cooccurrence" or file_type.startswith("cooccurrence"):
            metadata = extract_cooccurrence_metadata(df, mapping)
            metadata.file_id = file_id
            query.insert_sgc_cooccurrence_metadata(engine, metadata)
        
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
        
        # Delete the file
        deleted = query.delete_sgc_cohort_file(engine, file_id)
        if not deleted:
            raise fastapi.HTTPException(status_code=404, detail="File not found")
        
        return {"message": "File deleted successfully", "file_id": file_id}
    except fastapi.HTTPException:
        raise
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"Error deleting file: {str(e)}")


@router.post("/sgc/cohorts/{cohort_id}/validate-all-consistency")
async def validate_sgc_cohort_all_consistency(cohort_id: str, user: User = Depends(get_sgc_user)):
    """
    Comprehensive validation that runs all consistency checks for a cohort.
    Returns as soon as any error is found.
    Validates:
    1. Cases/controls files consistency (male, female, both)
    2. Co-occurrence files consistency (male, female, both)
    3. Cross-file consistency (co-occurrence phenotypes exist in cases/controls)

    - Users can validate cohorts they uploaded
    - Users with 'sgc-review-data' permission can validate any cohort
    """
    try:
        # Check permissions: get cohort to verify ownership
        cohort_data = query.get_sgc_cohort_by_id(engine, cohort_id)
        if not cohort_data:
            raise fastapi.HTTPException(status_code=404, detail="Cohort not found")

        # Check permissions: either the user owns the cohort or has review permissions
        cohort_owner = cohort_data[0]['uploaded_by']  # First row has cohort info
        if not (cohort_owner == user.user_name or check_review_permissions(user)):
            raise fastapi.HTTPException(
                status_code=403,
                detail="You can only validate cohorts you uploaded"
            )

        all_warnings = []

        # 1. Validate cases/controls files consistency first
        cases_controls_results = validate_cases_controls_file_consistency(cohort_id)
        if cases_controls_results.get('errors'):
            return {
                "cohort_id": cohort_id,
                "validation_status": "failed",
                "validation_step": "cases_controls_consistency",
                "message": "Cases/controls consistency validation failed",
                **cases_controls_results
            }
        all_warnings.extend(cases_controls_results.get('warnings', []))

        # 2. Validate co-occurrence files consistency
        cooccurrence_results = validate_cooccurrence_file_consistency(cohort_id)
        if cooccurrence_results.get('errors'):
            return {
                "cohort_id": cohort_id,
                "validation_status": "failed",
                "validation_step": "cooccurrence_consistency",
                "message": "Co-occurrence consistency validation failed",
                **cooccurrence_results
            }
        all_warnings.extend(cooccurrence_results.get('warnings', []))

        # 3. Validate cross-file consistency (co-occurrence vs cases/controls)
        cross_file_results = validate_cohort_cross_file_consistency(cohort_id)
        if cross_file_results.get('errors'):
            return {
                "cohort_id": cohort_id,
                "validation_status": "failed",
                "validation_step": "cross_file_consistency",
                "message": "Cross-file consistency validation failed",
                **cross_file_results
            }
        all_warnings.extend(cross_file_results.get('warnings', []))

        # If we get here, all validations passed
        has_warnings = len(all_warnings) > 0

        return {
            "cohort_id": cohort_id,
            "validation_status": "passed_with_warnings" if has_warnings else "passed",
            "validation_step": "all_completed",
            "message": "All consistency validations passed",
            "warnings": all_warnings,
            "validation_summary": {
                "cases_controls_files_found": cases_controls_results.get('files_found', []),
                "cooccurrence_files_found": cooccurrence_results.get('files_found', []),
                "cross_file_validation_results": cross_file_results.get('validation_results', {})
            }
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


