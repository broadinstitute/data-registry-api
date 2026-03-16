import json
import os
import re
from typing import Dict, List, Optional

import fastapi
import httpx
import boto3
import smart_open
from botocore.exceptions import ClientError
from fastapi import BackgroundTasks
from pydantic import BaseModel
from starlette.requests import Request
from streaming_form_data import StreamingFormDataParser
from streaming_form_data.targets import S3Target

from dataregistry.api import s3
from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api.model import User, NewUserRequest
from dataregistry.api.hcm_model import HCMGWASFile, HCMGWASValidationJob
from dataregistry.api import hcm_query
from dataregistry.api.mskkp import suggest_column_map

router = fastapi.APIRouter()
engine = DataRegistryReadWriteDB().get_engine()

USER_SERVICE_URL = os.getenv('USER_SERVICE_URL', 'https://users.kpndataregistry.org')
HCM_UPLOADER_TOKEN = os.getenv('HCM_UPLOADER_TOKEN')
HCM_REVIEWER_TOKEN = os.getenv('HCM_REVIEWER_TOKEN')

HCM_GWAS_VALIDATOR_JOB_QUEUE = os.getenv('HCM_GWAS_VALIDATOR_JOB_QUEUE', 'hcm-gwas-validator-queue')
HCM_GWAS_VALIDATOR_JOB_DEFINITION = os.getenv('HCM_GWAS_VALIDATOR_JOB_DEFINITION', 'hcm-gwas-validator-job')

# Valid enum values from the HCM GWAS analysis plan
VALID_SARC = {'ALL', 'SP', 'SN'}
VALID_ANCESTRY = {'EUR', 'AFR', 'EAS', 'SAS', 'AMR', 'ALL'}
VALID_SEX = {'ALL', 'MALE', 'FEMALE'}
VALID_GENOME_BUILD = {'GRCh37', 'GRCh38'}

# Required summary statistics fields per the HCM GWAS analysis plan (Section 9.1)
HCM_TARGET_FIELDS = [
    'variant_id',
    'chromosome',
    'position',
    'effect_allele',
    'non_effect_allele',
    'beta',
    'standard_error',
    'p_value',
    'effect_allele_frequency',
    'sample_size',
    'cases',
    'controls',
    'imputation_quality',
]

# Common GWAS column name aliases mapped to HCM canonical target field names
HCM_COLUMN_ALIASES = {
    # variant id
    'snp': 'variant_id',
    'snpid': 'variant_id',
    'rsid': 'variant_id',
    'id': 'variant_id',
    'variant': 'variant_id',
    'variant_id': 'variant_id',
    'markerid': 'variant_id',
    'markername': 'variant_id',
    # chromosome
    'chr': 'chromosome',
    'chrom': 'chromosome',
    '#chrom': 'chromosome',
    '#chr': 'chromosome',
    'chromosome': 'chromosome',
    # position
    'bp': 'position',
    'pos': 'position',
    'position': 'position',
    'base_pair_location': 'position',
    'bp_pos': 'position',
    'genpos': 'position',
    # effect allele
    'a1': 'effect_allele',
    'effect_allele': 'effect_allele',
    'allele1': 'effect_allele',
    'coded_allele': 'effect_allele',
    'ea': 'effect_allele',
    # non-effect allele
    'a2': 'non_effect_allele',
    'other_allele': 'non_effect_allele',
    'non_effect_allele': 'non_effect_allele',
    'allele2': 'non_effect_allele',
    'nea': 'non_effect_allele',
    'ref': 'non_effect_allele',
    # beta
    'beta': 'beta',
    'effect': 'beta',
    'effect_size': 'beta',
    'b': 'beta',
    # standard error
    'se': 'standard_error',
    'stderr': 'standard_error',
    'standard_error': 'standard_error',
    'sebeta': 'standard_error',
    # p-value
    'p': 'p_value',
    'pval': 'p_value',
    'pvalue': 'p_value',
    'p_value': 'p_value',
    'p-value': 'p_value',
    'p_val': 'p_value',
    # effect allele frequency
    'eaf': 'effect_allele_frequency',
    'freq': 'effect_allele_frequency',
    'a1freq': 'effect_allele_frequency',
    'a1_freq': 'effect_allele_frequency',
    'maf': 'effect_allele_frequency',
    'frq': 'effect_allele_frequency',
    'effect_allele_frequency': 'effect_allele_frequency',
    # sample size
    'n': 'sample_size',
    'n_total': 'sample_size',
    'sample_size': 'sample_size',
    'samplesize': 'sample_size',
    'neff': 'sample_size',
    'total_n': 'sample_size',
    # cases
    'cases': 'cases',
    'n_cases': 'cases',
    'ncases': 'cases',
    'n_case': 'cases',
    # controls
    'controls': 'controls',
    'n_controls': 'controls',
    'ncontrols': 'controls',
    'n_control': 'controls',
    # imputation quality
    'info': 'imputation_quality',
    'rsq': 'imputation_quality',
    'r2': 'imputation_quality',
    'imputation_quality': 'imputation_quality',
    'info_score': 'imputation_quality',
}

# File naming pattern: HCM_{SARC}_{STUDY}_{ANCESTRY}_{SEX}_{BUILD}_{SOFTWARE}_{ANALYST}_{DDMMYY}.gz
HCM_FILENAME_PATTERN = re.compile(
    r'^HCM_(?P<sarc>ALL|SP|SN)_(?P<study>[^_]+)_(?P<ancestry>EUR|AFR|EAS|SAS|AMR|ALL)'
    r'_(?P<sex>ALL|MALE|FEMALE)_(?P<build>GRCh37|GRCh38)_(?P<software>[^_]+)'
    r'_(?P<analyst>[^_]+)_(?P<date>\d{6})\.gz$'
)


class GzipS3Target(S3Target):
    def __init__(self, path, mode='wb', transport_params=None):
        super().__init__(path, mode, transport_params)

    def on_start(self):
        self._fd = smart_open.open(
            self._file_path,
            self._mode,
            compression='disable',
            transport_params=self._transport_params,
        )


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def check_review_permissions(user: User):
    return user.permissions and "hcm-review-data" in user.permissions


def check_add_user_permissions(user: User):
    return user.permissions and "hcm-add-user" in user.permissions


async def get_hcm_user(authorization: Optional[str] = fastapi.Header(None)):
    if not authorization:
        raise fastapi.HTTPException(status_code=401, detail='Authorization header required')

    schema, _, token = authorization.partition(' ')
    if schema.lower() != 'bearer' or not token:
        raise fastapi.HTTPException(status_code=401, detail='Bearer token required')

    hcm_user_group = os.getenv('HCM_USER_GROUP', 'hcm')

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{USER_SERVICE_URL}/api/auth/verify/",
                params={"group": hcm_user_group},
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


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def _validate_enum_fields(sarc: str, ancestry: str, sex: str, genome_build: str):
    """Validate controlled vocabulary fields. Raises HTTPException on failure."""
    errors = []
    if sarc not in VALID_SARC:
        errors.append(f"Invalid sarc '{sarc}'. Must be one of: {sorted(VALID_SARC)}")
    if ancestry not in VALID_ANCESTRY:
        errors.append(f"Invalid ancestry '{ancestry}'. Must be one of: {sorted(VALID_ANCESTRY)}")
    if sex not in VALID_SEX:
        errors.append(f"Invalid sex '{sex}'. Must be one of: {sorted(VALID_SEX)}")
    if genome_build not in VALID_GENOME_BUILD:
        errors.append(f"Invalid genome_build '{genome_build}'. Must be one of: {sorted(VALID_GENOME_BUILD)}")
    if errors:
        raise fastapi.HTTPException(status_code=400, detail="; ".join(errors))


def _validate_filename(filename: str) -> Optional[str]:
    """Check filename against the HCM naming convention. Returns error message or None."""
    if not HCM_FILENAME_PATTERN.match(filename):
        return (
            f"Filename '{filename}' does not match the required pattern: "
            "HCM_{{SARC}}_{{STUDY}}_{{ANCESTRY}}_{{SEX}}_{{BUILD}}_{{SOFTWARE}}_{{ANALYST}}_{{DDMMYY}}.gz"
        )
    return None


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------

class HCMColumnMapSuggestionRequest(BaseModel):
    columns: List[str]


class HCMGWASUploadInitRequest(BaseModel):
    cohort_name: str
    sarc: str
    ancestry: str
    sex: str
    genome_build: str
    software: str
    analyst: str
    filename: str
    column_mapping: Dict[str, str]
    cases: Optional[int] = None
    controls: Optional[int] = None
    metadata: Optional[Dict] = None


class HCMGWASUploadConfirmRequest(BaseModel):
    cohort_name: str
    sarc: str
    ancestry: str
    sex: str
    genome_build: str
    software: str
    analyst: str
    filename: str
    file_size: int
    s3_key: str
    column_mapping: Dict[str, str]
    cases: Optional[int] = None
    controls: Optional[int] = None
    metadata: Optional[Dict] = None


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/hcm/suggest-column-map")
async def suggest_hcm_column_mapping(request: HCMColumnMapSuggestionRequest, user: User = fastapi.Depends(get_hcm_user)):
    """Suggest mappings from uploaded file columns to HCM GWAS required fields."""
    suggested = suggest_column_map(request.columns, HCM_TARGET_FIELDS, aliases=HCM_COLUMN_ALIASES)
    return {
        "suggested_map": suggested,
        "target_fields": HCM_TARGET_FIELDS,
        "unmatched_targets": [t for t in HCM_TARGET_FIELDS if t not in suggested.values()],
    }


@router.post("/hcm/gwas-upload-url")
async def generate_hcm_gwas_upload_url(request: HCMGWASUploadInitRequest, user: User = fastapi.Depends(get_hcm_user)):
    """Generate a presigned S3 PUT URL for uploading an HCM GWAS file."""
    try:
        _validate_enum_fields(request.sarc, request.ancestry, request.sex, request.genome_build)

        filename_err = _validate_filename(request.filename)
        if filename_err:
            raise fastapi.HTTPException(status_code=400, detail=filename_err)

        s3_key = f"hcm/gwas/{request.cohort_name}/{request.sarc}/{request.ancestry}/{request.sex}/{request.filename}"

        presigned_url = s3.generate_presigned_url(
            'put_object',
            params={'Bucket': s3.BASE_BUCKET, 'Key': s3_key},
            expires_in=7200
        )

        return {
            "presigned_url": presigned_url,
            "s3_key": s3_key,
            "s3_path": f"s3://{s3.BASE_BUCKET}/{s3_key}",
            "expires_in_seconds": 7200
        }
    except fastapi.HTTPException:
        raise
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"Error generating upload URL: {str(e)}")


@router.post("/hcm/confirm-gwas-upload")
async def confirm_hcm_gwas_upload(request: HCMGWASUploadConfirmRequest, user: User = fastapi.Depends(get_hcm_user)):
    """Validate an uploaded HCM GWAS file in S3 and create the database record."""
    try:
        _validate_enum_fields(request.sarc, request.ancestry, request.sex, request.genome_build)

        filename_err = _validate_filename(request.filename)
        if filename_err:
            raise fastapi.HTTPException(status_code=400, detail=filename_err)

        # Check for duplicate
        existing = hcm_query.get_hcm_gwas_file_by_s3_path(engine, request.s3_key)
        if existing:
            raise fastapi.HTTPException(
                status_code=409,
                detail=f"A GWAS file already exists at this path. Delete the existing file (id: {existing['id']}) before uploading a new one."
            )

        gwas_file = HCMGWASFile(
            cohort_name=request.cohort_name,
            sarc=request.sarc,
            ancestry=request.ancestry,
            sex=request.sex,
            genome_build=request.genome_build,
            software=request.software,
            analyst=request.analyst,
            file_name=request.filename,
            file_size=request.file_size,
            s3_path=request.s3_key,
            uploaded_by=user.user_name,
            column_mapping=request.column_mapping,
            cases=request.cases,
            controls=request.controls,
            metadata=request.metadata
        )

        file_id = hcm_query.insert_hcm_gwas_file(engine, gwas_file)

        return {
            "message": "GWAS file upload confirmed",
            "file_id": file_id,
            "cohort_name": request.cohort_name,
            "sarc": request.sarc,
            "ancestry": request.ancestry,
            "sex": request.sex,
            "genome_build": request.genome_build,
            "file_name": request.filename,
            "file_size": request.file_size,
            "s3_path": f"s3://{s3.BASE_BUCKET}/{request.s3_key}",
            "uploaded_by": user.user_name
        }
    except fastapi.HTTPException:
        raise
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"Error confirming upload: {str(e)}")


@router.post("/hcm/upload-gwas-stream")
async def upload_hcm_gwas_stream(request: Request, user: User = fastapi.Depends(get_hcm_user)):
    """Stream upload an HCM GWAS file directly to S3 without buffering on disk.

    Required headers:
    - cohort_name, sarc, ancestry, sex, genome_build, software, analyst
    - filename, column_mapping (JSON string)

    Optional headers:
    - cases (int), controls (int), metadata (JSON string)
    """
    try:
        cohort_name = request.headers.get('cohort_name')
        sarc = request.headers.get('sarc')
        ancestry = request.headers.get('ancestry')
        sex = request.headers.get('sex')
        genome_build = request.headers.get('genome_build')
        software = request.headers.get('software')
        analyst = request.headers.get('analyst')
        filename = request.headers.get('filename')
        column_mapping_str = request.headers.get('column_mapping')
        metadata_str = request.headers.get('metadata')
        cases_str = request.headers.get('cases')
        controls_str = request.headers.get('controls')

        # Validate required fields
        if not all([cohort_name, sarc, ancestry, sex, genome_build, software, analyst, filename, column_mapping_str]):
            raise fastapi.HTTPException(
                status_code=400,
                detail="Missing required headers: cohort_name, sarc, ancestry, sex, genome_build, software, analyst, filename, column_mapping"
            )

        _validate_enum_fields(sarc, ancestry, sex, genome_build)

        filename_err = _validate_filename(filename)
        if filename_err:
            raise fastapi.HTTPException(status_code=400, detail=filename_err)

        # Parse optional fields
        cases = None
        controls = None
        if cases_str:
            try:
                cases = int(cases_str)
            except ValueError:
                raise fastapi.HTTPException(status_code=400, detail="cases header must be an integer")
        if controls_str:
            try:
                controls = int(controls_str)
            except ValueError:
                raise fastapi.HTTPException(status_code=400, detail="controls header must be an integer")

        try:
            col_map = json.loads(column_mapping_str)
        except json.JSONDecodeError as e:
            raise fastapi.HTTPException(status_code=400, detail=f"Invalid column_mapping JSON: {str(e)}")

        meta_dict = {}
        if metadata_str:
            try:
                meta_dict = json.loads(metadata_str)
            except json.JSONDecodeError as e:
                raise fastapi.HTTPException(status_code=400, detail=f"Invalid metadata JSON: {str(e)}")

        s3_key = f"hcm/gwas/{cohort_name}/{sarc}/{ancestry}/{sex}/{filename}"
        s3_path = f"s3://{s3.BASE_BUCKET}/{s3_key}"

        # Check for duplicate
        existing = hcm_query.get_hcm_gwas_file_by_s3_path(engine, s3_key)
        if existing:
            raise fastapi.HTTPException(
                status_code=409,
                detail=f"A GWAS file already exists at this path. Delete the existing file (id: {existing['id']}) before uploading a new one."
            )

        # Stream the file to S3
        file_size = 0
        parser = StreamingFormDataParser(request.headers)
        s3_target = GzipS3Target(s3_path, mode='wb')
        parser.register('file', s3_target)

        async for chunk in request.stream():
            parser.data_received(chunk)
            file_size += len(chunk)

        if file_size == 0:
            raise fastapi.HTTPException(status_code=400, detail="File is empty")

        gwas_file = HCMGWASFile(
            cohort_name=cohort_name,
            sarc=sarc,
            ancestry=ancestry,
            sex=sex,
            genome_build=genome_build,
            software=software,
            analyst=analyst,
            file_name=filename,
            file_size=file_size,
            s3_path=s3_key,
            uploaded_by=user.user_name,
            column_mapping=col_map,
            cases=cases,
            controls=controls,
            metadata=meta_dict or None
        )

        file_id = hcm_query.insert_hcm_gwas_file(engine, gwas_file)

        return {
            "message": "GWAS file uploaded successfully",
            "file_id": file_id,
            "cohort_name": cohort_name,
            "sarc": sarc,
            "ancestry": ancestry,
            "sex": sex,
            "genome_build": genome_build,
            "file_name": filename,
            "file_size": file_size,
            "s3_path": f"s3://{s3.BASE_BUCKET}/{s3_key}",
            "uploaded_by": user.user_name
        }

    except fastapi.HTTPException:
        raise
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"Error uploading file: {str(e)}")


@router.get("/hcm/gwas-files")
async def get_hcm_gwas_files(user: User = fastapi.Depends(get_hcm_user)):
    """List HCM GWAS files. Reviewers see all; uploaders see only their own."""
    try:
        if check_review_permissions(user):
            return hcm_query.get_all_hcm_gwas_files(engine)
        else:
            return hcm_query.get_hcm_gwas_files_by_uploader(engine, user.user_name)
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"Error retrieving GWAS files: {str(e)}")


@router.get("/hcm/gwas-files/{cohort_name}")
async def get_hcm_gwas_files_by_cohort(cohort_name: str, user: User = fastapi.Depends(get_hcm_user)):
    """List HCM GWAS files for a specific cohort."""
    try:
        files = hcm_query.get_hcm_gwas_files_by_cohort(engine, cohort_name)

        # Non-reviewers can only see their own files
        if not check_review_permissions(user):
            files = [f for f in files if f['uploaded_by'] == user.user_name]

        return files
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"Error retrieving GWAS files: {str(e)}")


@router.get("/hcm/gwas-file/{file_id}/download")
async def download_hcm_gwas_file(file_id: str, user: User = fastapi.Depends(get_hcm_user)):
    """Get a presigned download URL for an HCM GWAS file."""
    try:
        gwas_file = hcm_query.get_hcm_gwas_file_by_id(engine, file_id)
        if not gwas_file:
            raise fastapi.HTTPException(status_code=404, detail="GWAS file not found")

        # Permission check: uploader or reviewer
        if not (gwas_file['uploaded_by'] == user.user_name or check_review_permissions(user)):
            raise fastapi.HTTPException(status_code=403, detail="You can only download files you uploaded")

        s3_key = gwas_file['s3_path']
        presigned_url = s3.get_signed_url(s3.BASE_BUCKET, s3_key)

        return {
            "presigned_url": presigned_url,
            "file_name": gwas_file['file_name'],
            "file_size": gwas_file['file_size'],
            "cohort_name": gwas_file['cohort_name'],
            "sarc": gwas_file['sarc'],
            "ancestry": gwas_file['ancestry'],
            "sex": gwas_file['sex'],
            "genome_build": gwas_file['genome_build']
        }

    except fastapi.HTTPException:
        raise
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"Error downloading GWAS file: {str(e)}")


@router.delete("/hcm/gwas-file/{file_id}")
async def delete_hcm_gwas_file(file_id: str, user: User = fastapi.Depends(get_hcm_user)):
    """Delete an HCM GWAS file (S3 object + database record)."""
    try:
        gwas_file = hcm_query.get_hcm_gwas_file_by_id(engine, file_id)
        if not gwas_file:
            raise fastapi.HTTPException(status_code=404, detail="GWAS file not found")

        # Permission check: uploader or reviewer
        if not (gwas_file['uploaded_by'] == user.user_name or check_review_permissions(user)):
            raise fastapi.HTTPException(status_code=403, detail="You can only delete files you uploaded")

        # Delete from S3
        s3_client = boto3.client('s3', region_name=s3.S3_REGION)
        try:
            s3_client.delete_object(Bucket=s3.BASE_BUCKET, Key=gwas_file['s3_path'])
        except Exception:
            pass  # Don't fail if S3 delete fails

        # Delete from database
        deleted = hcm_query.delete_hcm_gwas_file(engine, file_id)
        if not deleted:
            raise fastapi.HTTPException(status_code=404, detail="GWAS file not found")

        return {"message": "GWAS file deleted successfully", "file_id": file_id}

    except fastapi.HTTPException:
        raise
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"Error deleting GWAS file: {str(e)}")


@router.get("/hcm/gwas-summary")
async def get_hcm_gwas_summary(user: User = fastapi.Depends(get_hcm_user)):
    """Reviewer-only summary of all HCM GWAS uploads."""
    if not check_review_permissions(user):
        raise fastapi.HTTPException(
            status_code=403,
            detail="You need 'hcm-review-data' permission to access GWAS summary"
        )

    try:
        return hcm_query.get_all_hcm_gwas_files(engine)
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"Error retrieving GWAS summary: {str(e)}")


# ---------------------------------------------------------------------------
# User Management
# ---------------------------------------------------------------------------

@router.get("/hcm/users")
async def get_all_hcm_users(user: User = fastapi.Depends(get_hcm_user)):
    """Get all HCM users from the dig-user-service. Requires reviewer permissions."""
    if not check_review_permissions(user):
        raise fastapi.HTTPException(
            status_code=403,
            detail="You need 'hcm-review-data' permission to list users"
        )

    token = HCM_UPLOADER_TOKEN or HCM_REVIEWER_TOKEN
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
                except Exception:
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
    except fastapi.HTTPException:
        raise
    except Exception as e:
        raise fastapi.HTTPException(
            status_code=500,
            detail=f"Error retrieving users: {str(e)}"
        )


@router.post("/hcm/create-user")
async def create_hcm_user(request: NewUserRequest, user: User = fastapi.Depends(get_hcm_user)):
    """Create a new HCM user via the dig-user-service. Requires 'hcm-add-user' permission."""
    if not check_add_user_permissions(user):
        raise fastapi.HTTPException(
            status_code=403,
            detail="You need 'hcm-add-user' permission to create users"
        )

    token = None
    if request.user_type == 'uploader':
        token = HCM_UPLOADER_TOKEN
    elif request.user_type == 'reviewer':
        token = HCM_REVIEWER_TOKEN
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
        user_data = {
            "token": token,
            "username": request.user_name,
            "password": request.password
        }

        if request.first_name:
            user_data["first_name"] = request.first_name
        if request.last_name:
            user_data["last_name"] = request.last_name
        if request.user_name:
            user_data["email"] = request.user_name

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{USER_SERVICE_URL}/api/auth/create-user/",
                data=user_data
            )

            if response.status_code in (200, 201):
                return {
                    "message": "User created successfully",
                    "username": request.user_name,
                    "user_type": request.user_type
                }
            else:
                try:
                    error_detail = response.json()
                except Exception:
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
    except fastapi.HTTPException:
        raise
    except Exception as e:
        raise fastapi.HTTPException(
            status_code=500,
            detail=f"Error creating user: {str(e)}"
        )


# ---------------------------------------------------------------------------
# GWAS Row-Level Validation (AWS Batch)
# ---------------------------------------------------------------------------

def _submit_gwas_validation_and_await(engine_ref, file_id: str, s3_path: str,
                                      column_mapping: dict, submitted_by: str,
                                      job_record_id: str):
    """Background task: submit Batch job, poll until done, update DB."""
    batch_client = boto3.client('batch', region_name=s3.S3_REGION)
    s3_client = boto3.client('s3', region_name=s3.S3_REGION)

    progress_s3_key = f"hcm/gwas-validation/{file_id}/progress.json"

    try:
        response = batch_client.submit_job(
            jobName=f'hcm-gwas-validator-{file_id[:8]}',
            jobQueue=HCM_GWAS_VALIDATOR_JOB_QUEUE,
            jobDefinition=HCM_GWAS_VALIDATOR_JOB_DEFINITION,
            parameters={
                's3-path': s3_path,
                'column-mapping': json.dumps(column_mapping),
                'progress-s3-key': progress_s3_key,
                'bucket': s3.BASE_BUCKET,
            },
        )
        batch_job_id = response['jobId']

        hcm_query.update_hcm_gwas_validation_job_status(
            engine_ref, job_record_id, 'RUNNING', batch_job_id=batch_job_id
        )

        while True:
            import time
            time.sleep(10)
            response = batch_client.describe_jobs(jobs=[batch_job_id])
            job_status = response['jobs'][0]['status']
            if job_status in ('SUCCEEDED', 'FAILED'):
                break

        # Read final progress JSON from S3 for error summary
        total_rows = None
        errors_found = None
        error_samples = None
        try:
            resp = s3_client.get_object(Bucket=s3.BASE_BUCKET, Key=progress_s3_key)
            progress = json.loads(resp['Body'].read().decode('utf-8'))
            total_rows = progress.get('total_rows')
            errors_found = progress.get('errors_found')
            error_samples = progress.get('error_samples')
        except Exception:
            pass

        final_status = 'COMPLETED' if job_status == 'SUCCEEDED' else 'FAILED'
        hcm_query.update_hcm_gwas_validation_job_status(
            engine_ref, job_record_id, final_status,
            total_rows=total_rows,
            errors_found=errors_found,
            error_summary=error_samples,
        )

    except Exception:
        try:
            hcm_query.update_hcm_gwas_validation_job_status(
                engine_ref, job_record_id, 'FAILED'
            )
        except Exception:
            pass


def _kick_off_gwas_validation(background_tasks: BackgroundTasks, file_id: str,
                              s3_path: str, column_mapping: dict,
                              submitted_by: str) -> str:
    """Create a DB record and schedule the background validation task. Returns job record ID."""
    progress_s3_key = f"hcm/gwas-validation/{file_id}/progress.json"
    job = HCMGWASValidationJob(
        file_id=file_id,
        status='SUBMITTED',
        progress_s3_key=progress_s3_key,
        submitted_by=submitted_by,
    )
    job_record_id = hcm_query.insert_hcm_gwas_validation_job(engine, job)

    background_tasks.add_task(
        _submit_gwas_validation_and_await,
        engine, file_id, s3_path, column_mapping, submitted_by, job_record_id,
    )
    return job_record_id


@router.post("/hcm/gwas-validate/{file_id}")
async def start_hcm_gwas_validation(file_id: str, background_tasks: BackgroundTasks,
                                    user: User = fastapi.Depends(get_hcm_user)):
    """Kick off an AWS Batch job to validate every row of an HCM GWAS file.

    Creates a DB record in hcm_gwas_validation_jobs, submits the Batch job
    in the background, and returns immediately.
    """
    try:
        gwas_file = hcm_query.get_hcm_gwas_file_by_id(engine, file_id)
        if not gwas_file:
            raise fastapi.HTTPException(status_code=404, detail="GWAS file not found")

        if not (gwas_file['uploaded_by'] == user.user_name or check_review_permissions(user)):
            raise fastapi.HTTPException(status_code=403, detail="Access denied")

        job_record_id = _kick_off_gwas_validation(
            background_tasks, file_id,
            gwas_file['s3_path'],
            gwas_file.get('column_mapping', {}),
            user.user_name,
        )

        return {
            "message": "Validation job submitted",
            "validation_job_id": job_record_id,
            "file_id": file_id,
            "progress_s3_key": f"hcm/gwas-validation/{file_id}/progress.json",
        }

    except fastapi.HTTPException:
        raise
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"Error submitting validation job: {str(e)}")


@router.get("/hcm/gwas-validate/{file_id}/progress")
async def get_hcm_gwas_validation_progress(file_id: str, user: User = fastapi.Depends(get_hcm_user)):
    """Get validation status for an HCM GWAS file.

    If the most recent job is still running, enriches the response with live
    progress read directly from the S3 progress JSON.
    """
    try:
        gwas_file = hcm_query.get_hcm_gwas_file_by_id(engine, file_id)
        if not gwas_file:
            raise fastapi.HTTPException(status_code=404, detail="GWAS file not found")

        if not (gwas_file['uploaded_by'] == user.user_name or check_review_permissions(user)):
            raise fastapi.HTTPException(status_code=403, detail="Access denied")

        jobs = hcm_query.get_hcm_gwas_validation_jobs_by_file_id(engine, file_id)
        if not jobs:
            raise fastapi.HTTPException(
                status_code=404,
                detail="No validation jobs found for this file."
            )

        latest = jobs[0]
        if latest['status'] in ('SUBMITTED', 'RUNNING') and latest.get('progress_s3_key'):
            try:
                s3_client = boto3.client('s3', region_name=s3.S3_REGION)
                resp = s3_client.get_object(Bucket=s3.BASE_BUCKET, Key=latest['progress_s3_key'])
                latest['live_progress'] = json.loads(resp['Body'].read().decode('utf-8'))
            except ClientError:
                latest['live_progress'] = None

        return {"validation_jobs": jobs}

    except fastapi.HTTPException:
        raise
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"Error reading validation progress: {str(e)}")


@router.get("/hcm/gwas-validate/{file_id}/errors")
async def get_hcm_gwas_validation_errors_url(file_id: str, user: User = fastapi.Depends(get_hcm_user)):
    """Return a presigned S3 download URL for the full validation error log (TSV).

    Returns 404 if no validation job exists or no errors were recorded.
    """
    try:
        gwas_file = hcm_query.get_hcm_gwas_file_by_id(engine, file_id)
        if not gwas_file:
            raise fastapi.HTTPException(status_code=404, detail="GWAS file not found")

        if not (gwas_file['uploaded_by'] == user.user_name or check_review_permissions(user)):
            raise fastapi.HTTPException(status_code=403, detail="Access denied")

        jobs = hcm_query.get_hcm_gwas_validation_jobs_by_file_id(engine, file_id)
        if not jobs:
            raise fastapi.HTTPException(status_code=404, detail="No validation jobs found for this file")

        latest = jobs[0]
        progress_s3_key = latest.get("progress_s3_key")
        if not progress_s3_key:
            raise fastapi.HTTPException(status_code=404, detail="No error log available")

        errors_key = progress_s3_key.replace("progress.json", "errors.tsv")
        try:
            presigned_url = s3.get_signed_url(s3.BASE_BUCKET, errors_key)
        except ClientError:
            raise fastapi.HTTPException(
                status_code=404,
                detail="No error log found — validation may have completed with no errors."
            )

        return {
            "errors_url": presigned_url,
            "file_id": file_id,
            "errors_s3_key": errors_key,
        }

    except fastapi.HTTPException:
        raise
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"Error retrieving error log: {str(e)}")
