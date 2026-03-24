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

from dataregistry.api import s3
from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api.model import User
from dataregistry.api.calr_model import CALRFile, CALRSubmission, CalRSession, AnovaRequest, PowerCalcRequest, QualityControlRequest
from dataregistry.api import calr_query

# Import CalR conversion functions
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from calr.loaders import detect_format, load_cal_file
from calr.oxymax_loader import load_oxymax_file, convert_oxymax
from calr.sable_loader import load_sable_file, convert_sable
from calr.tse_loader import load_tse_file, convert_tse
from calr.analysis import acute_ancova, filter_by_time_of_day, power_calc, quality_control

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


@router.post("/calr/files", status_code=201)
async def upload_calr_files(
    standard_file: UploadFile,
    name: str = Form(...),
    description: str = Form(''),
    public: bool = Form(False),
    user: User = Depends(get_calr_user)
):
    """
    Upload a standard CalR format file to create a submission.
    Sessions are created separately via POST /calr/sessions.
    """
    try:
        submission_id = str(uuid.uuid4()).replace('-', '')

        submission = CALRSubmission(
            id=submission_id,
            name=name,
            description=description or None,
            public=public,
            uploaded_by=user.user_name
        )
        saved_sub_id = calr_query.insert_calr_submission(engine, submission)

        content = await standard_file.read()
        s3_key = f"calr/{user.user_name}/{saved_sub_id}/standard/{standard_file.filename}"
        _upload_file_to_s3(content, s3_key, standard_file.content_type)

        calr_file = CALRFile(
            submission_id=saved_sub_id,
            file_type='standard',
            file_name=standard_file.filename,
            file_size=len(content),
            s3_path=s3_key,
        )
        file_id = calr_query.insert_calr_file(engine, calr_file)

        return {
            "submission_id": saved_sub_id,
            "file_id": file_id,
            "name": name,
            "public": public,
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
        return calr_query.get_calr_submissions_by_user(engine, user.user_name)
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
        return calr_query.get_public_calr_submissions(engine)
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
        file_info = calr_query.get_calr_file_by_id(engine, file_id)
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
        file_info = calr_query.get_calr_file_by_id(engine, file_id)
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
        files = calr_query.get_calr_files_by_submission(engine, submission_id)
        if not files:
            raise fastapi.HTTPException(status_code=404, detail="Submission not found")

        # Verify ownership by checking one of the files (they share the same submission)
        file_info = calr_query.get_calr_file_by_id(engine, files[0]['id'])
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
        deleted = calr_query.delete_calr_submission(engine, submission_id)
        if not deleted:
            raise fastapi.HTTPException(status_code=404, detail="Submission not found")

        return {"message": "Submission deleted successfully", "submission_id": submission_id}

    except fastapi.HTTPException:
        raise
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"Error deleting submission: {str(e)}")


def _validate_session_against_standard_file(session: CalRSession, standard_df) -> list[str]:
    """
    Validate session configuration against the standard CalR dataframe.
    Returns a list of error strings (empty if valid).
    """
    errors = []

    file_subjects = set(standard_df['subject.id'].astype(str).unique())

    # Check all groupIndexes are valid
    for subj in session.subjects:
        if subj.groupIndex < 0 or subj.groupIndex >= len(session.groups):
            errors.append(
                f"Subject '{subj.subject}' has invalid groupIndex {subj.groupIndex} "
                f"(session has {len(session.groups)} group(s))"
            )

    # Check all subjects exist in the file
    missing = [s.subject for s in session.subjects if s.subject not in file_subjects]
    if missing:
        errors.append(f"Subjects not found in standard file: {missing}")

    # Check hour_range is within the file's exp.hour bounds
    if 'exp.hour' in standard_df.columns:
        file_min = float(standard_df['exp.hour'].min())
        file_max = float(standard_df['exp.hour'].max())
        start, end = session.hour_range
        if start < file_min or end > file_max:
            errors.append(
                f"hour_range [{start}, {end}] is outside the file's exp.hour range [{file_min}, {file_max}]"
            )
        if start >= end:
            errors.append(f"hour_range start ({start}) must be less than end ({end})")

    return errors


@router.post("/calr/sessions", status_code=201)
async def create_calr_session(
    session: CalRSession,
    user: User = Depends(get_calr_user)
):
    """
    Create and persist a CalR experiment session.

    Validates the session configuration against the linked submission's standard file,
    then stores it in S3. Returns the session ID for use in subsequent analysis requests.
    """
    import pandas as pd

    # Verify the submission exists and belongs to this user
    files = calr_query.get_calr_files_by_submission(engine, session.submission_id)
    if not files:
        raise fastapi.HTTPException(status_code=404, detail="Submission not found")
    if files[0]['uploaded_by'] != user.user_name:
        raise fastapi.HTTPException(status_code=403, detail="Access denied")

    standard_file = next((f for f in files if f['file_type'] == 'standard'), None)
    if not standard_file:
        raise fastapi.HTTPException(status_code=404, detail="Standard file not found in submission")

    # Download and parse the standard file
    try:
        s3_client = boto3.client('s3', region_name=s3.S3_REGION)
        s3_response = s3_client.get_object(Bucket=s3.BASE_BUCKET, Key=standard_file['s3_path'])
        standard_df = pd.read_csv(s3_response['Body'])
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"Error reading standard file: {str(e)}")

    # Validate session against the standard file
    errors = _validate_session_against_standard_file(session, standard_df)
    if errors:
        raise fastapi.HTTPException(status_code=422, detail=errors)

    try:
        session_id = str(uuid.uuid4()).replace('-', '')
        s3_key = f"calr/{user.user_name}/sessions/{session_id}.json"
        _upload_file_to_s3(session.json().encode('utf-8'), s3_key, 'application/json')
        return {"session_id": session_id}
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"Error creating session: {str(e)}")


@router.get("/calr/sessions/{session_id}")
async def get_calr_session(
    session_id: str,
    user: User = Depends(get_calr_user)
):
    """
    Retrieve a CalR session by ID.

    Streams the session JSON from S3. Users can only retrieve their own sessions.
    """
    s3_key = f"calr/{user.user_name}/sessions/{session_id}.json"
    try:
        s3_client = boto3.client('s3', region_name=s3.S3_REGION)
        s3_response = s3_client.get_object(Bucket=s3.BASE_BUCKET, Key=s3_key)
    except s3_client.exceptions.NoSuchKey:
        raise fastapi.HTTPException(status_code=404, detail="Session not found")
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"Error retrieving session: {str(e)}")

    def stream():
        for chunk in s3_response['Body'].iter_chunks(chunk_size=8192):
            yield chunk

    return StreamingResponse(
        stream(),
        media_type='application/json',
        headers={"Content-Length": str(s3_response['ContentLength'])},
    )


def _load_session_and_standard_df(session_id: str, username: str):
    """
    Load a CalR session from S3 and its associated standard file as a DataFrame.
    Returns (session_dict, standard_df). Raises HTTPException on any failure.
    """
    import json
    import pandas as pd

    s3_client = boto3.client('s3', region_name=s3.S3_REGION)

    # Load session
    session_key = f"calr/{username}/sessions/{session_id}.json"
    try:
        session_obj = s3_client.get_object(Bucket=s3.BASE_BUCKET, Key=session_key)
        session_data = json.loads(session_obj['Body'].read())
    except s3_client.exceptions.NoSuchKey:
        raise fastapi.HTTPException(status_code=404, detail="Session not found")

    # Load standard file via submission_id stored in session
    submission_id = session_data.get('submission_id')
    if not submission_id:
        raise fastapi.HTTPException(status_code=422, detail="Session has no submission_id")

    files = calr_query.get_calr_files_by_submission(engine, submission_id)
    standard_file = next((f for f in files if f['file_type'] == 'standard'), None)
    if not standard_file:
        raise fastapi.HTTPException(status_code=404, detail="Standard file not found for session")

    try:
        file_obj = s3_client.get_object(Bucket=s3.BASE_BUCKET, Key=standard_file['s3_path'])
        standard_df = pd.read_csv(file_obj['Body'])
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"Error reading standard file: {str(e)}")

    return session_data, standard_df


@router.post("/calr/analysis/ancova")
async def run_ancova(
    request: AnovaRequest,
    user: User = Depends(get_calr_user)
):
    """
    Run per-hour ANCOVA on a CalR standard file using the given session configuration.

    For each hour in the analysis window, fits: variable ~ mass_variable + group
    Returns per-hour p-values, significance annotations, annotation y-positions,
    and per-group means and standard errors — everything a client needs to render
    a time-series plot with significance overlay.
    """
    if request.time_of_day not in ('light', 'dark', 'total'):
        raise fastapi.HTTPException(status_code=422, detail="time_of_day must be 'light', 'dark', or 'total'")

    session, df = _load_session_and_standard_df(request.session_id, user.user_name)

    if request.variable not in df.columns:
        raise fastapi.HTTPException(status_code=422, detail=f"Variable '{request.variable}' not found in standard file")
    if request.mass_variable not in df.columns:
        raise fastapi.HTTPException(status_code=422, detail=f"Mass variable '{request.mass_variable}' not found in standard file")

    # Assign groups from session
    groups = session['groups']
    subject_to_group = {
        s['subject']: groups[s['groupIndex']]['name']
        for s in session['subjects']
    }
    df['group'] = df['subject.id'].astype(str).map(subject_to_group)
    df = df[df['group'].notna()]

    # Apply hour range from session
    start_hour, end_hour = session['hour_range']
    df = df[(df['exp.hour'] >= start_hour) & (df['exp.hour'] <= end_hour)]

    # Filter by time of day
    df = filter_by_time_of_day(
        df,
        request.time_of_day,
        session['light_cycle_start'],
        session['dark_cycle_start'],
    )

    if df.empty:
        raise fastapi.HTTPException(status_code=422, detail="No data remaining after filters")

    result = acute_ancova(df, request.variable, request.mass_variable)
    return result


@router.post("/calr/analysis/power")
async def run_power_calc(
    request: PowerCalcRequest,
    user: User = Depends(get_calr_user)
):
    """
    Compute a statistical power curve for a CalR experiment.

    Auto-selects ANCOVA (for 'ee', 'feed', 'feed.acc') or ANOVA (all others)
    based on the variable. Returns per-group summary statistics, the effect size,
    and power estimates across the requested sample sizes.
    """
    if request.time_of_day not in ('light', 'dark', 'total'):
        raise fastapi.HTTPException(status_code=422, detail="time_of_day must be 'light', 'dark', or 'total'")

    session, df = _load_session_and_standard_df(request.session_id, user.user_name)

    if request.variable not in df.columns:
        raise fastapi.HTTPException(status_code=422, detail=f"Variable '{request.variable}' not found in standard file")
    if request.mass_variable not in df.columns:
        raise fastapi.HTTPException(status_code=422, detail=f"Mass variable '{request.mass_variable}' not found in standard file")

    # Assign groups from session
    groups = session['groups']
    subject_to_group = {
        s['subject']: groups[s['groupIndex']]['name']
        for s in session['subjects']
    }
    df['group'] = df['subject.id'].astype(str).map(subject_to_group)
    df = df[df['group'].notna()]

    # Apply hour range from session
    start_hour, end_hour = session['hour_range']
    df = df[(df['exp.hour'] >= start_hour) & (df['exp.hour'] <= end_hour)]

    df = filter_by_time_of_day(
        df,
        request.time_of_day,
        session['light_cycle_start'],
        session['dark_cycle_start'],
    )

    if df.empty:
        raise fastapi.HTTPException(status_code=422, detail="No data remaining after filters")

    try:
        result = power_calc(df, request.variable, request.mass_variable, request.sample_sizes, request.alpha)
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"Power calculation failed: {str(e)}")

    return result


@router.post("/calr/analysis/qc")
async def run_quality_control(
    request: QualityControlRequest,
    user: User = Depends(get_calr_user)
):
    """
    Run the CalR quality control analysis.

    For each subject, computes mass change and total cumulative energy balance
    over the session's hour window, then fits per-group and overall linear
    regressions. A well-controlled experiment should show strong positive
    correlation between mass loss and negative energy balance.

    Returns per-subject data points and regression statistics for client-side
    scatter plot rendering.
    """
    session, df = _load_session_and_standard_df(request.session_id, user.user_name)

    for col in ('subject.mass', 'feed', 'ee'):
        if col not in df.columns:
            raise fastapi.HTTPException(status_code=422, detail=f"Required column '{col}' not found in standard file")

    groups = session['groups']
    subject_to_group = {
        s['subject']: groups[s['groupIndex']]['name']
        for s in session['subjects']
    }
    df['group'] = df['subject.id'].astype(str).map(subject_to_group)
    df = df[df['group'].notna()]

    start_hour, end_hour = session['hour_range']
    df = df[(df['exp.hour'] >= start_hour) & (df['exp.hour'] <= end_hour)]

    if df.empty:
        raise fastapi.HTTPException(status_code=422, detail="No data remaining after filters")

    try:
        result = quality_control(df, request.n_mass_measurements)
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=f"QC analysis failed: {str(e)}")

    return result
