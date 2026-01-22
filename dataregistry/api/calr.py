"""
CalR API endpoints - Calorimetry data upload and visualization.

Handles file upload, parsing (TSE, Oxymax, Sable, CalR formats),
and data processing for interactive chart generation.
"""
import io
import re
from datetime import datetime
from typing import Dict, List, Optional
from uuid import uuid4

import fastapi
import pandas as pd
from fastapi import UploadFile, File
from pydantic import BaseModel, Field

router = fastapi.APIRouter()

# In-memory session storage (for MVP - replace with Redis/DB for production)
_sessions: Dict[str, pd.DataFrame] = {}


# --- Pydantic Models ---

class UploadResponse(BaseModel):
    """Response after successful file upload."""
    session_id: str
    subjects: List[str]
    variables: List[str]
    date_range: Dict[str, str]
    total_hours: int
    row_count: int


class ProcessRequest(BaseModel):
    """Request to process uploaded data for charting."""
    session_id: str
    subjects: List[str]
    variable: str = Field(default="ee", description="Variable to plot")
    aggregate_by: str = Field(default="hour", description="'raw' or 'hour'")
    dark_hour: int = Field(default=18, ge=0, le=23)
    light_hour: int = Field(default=6, ge=0, le=23)


class TraceData(BaseModel):
    """Data for a single chart trace (one subject)."""
    subject_id: str
    x: List[float]
    y: List[Optional[float]]


class DarkRegion(BaseModel):
    """Dark period region for chart shading."""
    x0: float
    x1: float


class ProcessResponse(BaseModel):
    """Response containing chart-ready data."""
    traces: List[TraceData]
    dark_regions: List[DarkRegion]
    y_label: str
    variable: str


# --- File Parsing ---

def detect_format(content: str) -> str:
    """Detect calorimetry file format from content."""
    lines = content.split('\n')[:5]
    first_line = lines[0] if lines else ''

    if re.search(r'oxymax', first_line, re.IGNORECASE):
        return 'oxymax'
    if any('TSE' in line for line in lines):
        return 'tse'
    if re.search(r'Date_Time_\d', first_line):
        return 'sable'
    if re.search(r'^cage', first_line, re.IGNORECASE):
        return 'calr'
    return 'generic'


def parse_tse(content: str) -> pd.DataFrame:
    """Parse TSE format calorimetry file."""
    lines = content.split('\n')

    # Handle European CSV (semicolons, comma decimals)
    if ';' in lines[0]:
        lines = [
            re.sub(r'(\d),(\d)', r'\1.\2', line).replace(';', ',')
            for line in lines
        ]

    # Find data header row
    data_start = None
    for i, line in enumerate(lines):
        if re.match(r'^Date[,\t]', line, re.IGNORECASE):
            data_start = i
            break

    if data_start is None:
        raise ValueError("Could not find data header in TSE file")

    # Skip units row if present
    header_line = lines[data_start]
    data_lines = lines[data_start + 1:]
    if data_lines and re.match(r'^\s*,*\s*\[', data_lines[0]):
        data_lines = data_lines[1:]

    # Parse CSV
    csv_content = header_line + '\n' + '\n'.join(data_lines)
    df = pd.read_csv(io.StringIO(csv_content), skipinitialspace=True)

    # Standardize columns
    col_map = {
        'Animal No.': 'subject_id', 'Animal No': 'subject_id',
        'Box': 'cage',
        'VO2(3)': 'vo2', 'VO2': 'vo2',
        'VCO2(3)': 'vco2', 'VCO2': 'vco2',
        'H(3)': 'ee', 'H': 'ee', 'EE': 'ee',
        'RER': 'rer',
        'Feed': 'feed',
        'Drink': 'drink',
        'XT': 'activity', 'XTot': 'activity',
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    # Combine Date + Time
    if 'Date' in df.columns and 'Time' in df.columns:
        df['datetime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'], errors='coerce')
    elif 'Date Time' in df.columns:
        df['datetime'] = pd.to_datetime(df['Date Time'], errors='coerce')
    elif 'Date.Time' in df.columns:
        df['datetime'] = pd.to_datetime(df['Date.Time'], errors='coerce')

    df['subject_id'] = df['subject_id'].astype(str)

    return df


def parse_calr(content: str) -> pd.DataFrame:
    """Parse CalR format file (already standardized)."""
    df = pd.read_csv(io.StringIO(content))

    # Normalize column names
    df.columns = df.columns.str.lower().str.replace('.', '_', regex=False)

    if 'date_time' in df.columns:
        df['datetime'] = pd.to_datetime(df['date_time'], errors='coerce')
    elif 'datetime' in df.columns:
        df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')

    if 'subject_id' not in df.columns and 'subjectid' in df.columns:
        df['subject_id'] = df['subjectid']

    df['subject_id'] = df['subject_id'].astype(str)

    return df


def parse_generic(content: str) -> pd.DataFrame:
    """Attempt to parse unknown CSV format."""
    df = pd.read_csv(io.StringIO(content))
    df.columns = df.columns.str.lower().str.strip()

    # Find datetime column
    date_col = next((c for c in df.columns if 'date' in c or 'time' in c), None)
    if date_col:
        df['datetime'] = pd.to_datetime(df[date_col], errors='coerce')

    # Find subject column
    subj_col = next((c for c in df.columns if any(x in c for x in ['subject', 'animal', 'mouse', 'id'])), None)
    if subj_col:
        df['subject_id'] = df[subj_col].astype(str)
    else:
        df['subject_id'] = 'unknown'

    return df


def parse_file(content: str, filename: str) -> pd.DataFrame:
    """Parse calorimetry file, auto-detecting format."""
    fmt = detect_format(content)

    if fmt == 'tse':
        df = parse_tse(content)
    elif fmt == 'calr':
        df = parse_calr(content)
    else:
        df = parse_generic(content)

    # Ensure required columns exist
    required = ['datetime', 'subject_id']
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    # Drop rows with invalid datetime
    df = df.dropna(subset=['datetime'])

    if df.empty:
        raise ValueError("No valid data rows found")

    return df


# --- Data Processing ---

def get_variable_label(variable: str) -> str:
    """Get human-readable label for variable."""
    labels = {
        'vo2': 'Oxygen Consumption (ml/hr)',
        'vco2': 'CO2 Production (ml/hr)',
        'ee': 'Energy Expenditure (kcal/hr)',
        'rer': 'Respiratory Exchange Ratio',
        'feed': 'Food Intake (g)',
        'drink': 'Water Intake (ml)',
        'activity': 'Locomotor Activity (counts)',
    }
    return labels.get(variable, variable)


def calculate_dark_regions(max_hours: float, dark_hour: int, light_hour: int, start_clock_hour: int = 0) -> List[DarkRegion]:
    """
    Calculate dark period regions for chart shading.

    Args:
        max_hours: Total experiment duration in hours
        dark_hour: Clock hour when dark period starts (e.g., 18 for 6pm)
        light_hour: Clock hour when light period starts (e.g., 6 for 6am)
        start_clock_hour: Clock hour when experiment started (e.g., 6 if started at 6am)
    """
    regions = []

    # Iterate through each hour and check if it's dark
    # This is simpler and more reliable than trying to calculate ranges
    hour = 0
    in_dark = False
    dark_start = None

    while hour <= max_hours:
        # What clock hour does this experiment hour correspond to?
        clock_hour = (start_clock_hour + int(hour)) % 24

        # Is this clock hour in the dark period?
        if light_hour < dark_hour:
            # Normal cycle: dark from dark_hour to light_hour (overnight)
            is_dark = clock_hour >= dark_hour or clock_hour < light_hour
        else:
            # Inverted cycle: dark from dark_hour to light_hour (same day)
            is_dark = clock_hour >= dark_hour and clock_hour < light_hour

        if is_dark and not in_dark:
            # Entering dark period
            dark_start = hour
            in_dark = True
        elif not is_dark and in_dark:
            # Leaving dark period
            regions.append(DarkRegion(x0=dark_start, x1=hour))
            in_dark = False

        hour += 1

    # Close final dark region if still in dark
    if in_dark and dark_start is not None:
        regions.append(DarkRegion(x0=dark_start, x1=max_hours))

    return regions


def process_data(
    df: pd.DataFrame,
    subjects: List[str],
    variable: str,
    aggregate_by: str,
    dark_hour: int,
    light_hour: int,
) -> ProcessResponse:
    """Process data and return chart-ready format."""
    # Filter subjects
    df = df[df['subject_id'].isin(subjects)].copy()

    if df.empty:
        return ProcessResponse(
            traces=[],
            dark_regions=[],
            y_label=get_variable_label(variable),
            variable=variable
        )

    # Calculate experiment hour (hours since start)
    min_time = df['datetime'].min()
    df['exp_hour'] = (df['datetime'] - min_time).dt.total_seconds() / 3600

    # Aggregate by hour if requested
    if aggregate_by == 'hour':
        df['hour_bin'] = df['exp_hour'].astype(int)
        numeric_cols = df.select_dtypes(include='number').columns.tolist()
        agg_cols = [c for c in numeric_cols if c not in ['hour_bin', 'exp_hour']]

        df = df.groupby(['subject_id', 'hour_bin']).agg({
            **{col: 'mean' for col in agg_cols},
            'exp_hour': 'first'
        }).reset_index()
        df['exp_hour'] = df['hour_bin'].astype(float)

    # Build traces
    traces = []
    for subject_id in subjects:
        subj_df = df[df['subject_id'] == subject_id].sort_values('exp_hour')
        if subj_df.empty:
            continue

        y_values = subj_df[variable].tolist() if variable in subj_df.columns else []

        traces.append(TraceData(
            subject_id=subject_id,
            x=subj_df['exp_hour'].tolist(),
            y=[v if pd.notna(v) else None for v in y_values]
        ))

    # Calculate dark regions (add 1 hour buffer to match chart x-axis)
    max_hours = (df['exp_hour'].max() + 1) if not df.empty else 24
    start_clock_hour = min_time.hour  # Clock hour when experiment started
    dark_regions = calculate_dark_regions(max_hours, dark_hour, light_hour, start_clock_hour)

    return ProcessResponse(
        traces=traces,
        dark_regions=dark_regions,
        y_label=get_variable_label(variable),
        variable=variable
    )


# --- API Endpoints ---

@router.post("/calr/upload", response_model=UploadResponse)
async def upload_calr_file(file: UploadFile = File(...)):
    """
    Upload and parse a calorimetry data file.

    Supports TSE, Oxymax, Sable, and CalR formats.
    Returns a session_id for subsequent processing requests.
    """
    try:
        contents = await file.read()
        content_str = contents.decode('utf-8')
    except UnicodeDecodeError:
        # Try latin-1 for European files
        content_str = contents.decode('latin-1')

    try:
        df = parse_file(content_str, file.filename or '')
    except Exception as e:
        raise fastapi.HTTPException(status_code=400, detail=f"Failed to parse file: {str(e)}")

    # Generate session ID and store data
    session_id = str(uuid4())
    _sessions[session_id] = df

    # Get available subjects and variables
    subjects = sorted(df['subject_id'].unique().tolist())
    variables = [col for col in ['vo2', 'vco2', 'ee', 'rer', 'feed', 'drink', 'activity']
                 if col in df.columns]

    # Calculate date range
    date_range = {
        'start': df['datetime'].min().isoformat(),
        'end': df['datetime'].max().isoformat(),
    }
    total_hours = int((df['datetime'].max() - df['datetime'].min()).total_seconds() / 3600)

    return UploadResponse(
        session_id=session_id,
        subjects=subjects,
        variables=variables,
        date_range=date_range,
        total_hours=total_hours,
        row_count=len(df)
    )


@router.post("/calr/process", response_model=ProcessResponse)
async def process_calr_data(request: ProcessRequest):
    """
    Process uploaded data for chart visualization.

    Filters by subjects, aggregates by hour if requested,
    and returns traces with dark period regions.
    """
    df = _sessions.get(request.session_id)
    if df is None:
        raise fastapi.HTTPException(status_code=404, detail="Session not found. Please upload a file first.")

    if request.variable not in df.columns:
        raise fastapi.HTTPException(
            status_code=400,
            detail=f"Variable '{request.variable}' not found. Available: {[c for c in df.columns if c not in ['datetime', 'subject_id']]}"
        )

    return process_data(
        df=df,
        subjects=request.subjects,
        variable=request.variable,
        aggregate_by=request.aggregate_by,
        dark_hour=request.dark_hour,
        light_hour=request.light_hour,
    )


@router.delete("/calr/session/{session_id}")
async def delete_session(session_id: str):
    """Delete a session and free memory."""
    if session_id in _sessions:
        del _sessions[session_id]
        return {"message": "Session deleted"}
    raise fastapi.HTTPException(status_code=404, detail="Session not found")
