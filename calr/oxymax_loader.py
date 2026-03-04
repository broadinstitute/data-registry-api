"""
Oxymax/CLAMS calorimetry data loader and converter.

Port of loadOxyFile.R and modOxy.R
"""

import pandas as pd
import numpy as np
import re
from pathlib import Path
from typing import List, Dict, Union


def load_oxymax_file(file_path: Union[str, Path]) -> Dict:
    """
    Load Oxymax/CLAMS file.

    Port of loadOxyFile function from R.
    Parses the Oxymax CSV format which has:
    - Header line ("Oxymax CSV File")
    - Metadata section (key,value pairs) up to :DATA marker
    - Data section (:DATA to :EVENTS) with measurement rows
    - Optional events section (:EVENTS to end)

    Args:
        file_path: Path to Oxymax CSV file

    Returns:
        Dict with 'meta_data' (DataFrame), 'measurements' (DataFrame),
        and optionally 'events' (DataFrame)
    """
    file_path = Path(file_path)

    # Read all lines
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        lines = f.readlines()

    # Try latin1 encoding if non-ASCII detected
    try:
        for line in lines:
            line.encode('ascii')
    except UnicodeEncodeError:
        with open(file_path, 'r', encoding='latin1') as f:
            lines = f.readlines()

    # Clean lines: sub semicolons for commas, remove trailing commas
    lines = [line.replace(';', ',') for line in lines]
    lines = [re.sub(r',*$', '', line.rstrip()) for line in lines]

    # Find section markers
    data_idx = None
    events_idx = None
    for i, line in enumerate(lines):
        if ':DATA' in line:
            data_idx = i
        if ':EVENTS' in line:
            events_idx = i

    if data_idx is None:
        raise ValueError("Could not find :DATA marker in Oxymax file")
    if events_idx is None:
        events_idx = len(lines)

    # --- Parse metadata (lines 3 to data_idx-1) ---
    meta_lines = lines[2:data_idx]
    meta_lines = [l for l in meta_lines if l.strip()]

    # Parse as key,value pairs
    meta_pairs = []
    for line in meta_lines:
        # Split on first comma (or colon for SUBJECT lines)
        parts = line.split(',', 1)
        if len(parts) == 2:
            meta_pairs.append(parts)
        elif 'SUBJECT' in line.upper():
            parts = line.split(':', 1)
            if len(parts) == 2:
                meta_pairs.append(parts)
            else:
                meta_pairs.append([line.strip(), ''])
        else:
            meta_pairs.append([line.strip(), ''])

    # Build metadata DataFrame (transposed like R: columns are keys, single row of values)
    if meta_pairs:
        keys = [p[0].strip() for p in meta_pairs]
        vals = [p[1].strip() for p in meta_pairs]
        meta_data = pd.DataFrame([vals], columns=keys)
    else:
        meta_data = pd.DataFrame()

    # Normalize column names to match R's make.names behavior
    meta_data.columns = [_make_name(c) for c in meta_data.columns]

    # Map to expected column names (handle various naming from make.names)
    # Subject ID: could be 'SUBJECT.ID', 'Subject.ID', 'Subject.Id', etc.
    id_col = next((c for c in meta_data.columns
                   if 'subject' in c.lower() and 'id' in c.lower()), None)
    if id_col and id_col != 'Subject.ID':
        meta_data = meta_data.rename(columns={id_col: 'Subject.ID'})

    # Subject Mass: could be 'Subject.Mass.G', 'Subject.Mass..G.', 'SUBJECT.MASS', etc.
    mass_col = next((c for c in meta_data.columns
                     if 'subject' in c.lower() and 'mass' in c.lower()), None)
    if mass_col and mass_col != 'Subject.Mass..G.':
        meta_data = meta_data.rename(columns={mass_col: 'Subject.Mass..G.'})
    if 'Subject.Mass..G.' in meta_data.columns:
        meta_data['Subject.Mass..G.'] = meta_data['Subject.Mass..G.'].str.replace(
            r'[a-zA-Z]', '', regex=True
        ).str.strip()

    # --- Parse measurements (data_idx+1 to events_idx-1) ---
    data_lines = lines[data_idx + 1:events_idx]
    # Filter out separator lines, comment lines, and blank lines
    data_lines = [l for l in data_lines
                  if l.strip()
                  and not l.strip().startswith('#')
                  and not l.strip().startswith('=')
                  and not l.strip().startswith(' ')]

    if not data_lines:
        raise ValueError("No measurement data found in Oxymax file")

    # Parse into rows
    data_rows = [line.split(',') for line in data_lines]

    # Handle column count mismatches (R logic: add/remove trailing colname)
    header_cols = len(data_rows[0])
    max_data_cols = max(len(row) for row in data_rows[1:]) if len(data_rows) > 1 else header_cols
    min_data_cols = min(len(row) for row in data_rows[1:]) if len(data_rows) > 1 else header_cols

    if max_data_cols > header_cols:
        data_rows[0].append('X1')
        header_cols += 1
    if min_data_cols < header_cols:
        data_rows[0] = data_rows[0][:min_data_cols]
        header_cols = min_data_cols

    # Trim all rows to consistent width
    data_rows = [row[:header_cols] for row in data_rows]
    # Pad short rows
    data_rows = [row + [''] * (header_cols - len(row)) for row in data_rows]

    headers = [_make_name(h.strip()) for h in data_rows[0]]
    measurements = pd.DataFrame(data_rows[1:], columns=headers)

    # Trim whitespace from DATE.TIME
    if 'DATE.TIME' in measurements.columns:
        measurements['DATE.TIME'] = measurements['DATE.TIME'].str.strip()

        # Remove rows where DATE.TIME is just a time or empty
        invalid = measurements['DATE.TIME'].str.match(r'^\d{2}:\d{2}:\d{2}$') | \
                  (measurements['DATE.TIME'] == '')
        measurements = measurements[~invalid].reset_index(drop=True)

        # Remove rows starting with "12:00:00 AM"
        measurements = measurements[
            ~measurements['DATE.TIME'].str.startswith('12:00:00 AM')
        ].reset_index(drop=True)

    # --- Parse events (optional) ---
    events = pd.DataFrame()
    if events_idx < len(lines) - 1:
        event_lines = lines[events_idx + 1:]
        event_lines = [l for l in event_lines
                       if l.strip()
                       and not l.strip().startswith('=')]
        if event_lines:
            event_rows = [line.split(',') for line in event_lines]
            ncols = len(event_rows[0])
            event_rows = [row[:ncols] for row in event_rows]
            event_rows = [row + [''] * (ncols - len(row)) for row in event_rows]
            if len(event_rows) > 1:
                headers_e = [_make_name(h.strip()) for h in event_rows[0]]
                events = pd.DataFrame(event_rows[1:], columns=headers_e)
                # Remove empty columns
                events = events.loc[:, (events != '').any(axis=0)]

    result = {'meta_data': meta_data, 'measurements': measurements}
    if not events.empty:
        result['events'] = events

    return result


def convert_oxymax(input_data: Union[Dict, List[Dict]]) -> pd.DataFrame:
    """
    Convert Oxymax data to standard CalR format.

    Port of modOxy function from R.

    Handles single file (Dict) or multiple files (List[Dict]) where each
    file represents one cage/subject.

    Args:
        input_data: Output from load_oxymax_file() - single dict or list of dicts

    Returns:
        DataFrame in standard CalR format
    """
    print("====> Running modOxy <====")

    # Normalize to list
    if isinstance(input_data, dict):
        input_data = [input_data]

    # Step 1: Flatten all cages into single DataFrame
    all_rows = []
    for file_data in input_data:
        meta = file_data['meta_data']
        msmt = file_data['measurements'].copy()

        subject_id = ''
        if 'Subject.ID' in meta.columns:
            subject_id = str(meta['Subject.ID'].iloc[0]).strip()

        subject_mass = ''
        if 'Subject.Mass..G.' in meta.columns:
            subject_mass = str(meta['Subject.Mass..G.'].iloc[0]).strip()

        msmt.insert(0, 'subject.id', subject_id)
        msmt.insert(1, 'subject.mass', subject_mass)
        all_rows.append(msmt)

    clams_data = pd.concat(all_rows, ignore_index=True)

    # Step 2: Parse DATE.TIME
    clams_data['DATE.TIME'] = _parse_datetime(clams_data['DATE.TIME'])

    # Drop rows with NaT datetime
    clams_data = clams_data[clams_data['DATE.TIME'].notna()].reset_index(drop=True)

    # Step 3: Create time bins
    clams_data['minute'] = clams_data['DATE.TIME'].dt.floor('min')
    clams_data['hour'] = clams_data['DATE.TIME'].dt.floor('h')
    clams_data['day'] = clams_data['DATE.TIME'].dt.floor('D')

    # Global exp.minute (before per-subject correction)
    global_min_minute = clams_data['minute'].min()
    clams_data['exp.minute'] = (
        clams_data['minute'] - global_min_minute
    ).dt.total_seconds() / 60

    # Per-subject minute offset correction (matching R's corMinIds loop)
    subject_ids = clams_data['subject.id'].unique()
    for sid in subject_ids:
        mask = clams_data['subject.id'] == sid
        offset = clams_data.loc[mask, 'exp.minute'].iloc[0]
        clams_data.loc[mask, 'exp.minute'] = clams_data.loc[mask, 'exp.minute'] - offset
        clams_data.loc[mask, 'minute'] = (
            clams_data.loc[mask, 'minute'] - pd.Timedelta(minutes=offset)
        )

    # exp.hour and exp.day (global, no per-subject correction in R for these)
    min_hour = clams_data['hour'].min()
    clams_data['exp.hour'] = (clams_data['hour'] - min_hour).dt.total_seconds() / 3600
    min_day = clams_data['day'].min()
    clams_data['exp.day'] = (clams_data['day'] - min_day).dt.total_seconds() / 86400

    # Step 4: Build output DataFrame
    subject_mass_numeric = pd.to_numeric(clams_data['subject.mass'], errors='coerce')

    clams_df = pd.DataFrame({
        'subject.id': clams_data['subject.id'],
        'subject.mass': subject_mass_numeric,
        'cage': clams_data.get('CHAN', pd.Series(dtype=str)),
        'Date.Time': clams_data['DATE.TIME'],
        'vo2': pd.to_numeric(clams_data.get('VO2', np.nan), errors='coerce') *
               subject_mass_numeric / 1000,
        'vco2': pd.to_numeric(clams_data.get('VCO2', np.nan), errors='coerce') *
                subject_mass_numeric / 1000,
        'ee': pd.to_numeric(clams_data.get('HEAT', np.nan), errors='coerce'),
        'rer': pd.to_numeric(clams_data.get('RER', np.nan), errors='coerce'),
        'feed': np.nan,
        'feed.acc': np.nan,
        'drink': np.nan,
        'drink.acc': np.nan,
        'xytot': np.nan,
        'xyamb': np.nan,
        'wheel': np.nan,
        'wheel.acc': np.nan,
        'pedmeter': np.nan,
        'allmeter': np.nan,
        'body.temp': np.nan,
        'minute': clams_data['minute'],
        'hour': clams_data['hour'],
        'day': clams_data['day'],
        'exp.minute': clams_data['exp.minute'],
        'exp.hour': clams_data['exp.hour'],
        'exp.day': clams_data['exp.day'],
    })

    # ee.acc = cumulative sum of ee per subject
    clams_df['ee.acc'] = clams_df.groupby('subject.id')['ee'].cumsum()

    # If subject.id is empty, use cage
    empty_mask = (clams_df['subject.id'] == '') | clams_df['subject.id'].isna()
    clams_df.loc[empty_mask, 'subject.id'] = clams_df.loc[empty_mask, 'cage'].astype(str)

    # Step 5: Optional sensor columns
    if 'FEED1' in clams_data.columns:
        clams_df['feed'] = pd.to_numeric(clams_data['FEED1'], errors='coerce')
    if 'FEED1.ACC' in clams_data.columns:
        clams_df['feed.acc'] = pd.to_numeric(clams_data['FEED1.ACC'], errors='coerce')

    if 'DRINK1' in clams_data.columns:
        clams_df['drink'] = pd.to_numeric(clams_data['DRINK1'], errors='coerce')
    if 'DRINK1.ACC' in clams_data.columns:
        clams_df['drink.acc'] = pd.to_numeric(clams_data['DRINK1.ACC'], errors='coerce')

    if 'XTOT' in clams_data.columns:
        clams_df['xytot'] = pd.to_numeric(clams_data['XTOT'], errors='coerce')
        if 'YTOT' in clams_data.columns:
            clams_df['xytot'] = (
                pd.to_numeric(clams_data['XTOT'], errors='coerce') +
                pd.to_numeric(clams_data['YTOT'], errors='coerce')
            )
        if 'XAMB' in clams_data.columns:
            clams_df['xyamb'] = pd.to_numeric(clams_data['XAMB'], errors='coerce')
            if 'YAMB' in clams_data.columns:
                clams_df['xyamb'] = (
                    pd.to_numeric(clams_data['XAMB'], errors='coerce') +
                    pd.to_numeric(clams_data['YAMB'], errors='coerce')
                )

    if 'WHEEL' in clams_data.columns:
        clams_df['wheel'] = pd.to_numeric(clams_data['WHEEL'], errors='coerce')
    if 'WHEEL.ACC' in clams_data.columns:
        clams_df['wheel.acc'] = pd.to_numeric(clams_data['WHEEL.ACC'], errors='coerce')

    if 'BODY.TEMP' in clams_data.columns:
        clams_df['body.temp'] = pd.to_numeric(clams_data['BODY.TEMP'], errors='coerce')
    elif 'TEMP' in clams_data.columns:
        clams_df['body.temp'] = pd.to_numeric(clams_data['TEMP'], errors='coerce')

    return clams_df


def _make_name(s: str) -> str:
    """Mimic R's make.names: replace non-alphanumeric chars with dots."""
    s = s.strip()
    s = re.sub(r'[^a-zA-Z0-9.]', '.', s)
    # Collapse multiple dots
    s = re.sub(r'\.{2,}', '.', s)
    # Remove trailing dots
    s = s.rstrip('.')
    return s


def _parse_datetime(datetime_series: pd.Series) -> pd.Series:
    """
    Parse datetime strings, handling US vs international format.

    Matches R's AsDateTime logic: try US format first, if consecutive
    dates are >1 day apart, try international format.
    """
    try:
        parsed = pd.to_datetime(datetime_series, format='mixed',
                                dayfirst=False, errors='coerce')

        # Check if we need international format
        unique_dates = parsed.dropna().dt.date.unique()
        if len(unique_dates) > 1:
            sorted_dates = sorted(unique_dates)
            date_diff = (sorted_dates[1] - sorted_dates[0]).days
            if date_diff != 1 and date_diff != 0:
                parsed = pd.to_datetime(datetime_series, format='mixed',
                                        dayfirst=True, errors='coerce')

        return parsed
    except Exception:
        try:
            return pd.to_datetime(datetime_series, errors='coerce')
        except Exception:
            return pd.to_datetime(datetime_series, dayfirst=True, errors='coerce')
