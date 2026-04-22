"""
TSE calorimetry data loader and converter.

Port of loadTSEFile.R and modTSE.R
"""

import pandas as pd
import numpy as np
import re
from pathlib import Path
from typing import List, Dict, Union
from datetime import datetime


def load_tse_file(file_path: Union[str, Path]) -> List[Dict]:
    """
    Load TSE calorimetry file.
    
    Port of loadTSEFile function from R.
    Parses TSE format which has:
    - Metadata section (lines 3 to before "Parameter"/"Date")
    - Measurements section (from "Parameter"/"Date" to end)
    
    TSE files can be in:
    - Wide format: Box in first column, dates across columns
    - Long format: Date Time in first column, Box identifier in data
    
    Args:
        file_path: Path to TSE CSV file
        
    Returns:
        List of dicts, each containing:
            {
                'meta_data': DataFrame with metadata for one subject,
                'measurements': DataFrame with measurements for one subject
            }
    """
    file_path = Path(file_path)
    
    # Read all lines
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        lines = f.readlines()
    
    # Remove extra commas at end of lines and empty lines
    lines = [re.sub(r',*$', '', line.rstrip()) for line in lines]
    lines = [line for line in lines if line]
    
    # Check if semicolon format (European CSV)
    if ';' in lines[0]:
        # Convert European format: change decimal comma to period, semicolon to comma
        lines = [re.sub(r'(?<=\d),(?=\d)', '.', line) for line in lines]  # decimal
        lines = [line.replace(';', ',') for line in lines]  # delimiter
    
    # If TSE found on first line but not second, add dummy first line
    if 'TSE' in lines[0] and not any('TSE' in line for line in lines[1:4]):
        lines.insert(0, 'dummy')
    
    # Find where measurements start (line with "Parameter" or "Date")
    data_start_idx = None
    for i, line in enumerate(lines):
        if 'Parameter' in line or (i > 0 and 'Date' in line):
            data_start_idx = i
            break
    
    if data_start_idx is None:
        raise ValueError("Could not find data section (no 'Parameter' or 'Date' header found)")
    
    # Split into metadata and measurements sections
    meta_lines = lines[2:data_start_idx]  # Skip first 2 lines, go to data start
    data_lines = lines[data_start_idx:]
    
    # Parse metadata section into DataFrame
    meta_rows = [line.split(',') for line in meta_lines]
    if meta_rows:
        max_cols = max(len(row) for row in meta_rows)
        # Pad rows to same length
        meta_rows = [row + [''] * (max_cols - len(row)) for row in meta_rows]
        # Parse as normal CSV: first row is headers, rest is data
        meta_df = pd.DataFrame(meta_rows[1:], columns=meta_rows[0])
        # Remove empty columns
        meta_df = meta_df.loc[:, (meta_df != '').any(axis=0)]
    else:
        meta_df = pd.DataFrame()
    
    # Parse measurements section into DataFrame
    data_rows = [line.split(',') for line in data_lines]
    max_cols = max(len(row) for row in data_rows)
    data_rows = [row + [''] * (max_cols - len(row)) for row in data_rows]
    
    # Check format by looking at the first header
    headers = data_rows[0]
    is_wide_format = headers[0] == 'Box'
    
    if not is_wide_format:  # Long format (normal CSV layout)
        # Parse as standard CSV: first row is headers, rest is data
        msmt_df = pd.DataFrame(data_rows[2:], columns=data_rows[0])  # Skip header and units row
        # Remove empty columns
        msmt_df = msmt_df.loc[:, (msmt_df != '').any(axis=0)]
        
        # Combine Date and Time columns if they exist separately
        if 'Date' in msmt_df.columns and 'Time' in msmt_df.columns:
            msmt_df['Date Time'] = msmt_df['Date'] + ' ' + msmt_df['Time']
            msmt_df = msmt_df.drop(columns=['Date', 'Time'])
    else:  # Wide format (transposed layout)
        # Parse as transposed: transpose to get proper layout
        msmt_df = pd.DataFrame(data_rows).T
        msmt_df.columns = msmt_df.iloc[0]
        msmt_df = msmt_df.iloc[1:].reset_index(drop=True)
        # Remove empty columns
        msmt_df = msmt_df.loc[:, (msmt_df != '').any(axis=0)]
        
        # Combine Date and Time columns if they exist separately
        if 'Date' in msmt_df.columns and 'Time' in msmt_df.columns:
            msmt_df['Date Time'] = msmt_df['Date'] + ' ' + msmt_df['Time']
            msmt_df = msmt_df.drop(columns=['Date', 'Time'])
    
    # Determine format and process accordingly
    cal_df_list = []
    
    if is_wide_format:  # Wide format
        # In wide format, measurements are transposed: parameters in rows, timepoints in columns
        # DATE.TIME is in first data row, transposed across columns
        date_time_row = msmt_df.iloc[0, 2:].values  # Skip Box and first param columns
        
        # Get unique boxes from metadata
        unique_boxes = meta_df['Box'].unique()
        
        for box_id in unique_boxes:
            # Get metadata for this box (metadata is rows now, not columns)
            meta_subset = meta_df[meta_df['Box'] == str(box_id)].copy()
            
            # Get measurements for this box (still transposed)
            msmt_subset = msmt_df[msmt_df['Box'] == str(box_id)].copy()
            
            # Transpose measurements to normal format
            msmt_subset = msmt_subset.T
            msmt_subset.columns = msmt_subset.iloc[1]  # Use row 1 (parameter names) as columns
            msmt_subset = msmt_subset.iloc[2:]  # Skip Box and parameter name rows
            msmt_subset['DATE.TIME'] = date_time_row
            
            cal_df_list.append({
                'meta_data': meta_subset,
                'measurements': msmt_subset
            })
    
    else:  # Long format (Date Time or Date.Time in first column)
        # Combine Date and Time if not already done
        if 'Date Time' not in msmt_df.columns:
            first_col = msmt_df.columns[0]
            if first_col in ['Date', 'Date.Time']:
                msmt_df = msmt_df.rename(columns={first_col: 'Date Time'})
        
        # Get unique boxes
        if 'Box' not in msmt_df.columns:
            raise ValueError("Long format TSE file missing 'Box' column")
        
        unique_boxes = sorted(msmt_df['Box'].unique())
        
        for box_id in unique_boxes:
            # Get metadata for this box (metadata is in rows)
            meta_subset = meta_df[meta_df['Box'] == str(box_id)].copy()
            
            # Get measurements for this box
            msmt_subset = msmt_df[msmt_df['Box'] == str(box_id)].copy()
            msmt_subset = msmt_subset.rename(columns={'Date Time': 'DATE.TIME'})
            
            cal_df_list.append({
                'meta_data': meta_subset,
                'measurements': msmt_subset
            })
    
    return cal_df_list


def convert_tse(tse_data: List[Dict]) -> pd.DataFrame:
    """
    Convert TSE data to standard CalR format.
    
    Port of modTSE function from R.
    
    Key transformations:
    - Flatten list of subject dicts to single DataFrame
    - Parse and normalize Date.Time
    - Create time bins (minute, hour, day, exp.*)
    - Detect if Feed/Drink are cumulative or incremental per subject
    - Map TSE columns to CalR standard names
    - Calculate accumulated energy expenditure
    
    Args:
        tse_data: Output from load_tse_file()
        
    Returns:
        DataFrame in standard CalR format
    """
    print("====> Running modTSE <====")
    
    # Step 1: Flatten input into single dataframe
    rows = []
    for subject_data in tse_data:
        measurements = subject_data['measurements'].copy()
        metadata = subject_data['meta_data']
        
        # Add subject metadata to each measurement row
        measurements['subject.id'] = metadata['Animal No.'].iloc[0] if 'Animal No.' in metadata.columns else ''
        measurements['subject.mass'] = float(metadata['Weight [g]'].iloc[0]) if 'Weight [g]' in metadata.columns else np.nan
        measurements['cage'] = metadata['Box'].iloc[0] if 'Box' in metadata.columns else ''
        
        rows.append(measurements)
    
    tse_df = pd.concat(rows, ignore_index=True)
    
    # Reorder columns: subject info first
    cols = ['subject.id', 'subject.mass', 'cage', 'DATE.TIME'] + \
           [c for c in tse_df.columns if c not in ['subject.id', 'subject.mass', 'cage', 'DATE.TIME']]
    tse_df = tse_df[cols]
    
    # Step 2: Date/Time normalization
    tse_df['DATE.TIME'] = _parse_datetime(tse_df['DATE.TIME'])
    
    # Create time bins
    tse_df['minute'] = tse_df['DATE.TIME'].dt.floor('min')
    tse_df['hour'] = tse_df['DATE.TIME'].dt.floor('h')
    tse_df['day'] = tse_df['DATE.TIME'].dt.floor('D')
    
    # Calculate experimental time offsets (each uses its own bin minimum, matching R)
    tse_df['exp.minute'] = (tse_df['minute'] - tse_df['minute'].min()).dt.total_seconds() / 60
    tse_df['exp.hour'] = (tse_df['hour'] - tse_df['hour'].min()).dt.total_seconds() / 3600
    tse_df['exp.day'] = (tse_df['day'] - tse_df['day'].min()).dt.total_seconds() / 86400
    
    # Step 3: Normalize Feed/Drink column names
    tse_df = _normalize_feed_drink_columns(tse_df)
    
    # Step 4: Build output dataframe with standard CalR columns
    cal_df = pd.DataFrame({
        'subject.id': tse_df['subject.id'],
        'subject.mass': pd.to_numeric(tse_df['subject.mass'], errors='coerce'),
        'cage': tse_df['cage'],
        'Date.Time': tse_df['DATE.TIME'],
        'vo2': pd.to_numeric(tse_df.get('VO2(3)', np.nan), errors='coerce'),
        'vco2': pd.to_numeric(tse_df.get('VCO2(3)', np.nan), errors='coerce'),
        'ee': pd.to_numeric(tse_df.get('H(3)', np.nan), errors='coerce'),
        'rer': pd.to_numeric(tse_df.get('RER', np.nan), errors='coerce'),
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
        'minute': tse_df['minute'],
        'hour': tse_df['hour'],
        'day': tse_df['day'],
        'exp.minute': tse_df['exp.minute'],
        'exp.hour': tse_df['exp.hour'],
        'exp.day': tse_df['exp.day']
    })
    
    # Calculate accumulated EE per subject
    cal_df['ee.acc'] = cal_df.groupby('subject.id')['ee'].cumsum()
    
    # Step 5: Handle optional sensors
    if 'XT' in tse_df.columns and 'YT' in tse_df.columns:
        cal_df['xytot'] = pd.to_numeric(tse_df['XT'], errors='coerce') + \
                          pd.to_numeric(tse_df['YT'], errors='coerce')
    elif 'XT.YT' in tse_df.columns:
        cal_df['xytot'] = pd.to_numeric(tse_df['XT.YT'], errors='coerce')
    
    if 'XA' in tse_df.columns and 'YA' in tse_df.columns:
        cal_df['xyamb'] = pd.to_numeric(tse_df['XA'], errors='coerce') + \
                          pd.to_numeric(tse_df['YA'], errors='coerce')
    
    if 'SumR.L' in tse_df.columns:
        cal_df['wheel'] = pd.to_numeric(tse_df['SumR.L'], errors='coerce')
    
    if 'Weight' in tse_df.columns:
        cal_df['subject.mass'] = pd.to_numeric(tse_df['Weight'], errors='coerce')
    
    # Step 6: Detect cumulative vs incremental for Feed/Drink per subject
    if 'Feed' in tse_df.columns:
        cal_df = _handle_feed_drink_cumulative(cal_df, tse_df, 'Feed', 'feed', 'feed.acc')
    
    if 'Drink' in tse_df.columns:
        cal_df = _handle_feed_drink_cumulative(cal_df, tse_df, 'Drink', 'drink', 'drink.acc')
    
    return cal_df


def _parse_datetime(datetime_series: pd.Series) -> pd.Series:
    """
    Parse datetime strings, trying multiple formats.
    
    Handles US vs international date formats by checking if parsing
    as MM/DD/YYYY would result in dates > 1 day apart.
    """
    # First try: infer format automatically
    try:
        parsed = pd.to_datetime(datetime_series, infer_datetime_format=True, errors='coerce')
        
        # Check if we need to try international format
        unique_dates = parsed.dt.date.unique()
        if len(unique_dates) > 1:
            date_diff = (unique_dates[1] - unique_dates[0]).days
            if date_diff != 1:
                # Try international format (day first)
                parsed = pd.to_datetime(datetime_series, format='%d/%m/%Y %H:%M', errors='coerce')
                if parsed.isna().all():
                    # Try other common formats
                    parsed = pd.to_datetime(datetime_series, dayfirst=True, errors='coerce')
        
        return parsed
    except:
        # Fallback: try both US and international
        try:
            return pd.to_datetime(datetime_series, errors='coerce')
        except:
            return pd.to_datetime(datetime_series, dayfirst=True, errors='coerce')


def _normalize_feed_drink_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize Feed/Drink column names.
    
    TSE files can have:
    - Exact 'Feed' or 'Drink' columns
    - Multiple channels: 'Feed.1', 'Feed.2', etc.
    
    If multiple channels exist, sum them into a single 'Feed' or 'Drink' column.
    """
    df = df.copy()
    
    # Handle Feed columns
    feed_cols = [col for col in df.columns if re.match(r'^Feed(\.\d+)?$', col)]
    if feed_cols:
        if 'Feed' in feed_cols and len(feed_cols) == 1:
            # Already have single Feed column
            pass
        elif 'Feed' in feed_cols:
            # Have 'Feed' plus other channels - keep Feed as authoritative
            pass
        else:
            # Multiple channels, no exact 'Feed' - sum them
            df['Feed'] = df[feed_cols].apply(pd.to_numeric, errors='coerce').sum(axis=1, skipna=True)
    
    # Handle Drink columns
    drink_cols = [col for col in df.columns if re.match(r'^Drink(\.\d+)?$', col)]
    if drink_cols:
        if 'Drink' in drink_cols and len(drink_cols) == 1:
            # Already have single Drink column
            pass
        elif 'Drink' in drink_cols:
            # Have 'Drink' plus other channels - keep Drink as authoritative
            pass
        else:
            # Multiple channels, no exact 'Drink' - sum them
            df['Drink'] = df[drink_cols].apply(pd.to_numeric, errors='coerce').sum(axis=1, skipna=True)
    
    return df


def _is_cumulative(values: pd.Series) -> bool:
    """
    Detect if a series is cumulative (monotonically non-decreasing).
    
    Returns True if values are cumulative, False if incremental.
    Ignores NaN values.
    """
    values = pd.to_numeric(values, errors='coerce').dropna()
    
    if len(values) < 2:
        return True
    
    # Check if differences are non-negative (allowing for small floating point errors)
    diffs = values.diff().dropna()
    return (diffs >= -0.001).all()  # Allow tiny negative values due to float precision


def _handle_feed_drink_cumulative(cal_df: pd.DataFrame, tse_df: pd.DataFrame, 
                                   source_col: str, incr_col: str, acc_col: str) -> pd.DataFrame:
    """
    Handle Feed/Drink data that may be cumulative or incremental, per subject.
    
    For each subject:
    - Detect if data is cumulative or incremental
    - If cumulative: compute increments, use raw values as accumulated
    - If incremental: use raw values, compute accumulated
    
    Args:
        cal_df: Output CalR DataFrame being built
        tse_df: TSE DataFrame with source data
        source_col: Column name in tse_df ('Feed' or 'Drink')
        incr_col: Output column for incremental values ('feed' or 'drink')
        acc_col: Output column for accumulated values ('feed.acc' or 'drink.acc')
    
    Returns:
        Updated cal_df with feed/drink columns filled
    """
    cal_df = cal_df.copy()
    source_values = pd.to_numeric(tse_df[source_col], errors='coerce')
    
    # Process per subject
    for subject_id in cal_df['subject.id'].unique():
        mask = cal_df['subject.id'] == subject_id
        subject_values = source_values[mask]
        
        if _is_cumulative(subject_values):
            # Cumulative → increments are diffs, raw values are accumulated
            increments = subject_values.diff().fillna(0)
            increments.iloc[0] = 0  # First value is not an increment
            accumulated = subject_values
        else:
            # Incremental → raw values are increments, compute accumulated
            increments = subject_values.fillna(0)
            accumulated = increments.cumsum()
        
        cal_df.loc[mask, incr_col] = increments.values
        cal_df.loc[mask, acc_col] = accumulated.values
    
    return cal_df
