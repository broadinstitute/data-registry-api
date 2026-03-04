"""
Sable calorimetry data loader and converter.

Port of loadSableFile.R and modSable.R
"""

import pandas as pd
import numpy as np
import re
from pathlib import Path
from typing import Union


def load_sable_file(file_path: Union[str, Path]) -> pd.DataFrame:
    """
    Load Sable file.

    Port of loadSableFile function from R.
    Reads Sable CSV (wide format, columns like vo2_1, vo2_2, ...).
    Drops all-NA columns and incomplete rows.

    Args:
        file_path: Path to Sable CSV file

    Returns:
        DataFrame with wide-format Sable data
    """
    file_path = Path(file_path)

    # Read CSV (equivalent to R's read_csv_arrow)
    df = pd.read_csv(file_path)

    # Drop all-NA columns
    df = df.dropna(axis=1, how='all')

    # Drop rows with any NAs (equivalent to R's complete.cases)
    df = df.dropna(how='any').reset_index(drop=True)

    return df


def convert_sable(sable_data: pd.DataFrame) -> pd.DataFrame:
    """
    Convert Sable data to standard CalR format.

    Port of modSable function from R.

    Key steps:
    1. Remove duration columns
    2. Extract and replicate enviro sensor data across cages
    3. Reshape from wide to long format
    4. Parse dates, create time bins
    5. Map to standard CalR columns
    6. Compute feed/drink from accumulated values

    Args:
        sable_data: Output from load_sable_file()

    Returns:
        DataFrame in standard CalR format
    """
    print("====> Running modSable <====")

    df = sable_data.copy()

    # Step 1: Remove duration columns
    duration_cols = [c for c in df.columns if 'duration' in c.lower()]
    if duration_cols:
        df = df.drop(columns=duration_cols)

    # Step 2: Handle environmental columns
    enviro_cols = [c for c in df.columns if 'enviro' in c.lower()]
    enviro_df = df[enviro_cols].copy() if enviro_cols else pd.DataFrame()

    # Remove enviro columns from main data
    if enviro_cols:
        df = df.drop(columns=enviro_cols)

    # Determine cage/subject IDs from non-datetime column suffixes
    # Columns are like vo2_1, vo2_2, ... — suffixes after last underscore are cage IDs
    non_dt_cols = [c for c in df.columns if not c.startswith('Date_Time')]
    subj_ids = sorted(set(
        int(c.rsplit('_', 1)[1])
        for c in non_dt_cols
        if '_' in c and c.rsplit('_', 1)[1].isdigit()
    ))

    # Determine enviro sensor IDs and replicate across cages
    if not enviro_df.empty:
        # Get unique enviro sensor IDs from column suffixes
        enviro_suffixes = sorted(set(
            int(c.rsplit('_', 1)[1])
            for c in enviro_cols
            if '_' in c and c.rsplit('_', 1)[1].isdigit()
        ))

        # Compute how many cages per chamber
        cages_per_chamber = len(subj_ids) // len(enviro_suffixes) if enviro_suffixes else 1

        # Assign sensors evenly: sensor IDs spaced by cages_per_chamber
        sensor_start_ids = list(range(
            min(subj_ids),
            max(subj_ids) + 1,
            cages_per_chamber
        ))

        # Build replicated enviro DataFrame
        replicated_enviro_parts = []
        for i, sensor_start in enumerate(sensor_start_ids):
            # Get the original enviro sensor columns for this chamber
            original_sensor_id = enviro_suffixes[i] if i < len(enviro_suffixes) else enviro_suffixes[-1]
            chamber_enviro_cols = [c for c in enviro_cols
                                  if c.endswith(f'_{original_sensor_id}')]

            # Determine which cages belong to this chamber
            if i < len(sensor_start_ids) - 1:
                chamber_cages = [sid for sid in subj_ids
                                 if sid >= sensor_start and sid < sensor_start_ids[i + 1]]
            else:
                chamber_cages = [sid for sid in subj_ids if sid >= sensor_start]

            # Replicate enviro data for each cage in this chamber
            for cage_id in chamber_cages:
                for col in chamber_enviro_cols:
                    # Rename: enviroX_originalSensor -> enviroX_cageId
                    base_name = col.rsplit('_', 1)[0]
                    new_col_name = f'{base_name}_{cage_id}'
                    replicated_enviro_parts.append((new_col_name, enviro_df[col].values))

        # Build replicated enviro DataFrame and join back
        if replicated_enviro_parts:
            replicated_enviro = pd.DataFrame(
                dict(replicated_enviro_parts),
                index=df.index
            )
            df = pd.concat([df, replicated_enviro], axis=1)

    # Step 3: Reshape from wide to long
    # Save the date column
    date_col_name = [c for c in df.columns if c.startswith('Date_Time')][0]
    date_values = df[date_col_name].copy()

    # Get variable stubs (column names without the _N suffix)
    all_cols_with_suffix = [c for c in df.columns
                            if '_' in c and c.rsplit('_', 1)[1].isdigit()
                            and not c.startswith('Date_Time')]
    var_stubs = sorted(set(c.rsplit('_', 1)[1 - 1] for c in all_cols_with_suffix
                           if c.rsplit('_', 1)[0]))
    # Deduplicate properly
    var_stubs = sorted(set(c.rsplit('_', 1)[0] for c in all_cols_with_suffix))

    # Melt: for each cage, extract all variable values
    long_rows = []
    for cage_id in subj_ids:
        cage_str = str(cage_id)
        row_data = {'Date_Time': date_values.values, 'cage': cage_str}

        for stub in var_stubs:
            col_name = f'{stub}_{cage_id}'
            if col_name in df.columns:
                row_data[stub] = df[col_name].values
            else:
                row_data[stub] = np.nan

        cage_df = pd.DataFrame(row_data)
        long_rows.append(cage_df)

    sable_long = pd.concat(long_rows, ignore_index=True)

    # Step 4: Parse Date_Time and create time bins
    sable_long['Date_Time'] = _parse_datetime(sable_long['Date_Time'])

    sable_long['minute'] = sable_long['Date_Time'].dt.floor('min')
    sable_long['hour'] = sable_long['Date_Time'].dt.floor('h')
    sable_long['day'] = sable_long['Date_Time'].dt.floor('D')

    min_minute = sable_long['minute'].min()
    sable_long['exp.minute'] = (
        sable_long['minute'] - min_minute
    ).dt.total_seconds() / 60

    min_hour = sable_long['hour'].min()
    sable_long['exp.hour'] = (
        sable_long['hour'] - min_hour
    ).dt.total_seconds() / 3600

    min_day = sable_long['day'].min()
    sable_long['exp.day'] = (
        sable_long['day'] - min_day
    ).dt.total_seconds() / 86400

    # Step 5: Build standard CalR output
    sable_df = pd.DataFrame({
        'subject.id': sable_long['cage'],
        'subject.mass': np.nan,
        'cage': sable_long['cage'],
        'Date.Time': sable_long['Date_Time'],
        'vo2': pd.to_numeric(sable_long.get('vo2', np.nan), errors='coerce') * 60,
        'vco2': pd.to_numeric(sable_long.get('vco2', np.nan), errors='coerce') * 60,
        'ee': pd.to_numeric(sable_long.get('kcal_hr', np.nan), errors='coerce'),
        'rer': pd.to_numeric(sable_long.get('rq', np.nan), errors='coerce'),
        'feed': np.nan,
        'feed.acc': pd.to_numeric(sable_long.get('foodupa', np.nan), errors='coerce'),
        'drink': np.nan,
        'drink.acc': np.nan,
        'xytot': (
            pd.to_numeric(sable_long.get('xbreak', np.nan), errors='coerce') +
            pd.to_numeric(sable_long.get('ybreak', np.nan), errors='coerce')
        ),
        'xyamb': np.nan,
        'wheel': np.nan,
        'wheel.acc': np.nan,
        'pedmeter': pd.to_numeric(sable_long.get('pedmeters', np.nan), errors='coerce'),
        'allmeter': pd.to_numeric(sable_long.get('allmeters', np.nan), errors='coerce'),
        'body.temp': np.nan,
        'C13': np.nan,
        'minute': sable_long['minute'],
        'hour': sable_long['hour'],
        'day': sable_long['day'],
        'exp.minute': sable_long['exp.minute'],
        'exp.hour': sable_long['exp.hour'],
        'exp.day': sable_long['exp.day'],
    })

    # ee.acc per cage
    sable_df['ee.acc'] = sable_df.groupby('cage')['ee'].cumsum()

    # Enviro columns
    if 'envirolightlux' in sable_long.columns:
        sable_df['enviro.light'] = sable_long['envirolightlux']
    if 'envirotemp' in sable_long.columns:
        sable_df['enviro.temp'] = sable_long['envirotemp']
    if 'envirosound' in sable_long.columns:
        sable_df['enviro.sound'] = sable_long['envirosound']

    # Optional columns
    if 'bodymass' in sable_long.columns:
        sable_df['subject.mass'] = pd.to_numeric(
            sable_long['bodymass'], errors='coerce'
        )
    if 'bodytemp' in sable_long.columns:
        sable_df['body.temp'] = pd.to_numeric(
            sable_long['bodytemp'], errors='coerce'
        )
    if 'waterupa' in sable_long.columns:
        sable_df['drink.acc'] = pd.to_numeric(
            sable_long['waterupa'], errors='coerce'
        )
    if 'wheelmeters' in sable_long.columns:
        sable_df['wheel.acc'] = pd.to_numeric(
            sable_long['wheelmeters'], errors='coerce'
        )
    if 'si13c' in sable_long.columns:
        sable_df['C13'] = pd.to_numeric(
            sable_long['si13c'], errors='coerce'
        )

    # Step 6: Compute feed/drink/wheel from accumulated values per subject
    def compute_increments(group):
        """Compute incremental values from accumulated, matching R's hrlyfeed."""
        g = group.copy()

        # feed: first value = feed.acc[0], rest = diff of feed.acc
        if 'feed.acc' in g.columns and g['feed.acc'].notna().any():
            g['feed'] = g['feed.acc'].diff()
            g.iloc[0, g.columns.get_loc('feed')] = g['feed.acc'].iloc[0]

        # drink: first value = drink.acc[0], rest = diff of drink.acc
        if 'drink.acc' in g.columns and g['drink.acc'].notna().any():
            g['drink'] = g['drink.acc'].diff()
            g.iloc[0, g.columns.get_loc('drink')] = g['drink.acc'].iloc[0]

        # wheel: first value = wheel.acc[0], rest = diff of wheel.acc
        if 'wheel.acc' in g.columns and g['wheel.acc'].notna().all():
            g['wheel'] = g['wheel.acc'].diff()
            g.iloc[0, g.columns.get_loc('wheel')] = g['wheel.acc'].iloc[0]

        return g

    sable_df = sable_df.groupby('subject.id', group_keys=False).apply(
        compute_increments
    ).reset_index(drop=True)

    # Make subject.id a categorical/factor (matching R)
    sable_df['subject.id'] = sable_df['subject.id'].astype(str)

    return sable_df


def _parse_datetime(datetime_series: pd.Series) -> pd.Series:
    """
    Parse datetime strings, handling US vs international format.

    Matches R's AsDateTime logic.
    """
    try:
        parsed = pd.to_datetime(datetime_series, format='mixed',
                                dayfirst=False, errors='coerce')

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
