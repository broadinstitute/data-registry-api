"""
File loaders for detecting and parsing calorimetry data formats.

This module contains the main entry point load_cal_file() which
automatically detects the file format and routes to the appropriate
loader function.
"""

import pandas as pd
import re
from pathlib import Path
from typing import Union
from .tse_loader import load_tse_file, convert_tse
from .oxymax_loader import load_oxymax_file, convert_oxymax
from .sable_loader import load_sable_file, convert_sable
from .calr_loader import load_calr_file, retrofit_calr


def detect_format(file_path: Union[str, Path]) -> str:
    """
    Detect the calorimetry file format by examining the first few lines.
    
    Detection logic ported from loadCalFile.R:
    - Oxymax: "Oxymax" in first line (case-insensitive)
    - TSE: "TSE" in first 4 lines
    - Sable: "Date_Time_\\d" pattern in first line  
    - CalR: "cage" in first line (case-insensitive)
    
    Args:
        file_path: Path to the calorimetry file
        
    Returns:
        Format name: 'oxymax', 'tse', 'sable', or 'calr'
        
    Raises:
        ValueError: If format cannot be detected
    """
    file_path = Path(file_path)
    
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        # Read first 5 lines for detection
        lines = [f.readline() for _ in range(5)]
    
    # Check for Oxymax
    if re.search(r'oxymax', lines[0], re.IGNORECASE):
        return 'oxymax'
    
    # Check for TSE in first 4 lines
    for line in lines[:4]:
        if re.search(r'TSE', line):
            return 'tse'
    
    # Check for Sable pattern
    if re.search(r'Date_Time_\d', lines[0]):
        return 'sable'
    
    # Check for CalR format
    if re.search(r'cage', lines[0], re.IGNORECASE):
        return 'calr'
    
    raise ValueError(
        f"Unable to detect format for {file_path}. "
        f"First line: {lines[0][:100]}"
    )


def load_cal_file(file_path: Union[str, Path]) -> pd.DataFrame:
    """
    Load and convert calorimetry file to standard CalR format.
    
    This is the main entry point that:
    1. Detects the file format
    2. Routes to appropriate loader
    3. Converts to standard CalR format
    4. Returns a pandas DataFrame
    
    Args:
        file_path: Path to the calorimetry data file
        
    Returns:
        DataFrame in standard CalR format with columns:
            - subject.id: Subject identifier
            - subject.mass: Subject mass in grams
            - cage: Cage number
            - Date.Time: Timestamp
            - vo2, vco2, ee, rer: Metabolic variables
            - feed, feed.acc, drink, drink.acc: Feeding/drinking
            - xytot, xyamb: Locomotion
            - wheel, wheel.acc: Wheel activity
            - pedmeter, allmeter, body.temp: Other sensors
            - minute, hour, day: Time bins
            - exp.minute, exp.hour, exp.day: Experimental time offsets
            
    Example:
        >>> df = load_cal_file('data/experiment.csv')
        >>> print(df.columns)
        >>> print(df.head())
    """
    file_path = Path(file_path)
    
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    # Detect format
    format_type = detect_format(file_path)
    print(f"> Detected format: {format_type.upper()}")
    
    # Load and convert based on format
    if format_type == 'oxymax':
        print("> Loading Oxymax file(s)...", end='')
        raw_data = load_oxymax_file(file_path)
        print(" done")
        cal_df = convert_oxymax(raw_data)
        
    elif format_type == 'tse':
        print("> Loading TSE file...", end='')
        raw_data = load_tse_file(file_path)
        print(" done")
        cal_df = convert_tse(raw_data)
        
    elif format_type == 'sable':
        print("> Load SABLE file...", end='')
        raw_data = load_sable_file(file_path)
        print(" done")
        cal_df = convert_sable(raw_data)
        
    elif format_type == 'calr':
        print("> Loading CalR file...", end='')
        cal_df = load_calr_file(file_path)
        print(" done")
        
        print("> Retrofitting CalR files...", end='')
        cal_df = retrofit_calr(cal_df)
        print(" done")
    
    return cal_df
