"""CalR format loader and retrofit. Port of retrofitCalR.R"""
import pandas as pd
from pathlib import Path
from typing import Union

def load_calr_file(file_path: Union[str, Path]) -> pd.DataFrame:
    """Load existing CalR format file."""
    raise NotImplementedError("CalR loader not yet implemented")

def retrofit_calr(cal_df: pd.DataFrame) -> pd.DataFrame:
    """Retrofit older CalR files to current format. Port of retrofitCalR."""
    raise NotImplementedError("CalR retrofit not yet implemented")
