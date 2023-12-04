import pandas as pd
import numpy as np


def infer_data_type(val):
    if isinstance(val, np.int64):
        return 'int'
    elif isinstance(val, float):
        return 'float'
    else:
        return 'string'


async def parse_file(file_content, file_name) -> pd.DataFrame:
    if file_name.endswith('.csv'):
        return pd.read_csv(file_content)
    elif file_name.endswith('.tsv'):
        return pd.read_csv(file_content, sep='\t')
    else:
        raise ValueError("Unsupported file format")
