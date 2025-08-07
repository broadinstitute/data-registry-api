import csv
import gzip
import io
from typing import Tuple

import pandas as pd
import numpy as np
from fastapi import UploadFile


def infer_data_type(val):
    if isinstance(val, np.int64):
        return 'INTEGER'
    elif isinstance(val, float):
        return 'DECIMAL'
    else:
        return 'TEXT'

def convert_json_to_csv(json):
    flat_data = {}
    for key, value in json.items():
        if isinstance(value, dict):
            for nested_key, nested_value in value.items():
                flat_data[f"{key}_{nested_key}"] = nested_value
        else:
            flat_data[key] = value

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(flat_data.keys())
    writer.writerow(flat_data.values())
    output.seek(0)
    return output

def convert_multiple_datasets_to_csv(datasets):
    if not datasets:
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(['No datasets found'])
        output.seek(0)
        return output
        
    # Collect all possible keys from all datasets to create headers
    all_keys = set()
    flattened_datasets = []
    
    for dataset in datasets:
        flat_data = {'dataset_name': dataset.dataset_name}
        for key, value in dataset.metadata.items():
            if isinstance(value, dict):
                for nested_key, nested_value in value.items():
                    flat_key = f"{key}_{nested_key}"
                    flat_data[flat_key] = nested_value
                    all_keys.add(flat_key)
            else:
                flat_data[key] = value
                all_keys.add(key)
        all_keys.add('dataset_name')
        flattened_datasets.append(flat_data)
    
    # Sort keys for consistent output
    sorted_keys = ['dataset_name'] + sorted([k for k in all_keys if k != 'dataset_name'])
    
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(sorted_keys)
    
    for flat_data in flattened_datasets:
        row = [flat_data.get(key, '') for key in sorted_keys]
        writer.writerow(row)
    
    output.seek(0)
    return output

async def parse_file(file_content, file_name) -> pd.DataFrame:
    if '.csv' in file_name:
        return pd.read_csv(file_content)
    elif '.tsv' or '.txt' in file_name:
        return pd.read_csv(file_content, sep='\t')
    else:
        raise ValueError("Unsupported file format")


async def is_gzip(stream: bytes) -> bool:
    return stream.startswith(b'\x1f\x8b')


async def decompress_gzip(stream: bytes) -> bytes:
    with gzip.GzipFile(fileobj=io.BytesIO(stream), mode='rb') as gz:
        return gz.read()


async def get_text_sample(file: UploadFile) -> list:
    text_bytes = b""
    while True:
        chunk = await file.read(2048)
        if not chunk:
            break
        text_bytes += chunk

    return await convert_text_bytes_to_list(text_bytes)


async def convert_text_bytes_to_list(text_bytes):
    lines = []
    text_stream = io.StringIO(text_bytes.decode('utf-8'))
    try:
        line = text_stream.readline()
        while line:
            lines.append(line.rstrip('\n'))
            line = text_stream.readline()
    except EOFError:
        pass
    return lines[:-1]


async def get_compressed_sample(file: UploadFile) -> list:
    compressed_bytes = b""
    while True:
        chunk = await file.read(2048)
        if not chunk:
            break
        compressed_bytes += chunk

    return await convert_compressed_bytes_to_list(compressed_bytes)


async def convert_compressed_bytes_to_list(compressed_bytes):
    lines = []
    with gzip.open(io.BytesIO(compressed_bytes), 'rt') as f:
        try:
            line = f.readline()
            while line:
                lines.append(line.rstrip('\n'))
                line = f.readline()
        except EOFError:
            pass
    # last line might not be a full line
    return lines[:-1]
