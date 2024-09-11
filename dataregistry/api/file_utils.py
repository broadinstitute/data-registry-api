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


async def parse_file(file_content, file_name) -> pd.DataFrame:
    if '.csv' in file_name:
        return pd.read_csv(file_content)
    elif '.tsv' in file_name:
        return pd.read_csv(file_content, sep='\t')
    else:
        raise ValueError("Unsupported file format")


async def is_gzip(stream: bytes) -> bool:
    return stream.startswith(b'\x1f\x8b')


async def decompress_gzip(stream: bytes) -> bytes:
    with gzip.GzipFile(fileobj=io.BytesIO(stream), mode='rb') as gz:
        return gz.read()


async def sample_file(lines: list) -> Tuple[io.StringIO, str]:
    sample = ''
    sample_size = min(10, len(lines))
    for line in lines[:sample_size]:
        line_content = line if isinstance(line, str) else line.decode('utf-8')
        if line_content == '\r':
            break
        sample += line_content + '\n'
    file_name = lines[1].decode('utf-8').split(';')[2].split('=')[1].strip().replace("\"", "")
    return io.StringIO(sample), file_name


async def get_text_sample(file: UploadFile) -> list:
    text_bytes = b""
    while True:
        chunk = await file.read(2048)
        if not chunk:
            break
        text_bytes += chunk

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
