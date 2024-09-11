import gzip

import boto3
import csv
from io import TextIOWrapper

def validate_chromosome(chromosome):
    if not chromosome:
        return False
    try:
        chromosome_num = int(chromosome)
        return 1 <= chromosome_num <= 26
    except ValueError:
        return False

def validate_position(position):
    if not position:
        return False
    try:
        position_num = int(position)
        return 0 <= position_num
    except ValueError:
        return False

def validate_zero_to_one(val):
    if not val:
        return False
    try:
        val_num = float(val)
        return 0 <= val_num <= 1
    except ValueError:
        return False

def validate_numeric_and_positive(val):
    if not val:
        return False
    try:
        val_num = float(val)
        return val_num > 0
    except ValueError:
        return False

def validate_numeric(val):
    if not val:
        return False
    try:
        float(val)
        return True
    except ValueError:
        return False

VALIDATORS = [
    {
        "name": "chromosome",
        "error": "Chromosome data should be coded 1-26, where X->23, Y->24, X/Y(PAR)->25, and MT->26. No missing values are allowed. Please recode or remove missing values as appropriate.",
        "validator": validate_chromosome
    },
    {
        "name": "position",
        "error": "Base position data should be positive integers. No missing values are allowed. Please recode or remove missing values as appropriate.",
        "validator": validate_position
    },
    {
        "name": "pValue",
        "error": "pValue data should be numeric between 0 and 1 inclusive. Zero values should be recoded as minimum machine precision if appropriate. Please recode or remove missing values as appropriate.",
        "validator": validate_zero_to_one
    },
    {
        "name": "eaf",
        "alt_name": "maf",
        "error": "Allele frequency data should be numeric between 0 and 1 exclusive. Please recode or remove missing values as appropriate.",
        "validator": validate_zero_to_one
    },
    {
        "name": "beta",
        "error": "Beta / effect estimate data should be numeric. No missing values are allowed. Please recode or remove missing values as appropriate.",
        "validator": validate_numeric
    },
    {
        "name": "se",
        "error": "Standard error (for beta) data should be numeric and positive. No missing values are allowed. Please recode or remove missing values as appropriate.",
        "validator": validate_numeric_and_positive
    }
]



def split_s3_path(s3_path):
    path_without_prefix = s3_path[5:]
    bucket_end_index = path_without_prefix.find('/')
    bucket_name = path_without_prefix[:bucket_end_index]
    s3_key = path_without_prefix[bucket_end_index + 1:]
    return bucket_name, s3_key


def validate_row(row, schema, errors, active_validators):
    to_remove = []
    for val in active_validators:
        if val['error'] in errors:
            continue
        if schema.get(val['name']) is None:
            col_name = schema.get(val['alt_name'])
        else:
            col_name = schema.get(val['name'])
        if not val['validator'](row.get(col_name)):
            errors.add(val['error'])
            to_remove.append(val)
    for val in to_remove:
        active_validators.remove(val)
    return len(active_validators) == 0

async def validate_file(s3_path: str, schema: dict) -> list:
    s3_client = boto3.client('s3')
    bucket, key = split_s3_path(s3_path)
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    errors = set()
    active_validators = VALIDATORS.copy()
    if key.endswith('.gz'):
        gzipfile = gzip.GzipFile(fileobj=obj['Body'], mode='rb')
        text_file = TextIOWrapper(gzipfile, encoding='utf-8')  # Decoding bytes to text
        reader = csv.DictReader(text_file)
        for row in reader:
            if validate_row(row, schema, errors, active_validators):
                break
        text_file.close()
    else:
        text_file = TextIOWrapper(obj['Body'], encoding='utf-8')
        reader = csv.DictReader(text_file)
        for row in reader:
            if validate_row(row, schema, errors, active_validators):
                break
    text_file.close()
    return list(errors)
