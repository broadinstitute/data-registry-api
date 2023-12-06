import csv

import boto3
import click
import os
import json
import dask.dataframe as dd
import pandas as pd
import re


def download_file_from_s3(s3_path):
    s3 = boto3.client('s3')
    bucket, key = s3_path.replace("s3://", "").split("/", 1)
    remote_file_name = key.split('/')[-1]
    s3.download_file(bucket, key, remote_file_name)
    return remote_file_name


def upload_file_to_s3(file_name, s3_path):
    s3 = boto3.client('s3')
    bucket, key = s3_path.replace("s3://", "").split("/", 1)
    key = re.sub(r'/[^/]*$', '/' + file_name, key)
    s3.upload_file(file_name, bucket, key)


def get_column_names_pandas(csv_file_path):
    df = pd.read_csv(csv_file_path, nrows=0, sep=',' if csv_file_path.endswith('.csv') else '\t')
    return df.columns.tolist()


def sort_file(file_name, columns_to_sort, schema_info):
    columns = get_column_names_pandas(file_name)
    if not set(columns_to_sort).issubset(set(columns)):
        raise Exception(f"Columns to sort {columns_to_sort} not found in file {file_name}")
    to_panda_types = {"TEXT": "str", "INTEGER": "Int64", "DECIMAL": "Float64"}
    schema_info = {k: to_panda_types[v] for k, v in schema_info.items()}
    df = dd.read_csv(file_name, dtype=schema_info, assume_missing=True,
                     sep=',' if file_name.endswith('.csv') else '\t')
    sorted_df = df.sort_values(by=columns_to_sort)
    sorted_file_name = 'sorted_' + file_name
    sorted_df.to_csv(sorted_file_name, single_file=True, index=False, quoting=csv.QUOTE_NONNUMERIC)
    return sorted_file_name


def convert_to_number(s):
    try:
        return int(s)
    except ValueError:
        try:
            return float(s)
        except ValueError:
            return s


def csv_to_jsonl(csv_file_path, jsonl_file_path):
    with open(csv_file_path, 'r', newline='', encoding='utf-8') as csv_file, \
            open(jsonl_file_path, 'w', encoding='utf-8') as jsonl_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            row_dict = {k: convert_to_number(v) for k, v in row.items()}
            jsonl_file.write(json.dumps(row_dict) + '\n')


@click.command()
@click.option('--s3_path', '-s', type=str, required=True)
@click.option('--columns_to_sort', '-c', type=str, required=True)
@click.option('--schema', '-a', type=str, required=True)
@click.option('--already_sorted', '-o', type=bool, required=True)
def main(s3_path, columns_to_sort, schema, already_sorted):
    sorted_file = None
    schema_info = json.loads(schema)
    columns_to_sort = columns_to_sort.split(',')

    local_file = download_file_from_s3(s3_path)
    if not already_sorted:
        print("Sorting file")

    json_file = local_file[:-3] + 'json'
    print("Converting to json")
    csv_to_jsonl(local_file if already_sorted else sorted_file, json_file)

    print("Uploading json to s3")
    upload_file_to_s3(json_file, s3_path)
    print("finished")

    # Clean up local files
    os.remove(local_file)
    if sorted_file:
        os.remove(sorted_file)
    os.remove(json_file)


if __name__ == "__main__":
    main()
