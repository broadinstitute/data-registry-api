import csv

import boto3
import click
import os
import json
import pandas as pd
import sqlite3


def download_file_from_s3(s3_path):
    s3 = boto3.client('s3')
    bucket, key = s3_path.replace("s3://", "").split("/", 1)
    remote_file_name = key.split('/')[-1]
    s3.download_file(bucket, key, remote_file_name)
    return remote_file_name


def upload_file_to_s3(file_name, s3_path, process_id):
    s3 = boto3.client('s3')
    bucket = s3_path.replace("s3://", "").split("/")[0]
    key = "bioindex/" + process_id + "/" + file_name
    s3.upload_file(file_name, bucket, key)


def get_column_names_pandas(csv_file_path):
    df = pd.read_csv(csv_file_path, nrows=0, sep=',' if csv_file_path.endswith('.csv') else '\t')
    return df.columns.tolist()


def sort_file(file_name, columns_to_sort, schema_info):
    conn = sqlite3.connect('temp.db')
    columns = get_column_names_pandas(file_name)
    if not set(columns_to_sort).issubset(set(columns)):
        raise Exception(f"Columns to sort {columns_to_sort} not found in file {file_name}")
    to_panda_types = {"TEXT": "str", "INTEGER": "Int64", "DECIMAL": "Float64"}
    schema_info = {k: to_panda_types[v] for k, v in schema_info.items()}
    for chunk in pd.read_csv(file_name, dtype=schema_info,
                             sep=',' if file_name.endswith('.csv') else '\t', chunksize=10 ** 6):
        chunk.to_sql('data', conn, if_exists='append', index=False)

    sorted_file_name = 'sorted_' + file_name
    with open(sorted_file_name, 'w') as file:
        conn.text_factory = str
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM data LIMIT 0")
        delimiter = ',' if file_name.endswith('.csv') else '\t'
        file.write(delimiter.join([desc[0] for desc in cursor.description]) + '\n')
        quoted_columns = [f'\"{column}\"' for column in columns_to_sort]
        for row in cursor.execute(f"SELECT * FROM data ORDER BY {', '.join(quoted_columns)}"):
            formatted_row = ["" if value is None else str(value) for value in row]
            file.write(delimiter.join(formatted_row) + '\n')
    return sorted_file_name


def convert_to_type(val, col, mapping):
    if val is None or val == '':
        return None
    if mapping[col] == 'TEXT':
        return val
    if mapping[col] == 'INTEGER':
        return int(val)
    if mapping[col] == 'DECIMAL':
        return float(val)


def csv_to_jsonl(csv_file_path, jsonl_file_path, mapping):
    with open(csv_file_path, 'r', newline='', encoding='utf-8') as csv_file, \
            open(jsonl_file_path, 'w', encoding='utf-8') as jsonl_file:
        reader = csv.DictReader(csv_file, delimiter=',' if csv_file_path.endswith('.csv') else '\t')
        for row in reader:
            row_dict = {k: convert_to_type(v, k, mapping) for k, v in row.items()}
            jsonl_file.write(json.dumps(row_dict) + '\n')


@click.command()
@click.option('--s3_path', '-s', type=str, required=True)
@click.option('--columns_to_sort', '-c', type=str, required=True)
@click.option('--schema', '-a', type=str, required=True)
@click.option('--already_sorted', '-o', type=bool, required=True)
@click.option('--process_id', '-p', type=str, required=True)
def main(s3_path, columns_to_sort, schema, already_sorted, process_id):
    sorted_file = None
    schema_info = json.loads(schema)
    columns_to_sort = columns_to_sort.split(',')

    local_file = download_file_from_s3(s3_path)
    if not already_sorted:
        print("Sorting file")
        sorted_file = sort_file(local_file, columns_to_sort, schema_info)

    json_file = local_file[:-3] + 'json'
    print("Converting to json")
    csv_to_jsonl(local_file if already_sorted else sorted_file, json_file, schema_info)

    print("Uploading json to s3")
    upload_file_to_s3(json_file, s3_path, process_id)
    print("finished")

    # Clean up local files
    os.remove(local_file)
    if sorted_file:
        os.remove(sorted_file)
    os.remove(json_file)


if __name__ == "__main__":
    main()
