#!/usr/bin/env python3
import argparse
import pandas as pd
import os
import boto3
from urllib.parse import urlparse
from liftover import get_lifter
import tempfile
import time

def parse_args():
    parser = argparse.ArgumentParser(description='Lift GWAS data from hg38 to hg19')
    parser.add_argument('--input', required=True, help='Input S3 path (s3://bucket/path)')
    parser.add_argument('--chr-col', default='CHR', help='Chromosome column name')
    parser.add_argument('--pos-col', default='POS', help='Position column name')
    parser.add_argument('--delimiter', default='\t', help='Delimiter in input file (tab is default)')
    return parser.parse_args()

def parse_s3_path(s3_path):
    parsed = urlparse(s3_path)
    if parsed.scheme != 's3':
        raise ValueError(f"Invalid S3 path: {s3_path}. Must start with s3://")
    bucket = parsed.netloc
    key = parsed.path.lstrip('/')
    filename = os.path.basename(key)
    return bucket, key, filename

def download_from_s3(s3_path, local_path):
    print(f"Downloading {s3_path} to {local_path}")
    bucket, key, filename = parse_s3_path(s3_path)
    s3_client = boto3.client('s3')
    local_path = f"{local_path}/to_convert_{filename}"
    s3_client.download_file(bucket, key, local_path)
    print("Download complete")
    return local_path, filename

def upload_to_s3(local_path, s3_path):
    print(f"Uploading {local_path} to {s3_path}")
    bucket, key, filename = parse_s3_path(s3_path)
    s3_client = boto3.client('s3')
    s3_client.upload_file(local_path, bucket, key)
    print("Upload complete")

def main():
    start_time = time.time()
    args = parse_args()

    temp_dir = tempfile.mkdtemp()

    file_to_convert, original_file = download_from_s3(args.input, temp_dir)

    print("Initializing liftover converter...")
    converter = get_lifter('hg38', 'hg19', one_based=True)
    print("Converter initialized")

    total_processed = 0
    total_failed = 0
    first_chunk = True
    chunk_size = 100000
    print(f"Processing in chunks of {chunk_size} rows with delimiter: {repr(args.delimiter)}")

    for chunk_idx, chunk in enumerate(pd.read_csv(file_to_convert,
                                                  sep=args.delimiter,
                                                  chunksize=chunk_size,
                                                  low_memory=False)):
        chunk_start = time.time()
        print(f"Processing chunk {chunk_idx+1} with {len(chunk)} rows")

        chunk[args.chr_col] = chunk[args.chr_col].astype(str)
        hg38_column = f"{args.pos_col}_hg38"
        chunk.loc[:, hg38_column] = chunk[args.pos_col].copy()
        chunk.loc[:, args.pos_col] = None

        chunk_failed = 0

        for idx, row in chunk.iterrows():
            try:
                chr_value = row[args.chr_col]
                pos_value = int(row[hg38_column])

                new_pos = converter[chr_value][pos_value]

                if new_pos:
                    chunk.at[idx, args.pos_col] = new_pos[0][1]
                else:
                    chunk_failed += 1
            except Exception as e:
                print(f"Error processing row {idx}: {e}")
                chunk_failed += 1



        total_failed += chunk_failed
        total_processed += len(chunk)

        if first_chunk:
            chunk.to_csv(original_file, sep=args.delimiter, index=False, mode='w')
            first_chunk = False
        else:
            chunk.to_csv(original_file, sep=args.delimiter, index=False, mode='a', header=False)

        chunk_time = time.time() - chunk_start
        print(f"Chunk {chunk_idx+1}: Processed {len(chunk)} rows in {chunk_time:.2f} seconds. Failed liftovers: {chunk_failed}")

    print(f"Liftover complete. Failed to lift {total_failed} out of {total_processed} variants ({(total_failed/total_processed)*100:.2f}%).")

    upload_to_s3(original_file, args.input)

    total_time = time.time() - start_time
    print(f"Process completed successfully in {total_time:.2f} seconds")

if __name__ == "__main__":
    main()
