import time

import boto3
import uuid

from dataregistry.api import ecs

ecs_client = boto3.client('ecs')

s3_path = "s3://dig-data-registry-qa/csv/sample.tsv"  # Replace with your dynamic value
sort_columns = "VAR_ID"  # Replace with your dynamic value
schema = {"VAR_ID": "TEXT", "Effect_Allele_PH": "TEXT", "EAF_PH": "DECIMAL", "MAF_PH": "DECIMAL", "ODDS_RATIO": "DECIMAL",
          "P_VALUE": "DECIMAL", "Neff": "INTEGER", "chr_ext": "TEXT", "pos_ext": "INTEGER", "ref_ext": "TEXT", "alt_ext": "TEXT"}
ecs.run_ecs_sort_and_convert_job(s3_path, sort_columns, schema, False, str(uuid.uuid4()).replace('-', ''))
