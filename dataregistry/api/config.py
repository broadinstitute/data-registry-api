import json
from functools import lru_cache

from boto3 import Session
from botocore.exceptions import NoCredentialsError


@lru_cache
def get_sensitive_config():
    try:
        client = Session().client('secretsmanager', region_name='us-east-1')
        return json.loads(client.get_secret_value(SecretId='data-registry')['SecretString'])
    except NoCredentialsError as e:
        return None
