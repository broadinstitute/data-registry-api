import json
from functools import lru_cache

from boto3 import Session


@lru_cache
def get_sensitive_config():
    try:
        client = Session().client('secretsmanager', region_name='us-east-1')
        return json.loads(client.get_secret_value(SecretId='data-registry')['SecretString'])
    except Exception as e:
        print(f"Failed to get sensitive config: {str(e)}")
        return None
