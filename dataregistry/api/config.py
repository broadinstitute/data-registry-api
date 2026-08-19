import json
import os
from functools import lru_cache

from boto3 import Session


def bioindex_base_url() -> str:
    """Server-side bioindex origin. The public bioindex.hugeamp.org sits behind
    a Cloudflare bot challenge that 403s requests from datacenter IPs, so
    server-to-server calls go to the environment's origin host directly.
    Browser-side fetches keep using the public domain."""
    override = os.environ.get('BIOINDEX_BASE_URL')
    if override:
        return override.rstrip('/')
    env = 'prod' if os.environ.get('DATA_REGISTRY_DB_NAME') == 'dataregistry' else 'qa'
    return f'http://bioindex-{env}.hugeampkpnbi.org/main'


@lru_cache
def get_sensitive_config():
    try:
        client = Session().client('secretsmanager', region_name='us-east-1')
        return json.loads(client.get_secret_value(SecretId='data-registry')['SecretString'])
    except Exception as e:
        print(f"Failed to get sensitive config: {str(e)}")
        return None
