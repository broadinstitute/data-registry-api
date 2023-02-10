import json

from boto3 import Session


def get_sensitive_config():
    client = Session().client('secretsmanager', region_name='us-east-1')
    return json.loads(client.get_secret_value(SecretId='data-registry')['SecretString'])
