from boto3.session import Session
import json
import sqlalchemy


class DataRegistryDB:
    def __init__(self):
        self.secret_id = 'data-registry'
        self.region = 'us-east-1'
        self.config = None
        self.url = None
        self.engine = None

    def get_config(self):
        if self.config is None:
            client = Session().client('secretsmanager', region_name=self.region)
            self.config = json.loads(client.get_secret_value(SecretId=self.secret_id)['SecretString'])
        return self.config

    def get_url(self):
        if self.url is None:
            self.config = self.get_config()
            return '{engine}://{username}:{password}@{host}:{port}/{db}'.format(
                engine=self.config['engine'] + ('+pymysql' if self.config['engine'] == 'mysql' else ''),
                username=self.config['username'],
                password=self.config['password'],
                host=self.config['host'],
                port=self.config['port'],
                db=self.config['dbname']
            )

    def get_engine(self):
        if self.engine is None:
            self.engine = sqlalchemy.create_engine(self.get_url())
        return self.engine
