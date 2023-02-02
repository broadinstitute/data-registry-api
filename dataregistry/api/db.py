from boto3.session import Session
import json
import sqlalchemy


class DataRegistryDB:
    def __init__(self, username_field, password_field):
        self.secret_id = 'data-registry'
        self.region = 'us-east-1'
        self.username_field = username_field
        self.password_field = password_field
        self.config = None
        self.url = None

    def get_config(self):
        if self.config is None:
            client = Session().client('secretsmanager', region_name=self.region)
            self.config = json.loads(client.get_secret_value(SecretId=self.secret_id)['SecretString'])
        return self.config

    def get_url(self):
        if self.url is None:
            self.config = self.get_config()
            self.url = '{engine}://{username}:{password}@{host}:{port}/{db}'.format(
                engine=self.config['engine'] + ('+pymysql' if self.config['engine'] == 'mysql' else ''),
                username=self.config[self.username_field],
                password=self.config[self.password_field],
                host=self.config['host'],
                port=self.config['port'],
                db=self.config['dbname']
            )
        return self.url

    def get_engine(self):
        return sqlalchemy.create_engine(self.get_url())


class DataRegistryMigrationDB(DataRegistryDB):
    def __init__(self):
        DataRegistryDB.__init__(self, 'migrationUsername', 'migrationPassword')


class DataRegistryReadWriteDB(DataRegistryDB):
    def __init__(self):
        DataRegistryDB.__init__(self, 'registryUsername', 'registryPassword')
