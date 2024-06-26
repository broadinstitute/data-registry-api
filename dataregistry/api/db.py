import os

import sqlalchemy

from dataregistry.api.config import get_sensitive_config


class DataRegistryDB:
    def __init__(self, username_field, password_field):
        self.secret_id = 'data-registry'
        self.region = 'us-east-1'
        self.username_field = username_field
        self.password_field = password_field
        self.config = None
        self.url = None

    def get_url(self):
        if self.url is None:
            if os.getenv('DATA_REGISTRY_DB_CONNECTION'):
                self.url = os.getenv('DATA_REGISTRY_DB_CONNECTION')
            else:
                self.config = get_sensitive_config()
                self.url = '{engine}://{username}:{password}@{host}:{port}/{db}?local_infile=1'.format(
                    engine=self.config['engine'] + ('+pymysql' if self.config['engine'] == 'mysql' else ''),
                    username=self.config[self.username_field],
                    password=self.config[self.password_field],
                    host=self.config['host'],
                    port=self.config['port'],
                    db=os.getenv('DATA_REGISTRY_DB_NAME', 'dataregistry_qa')
                )
        return self.url

    def get_engine(self):
        return sqlalchemy.create_engine(self.get_url(), pool_size=3, pool_pre_ping=True, pool_recycle=7200)


class DataRegistryMigrationDB(DataRegistryDB):
    def __init__(self):
        DataRegistryDB.__init__(self, 'migrationUsername', 'migrationPassword')


class DataRegistryReadWriteDB(DataRegistryDB):
    def __init__(self):
        DataRegistryDB.__init__(self, 'registryUsername', 'registryPassword')
