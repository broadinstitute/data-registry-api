from os import environ

import pytest
from alembic import command
from alembic.config import Config
from fastapi.testclient import TestClient
from sqlalchemy import text

# allow for a unit test db and a local dev db if desired
if environ.get('DATA_REGISTRY_TEST_DB_CONNECTION'):
    environ['DATA_REGISTRY_DB_CONNECTION'] = environ['DATA_REGISTRY_TEST_DB_CONNECTION']
else:
    environ['DATA_REGISTRY_DB_CONNECTION'] = 'mysql+pymysql://dataregistry:dataregistry@127.0.0.1:3307/dataregistry'

from dataregistry.api.db import DataRegistryReadWriteDB

from dataregistry.server import app

client = TestClient(app)

db = DataRegistryReadWriteDB()


def pytest_sessionstart(session):
    """
    run db migrations before we start tests
    """
    alembic_cfg = Config("./alembic.ini")
    command.upgrade(alembic_cfg, "head")


def before_each_test():
    """
    runs before each test
    """
    with db.get_engine().connect() as con:
        con.execute(text("SET FOREIGN_KEY_CHECKS = 0"))
        con.execute(text("TRUNCATE TABLE studies"))
        con.execute(text("TRUNCATE TABLE datasets"))
        con.execute(text("TRUNCATE TABLE dataset_phenotypes"))
        con.execute(text("TRUNCATE TABLE credible_sets"))
        con.execute(text("TRUNCATE TABLE users"))
        con.execute(text("INSERT INTO users (id, user_name, roles, oauth_provider, created_at) "
                         "values (1, 'testuser@broadinstitute.org', JSON_ARRAY('admin'), 'google', NOW())"))
        con.commit()
        con.execute(text("SET FOREIGN_KEY_CHECKS = 1"))


@pytest.fixture(autouse=True)
def api_client():
    before_each_test()
    return client
