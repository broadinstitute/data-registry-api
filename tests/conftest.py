from os import environ

import pytest
from alembic import command
from alembic.config import Config
from fastapi.testclient import TestClient
from sqlalchemy import text

from dataregistry.api.db import DataRegistryReadWriteDB

# allow for a unit test db and a local dev db if desired
if environ.get('DATA_REGISTRY_TEST_DB_CONNECTION'):
    environ['DATA_REGISTRY_DB_CONNECTION'] = environ['DATA_REGISTRY_TEST_DB_CONNECTION']
else:
    environ['DATA_REGISTRY_DB_CONNECTION'] = 'mysql+pymysql://dataregistry:dataregistry@localhost:3307/dataregistry'

from dataregistry.server import app

client = TestClient(app)

db = DataRegistryReadWriteDB()


def pytest_sessionstart(session):
    """
    run db migrations before we start tests
    """
    if 'localhost' not in db.get_url():
        print("DB url is not pointing to localhost, quiting test suite")
        exit(1)
    alembic_cfg = Config("./alembic.ini")
    command.upgrade(alembic_cfg, "head")


@pytest.fixture(scope="module", autouse=True)
def before_each_test():
    """
    runs before each test
    """
    with db.get_engine().connect() as con:
        con.execute(text("SET FOREIGN_KEY_CHECKS = 0"))
        con.execute(text("TRUNCATE TABLE datasets"))
        con.execute(text("TRUNCATE TABLE records"))
        con.execute(text("SET FOREIGN_KEY_CHECKS = 1"))


@pytest.fixture
def api_client():
    return client
