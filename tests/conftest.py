from os import environ

import pytest
from alembic import command
from alembic.config import Config
from fastapi.testclient import TestClient
from sqlalchemy import text

if environ.get('DATA_REGISTRY_TEST_DB_CONNECTION'):
    environ['DATA_REGISTRY_DB_CONNECTION'] = environ['DATA_REGISTRY_TEST_DB_CONNECTION']
else:
    environ['DATA_REGISTRY_DB_CONNECTION'] = 'mysql+pymysql://dataregistry:dataregistry@localhost:3307/dataregistry'

from dataregistry.server import app

client = TestClient(app)


def pytest_sessionstart(session):
    """
    run db migrations before we start tests
    """
    alembic_cfg = Config("./alembic.ini")
    command.upgrade(alembic_cfg, "head")


@pytest.fixture(scope="module", autouse=True)
def before_each_test():
    from dataregistry.api.db import SessionLocal
    db = SessionLocal()
    db.execute(text("SET FOREIGN_KEY_CHECKS = 0"))
    db.execute(text("TRUNCATE TABLE datasets"))
    db.execute(text("TRUNCATE TABLE records"))
    db.execute(text("SET FOREIGN_KEY_CHECKS = 1"))


@pytest.fixture
def api_client():
    return client
