import boto3
import pytest
from fastapi.testclient import TestClient
from moto import mock_aws
from sqlalchemy import text

from dataregistry.server import app
from dataregistry.api.calr import get_calr_user
from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api.model import User

CALR_USER = User(user_name='calruser', roles=[])
OTHER_USER = User(user_name='otheruser', roles=[])


def set_up_moto_bucket():
    # Bucket must exist in Moto's virtual AWS account before uploads
    boto3.resource("s3", region_name="us-east-1").create_bucket(Bucket="dig-data-registry")


@pytest.fixture(autouse=True)
def calr_setup(api_client):
    """Start from empty calr tables and authenticate as CALR_USER by default."""
    engine = DataRegistryReadWriteDB().get_engine()
    with engine.connect() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 0"))
        conn.execute(text("TRUNCATE TABLE calr_files"))
        conn.execute(text("TRUNCATE TABLE calr_submissions"))
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 1"))
        conn.commit()
    app.dependency_overrides[get_calr_user] = lambda: CALR_USER
    yield
    app.dependency_overrides.pop(get_calr_user, None)


def _upload_standard(api_client, name="Experiment A", filename="original.csv", body=b"col1,col2\n1,2\n"):
    resp = api_client.post(
        "/api/calr/files",
        data={"name": name},
        files={"standard_file": (filename, body, "text/csv")},
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


@mock_aws
def test_replace_standard_file_updates_in_place(api_client: TestClient):
    set_up_moto_bucket()

    created = _upload_standard(api_client)
    submission_id = created["submission_id"]
    original_file_id = created["file_id"]

    resp = api_client.put(
        f"/api/calr/files/{submission_id}",
        files={"standard_file": ("replacement.csv", b"col1,col2\n3,4\n", "text/csv")},
    )
    assert resp.status_code == 200, resp.text

    # The reported bug: replacing must NOT create a second submission/file.
    listing = api_client.get("/api/calr/files").json()
    assert len(listing) == 1
    sub = listing[0]
    assert sub["id"] == submission_id

    standard_files = [f for f in sub["files"] if f["file_type"] == "standard"]
    assert len(standard_files) == 1
    assert standard_files[0]["id"] == original_file_id  # same row, updated in place
    assert standard_files[0]["file_name"] == "replacement.csv"


@mock_aws
def test_replace_unknown_submission_returns_404(api_client: TestClient):
    set_up_moto_bucket()
    resp = api_client.put(
        "/api/calr/files/doesnotexist",
        files={"standard_file": ("replacement.csv", b"a,b\n1,2\n", "text/csv")},
    )
    assert resp.status_code == 404


@mock_aws
def test_replace_other_users_submission_returns_403(api_client: TestClient):
    set_up_moto_bucket()
    created = _upload_standard(api_client)
    submission_id = created["submission_id"]

    # Authenticate as a different user and attempt to replace
    app.dependency_overrides[get_calr_user] = lambda: OTHER_USER
    resp = api_client.put(
        f"/api/calr/files/{submission_id}",
        files={"standard_file": ("replacement.csv", b"a,b\n9,9\n", "text/csv")},
    )
    assert resp.status_code == 403

    # The original file must be untouched
    app.dependency_overrides[get_calr_user] = lambda: CALR_USER
    sub = api_client.get("/api/calr/files").json()[0]
    standard = next(f for f in sub["files"] if f["file_type"] == "standard")
    assert standard["file_name"] == "original.csv"
