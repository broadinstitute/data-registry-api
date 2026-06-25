import boto3
import pytest
from fastapi.testclient import TestClient
from moto import mock_aws
from sqlalchemy import text

from dataregistry.server import app
from dataregistry.api import calr_query
from dataregistry.api.calr import _can_read, get_calr_user, get_calr_user_optional
from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api.model import User

CALR_USER = User(user_name='calruser', roles=[])
OTHER_USER = User(user_name='otheruser', roles=[])

engine = DataRegistryReadWriteDB().get_engine()


def set_up_moto_bucket():
    boto3.resource("s3", region_name="us-east-1").create_bucket(Bucket="dig-data-registry")


@pytest.fixture(autouse=True)
def calr_setup(api_client):
    """Start from empty calr tables and authenticate as CALR_USER by default."""
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
def test_listing_includes_shared_defaults_false(api_client: TestClient):
    set_up_moto_bucket()
    _upload_standard(api_client)
    sub = api_client.get("/api/calr/files").json()[0]
    assert sub["shared"] is False


def test_can_read_public():
    assert _can_read({'public': 1, 'shared': 0, 'uploaded_by': 'a'}, None) is True


def test_can_read_shared():
    assert _can_read({'public': 0, 'shared': 1, 'uploaded_by': 'a'}, None) is True


def test_can_read_owner_match():
    assert _can_read({'public': 0, 'shared': 0, 'uploaded_by': 'a'}, 'a') is True


def test_can_read_denies_anonymous_private():
    assert _can_read({'public': 0, 'shared': 0, 'uploaded_by': 'a'}, None) is False


def test_can_read_denies_other_user():
    assert _can_read({'public': 0, 'shared': 0, 'uploaded_by': 'a'}, 'b') is False


@mock_aws
def test_anonymous_cannot_download_private_file(api_client: TestClient):
    set_up_moto_bucket()
    created = _upload_standard(api_client)
    # download_calr_file uses get_calr_user_optional (not overridden) -> None == anonymous
    resp = api_client.get(f"/api/calr/files/{created['file_id']}")
    assert resp.status_code == 404


@mock_aws
def test_anonymous_can_download_shared_file(api_client: TestClient):
    set_up_moto_bucket()
    created = _upload_standard(api_client)
    calr_query.set_calr_submission_shared(engine, created["submission_id"], True)
    resp = api_client.get(f"/api/calr/files/{created['file_id']}")
    assert resp.status_code == 200
    assert resp.content == b"col1,col2\n1,2\n"


@mock_aws
def test_file_info_404_private_200_shared(api_client: TestClient):
    set_up_moto_bucket()
    created = _upload_standard(api_client)
    assert api_client.get(f"/api/calr/files/{created['file_id']}/info").status_code == 404
    calr_query.set_calr_submission_shared(engine, created["submission_id"], True)
    resp = api_client.get(f"/api/calr/files/{created['file_id']}/info")
    assert resp.status_code == 200
    assert "s3_path" not in resp.json()


@mock_aws
def test_owner_can_set_shared(api_client: TestClient):
    set_up_moto_bucket()
    created = _upload_standard(api_client)
    sid = created["submission_id"]
    resp = api_client.patch(f"/api/calr/files/{sid}?shared=true")
    assert resp.status_code == 200, resp.text
    assert resp.json()["shared"] is True
    assert resp.json()["public"] is False
    sub = api_client.get("/api/calr/files").json()[0]
    assert sub["shared"] is True


@mock_aws
def test_non_owner_cannot_set_shared(api_client: TestClient):
    set_up_moto_bucket()
    created = _upload_standard(api_client)
    sid = created["submission_id"]
    app.dependency_overrides[get_calr_user] = lambda: OTHER_USER
    try:
        resp = api_client.patch(f"/api/calr/files/{sid}?shared=true")
        assert resp.status_code == 403
    finally:
        app.dependency_overrides[get_calr_user] = lambda: CALR_USER


@mock_aws
def test_public_patch_backward_compatible_and_independent(api_client: TestClient):
    set_up_moto_bucket()
    created = _upload_standard(api_client)
    sid = created["submission_id"]
    r1 = api_client.patch(f"/api/calr/files/{sid}?public=true")
    assert r1.status_code == 200
    assert r1.json()["public"] is True
    assert r1.json()["shared"] is False
    r2 = api_client.patch(f"/api/calr/files/{sid}?shared=true")
    assert r2.json()["public"] is True   # unchanged by the shared toggle
    assert r2.json()["shared"] is True


@mock_aws
def test_patch_with_neither_flag_is_noop(api_client: TestClient):
    set_up_moto_bucket()
    created = _upload_standard(api_client)
    sid = created["submission_id"]
    api_client.patch(f"/api/calr/files/{sid}?shared=true")
    resp = api_client.patch(f"/api/calr/files/{sid}")
    assert resp.status_code == 200
    assert resp.json()["shared"] is True
    assert resp.json()["public"] is False


@mock_aws
def test_shared_only_submission_absent_from_public_list(api_client: TestClient):
    set_up_moto_bucket()
    created = _upload_standard(api_client)
    sid = created["submission_id"]
    api_client.patch(f"/api/calr/files/{sid}?shared=true")
    public_list = api_client.get("/api/calr/public").json()
    assert all(s["id"] != sid for s in public_list)


@mock_aws
def test_shared_endpoint_returns_bundle_when_shared(api_client: TestClient):
    set_up_moto_bucket()
    created = _upload_standard(api_client)
    sid = created["submission_id"]
    api_client.patch(f"/api/calr/files/{sid}?shared=true")
    # No auth header; endpoint is unauthenticated
    resp = api_client.get(f"/api/calr/shared/{sid}")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["id"] == sid
    assert body["shared"] is True
    assert body["public"] is False
    assert body["uploaded_by"] == "calruser"
    assert any(f["file_type"] == "standard" for f in body["files"])
    for f in body["files"]:
        assert "s3_path" not in f


@mock_aws
def test_shared_endpoint_404_when_not_shared(api_client: TestClient):
    set_up_moto_bucket()
    created = _upload_standard(api_client)
    resp = api_client.get(f"/api/calr/shared/{created['submission_id']}")
    assert resp.status_code == 404


def test_shared_endpoint_404_unknown_id(api_client: TestClient):
    resp = api_client.get("/api/calr/shared/doesnotexist")
    assert resp.status_code == 404


@mock_aws
def test_owner_can_read_own_private_file(api_client: TestClient):
    set_up_moto_bucket()
    created = _upload_standard(api_client)  # private by default
    app.dependency_overrides[get_calr_user_optional] = lambda: CALR_USER
    try:
        resp = api_client.get(f"/api/calr/files/{created['file_id']}")
        assert resp.status_code == 200, resp.text
        assert resp.content == b"col1,col2\n1,2\n"
    finally:
        app.dependency_overrides.pop(get_calr_user_optional, None)


@mock_aws
def test_patch_can_set_both_flags_false(api_client: TestClient):
    set_up_moto_bucket()
    created = _upload_standard(api_client)
    sid = created["submission_id"]
    api_client.patch(f"/api/calr/files/{sid}?public=true&shared=true")
    resp = api_client.patch(f"/api/calr/files/{sid}?public=false&shared=false")
    assert resp.status_code == 200, resp.text
    assert resp.json()["public"] is False
    assert resp.json()["shared"] is False
