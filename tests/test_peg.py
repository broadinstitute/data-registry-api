import boto3
from fastapi.testclient import TestClient
from moto import mock_aws

from dataregistry.api import peg
from dataregistry.api import query
from dataregistry.api.model import User
from dataregistry.server import app
from tests.peg_fixtures import helpers


class _FakeResponse:
    def __init__(self, status_code, json_data=None, text=""):
        self.status_code = status_code
        self._json = json_data
        self.text = text

    def json(self):
        if self._json is None:
            raise ValueError("No JSON body")
        return self._json


class _FakeAsyncClient:
    """Stand-in for httpx.AsyncClient as an async context manager.

    Routes POSTs to canned responses keyed by URL suffix and records every
    call so tests can assert which user-service endpoints were hit.
    """

    def __init__(self, responses_by_suffix, calls):
        self._responses_by_suffix = responses_by_suffix
        self._calls = calls

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def post(self, url, data=None, **kwargs):
        self._calls.append((url, data))
        for suffix, resp in self._responses_by_suffix.items():
            if url.endswith(suffix):
                return resp
        raise AssertionError(f"Unexpected POST to {url}")


def test_create_peg_user_adds_existing_user_to_group(api_client: TestClient, monkeypatch):
    """If the user already exists in the user service, self-service signup
    should fall back to add-user-to-group so the existing account is granted
    the peg(-prod) group/roles instead of failing with 'already exists'."""
    monkeypatch.setattr(peg, "PEG_USER_TOKEN", "test-token")

    calls = []
    responses = {
        "/api/auth/create-user/": _FakeResponse(
            400,
            {"error": "Username already exists"},
            '{"error": "Username already exists"}',
        ),
        "/api/auth/add-user-to-group/": _FakeResponse(
            200, {"message": "User added to group"}
        ),
    }
    monkeypatch.setattr(
        peg.httpx, "AsyncClient", lambda *a, **k: _FakeAsyncClient(responses, calls)
    )

    response = api_client.post(
        "/api/peg/create-user",
        json={"user_name": "existing@example.com", "password": "supersecret"},
    )

    assert response.status_code == 201, response.text
    posted = [url for url, _ in calls]
    assert any(p.endswith("/api/auth/add-user-to-group/") for p in posted), (
        f"expected fallback to add-user-to-group, got: {posted}"
    )
    assert response.json()["existing_user"] is True, response.text


def test_create_peg_user_new_user_does_not_call_add_to_group(api_client: TestClient, monkeypatch):
    """A brand-new user is created directly; the add-user-to-group fallback
    must not be invoked when create-user succeeds."""
    monkeypatch.setattr(peg, "PEG_USER_TOKEN", "test-token")

    calls = []
    responses = {
        "/api/auth/create-user/": _FakeResponse(201, {"message": "User created successfully"}),
        "/api/auth/add-user-to-group/": _FakeResponse(200, {"message": "User added to group"}),
    }
    monkeypatch.setattr(
        peg.httpx, "AsyncClient", lambda *a, **k: _FakeAsyncClient(responses, calls)
    )

    response = api_client.post(
        "/api/peg/create-user",
        json={"user_name": "brand-new@example.com", "password": "supersecret"},
    )

    assert response.status_code == 201, response.text
    posted = [url for url, _ in calls]
    assert not any(p.endswith("/api/auth/add-user-to-group/") for p in posted), (
        f"add-user-to-group should not be called for a new user, got: {posted}"
    )
    assert response.json()["existing_user"] is False, response.text


PEG_USER = User(user_name="peguser@example.org", roles=[], permissions=[])
OTHER_USER = User(user_name="other@example.org", roles=[], permissions=[])
REVIEWER = User(user_name="reviewer@example.org", roles=[], permissions=["peg-review-data"])


def _login(user):
    app.dependency_overrides[peg.get_peg_user] = lambda: user


def _logout():
    app.dependency_overrides.pop(peg.get_peg_user, None)


def _set_up_moto_bucket():
    boto3.resource("s3", region_name="us-east-1").create_bucket(Bucket="dig-data-registry")


def test_validate_files_returns_report_for_valid_input(api_client: TestClient):
    _login(PEG_USER)
    try:
        resp = api_client.post("/api/peg/validate-files", files=helpers.as_multipart(helpers.valid_files()))
    finally:
        _logout()
    assert resp.status_code == 200, resp.text
    report = resp.json()
    assert report["status"] == "success"
    assert report["files"]["list"]["path"] == helpers.LIST_NAME


def test_validate_files_returns_200_with_error_report_for_broken_input(api_client: TestClient):
    _login(PEG_USER)
    try:
        files = helpers.broken_list_header(helpers.valid_files())
        resp = api_client.post("/api/peg/validate-files", files=helpers.as_multipart(files))
    finally:
        _logout()
    assert resp.status_code == 200, resp.text
    assert resp.json()["status"] == "error"


def test_validate_files_requires_all_three(api_client: TestClient):
    _login(PEG_USER)
    try:
        files = helpers.as_multipart(helpers.valid_files())
        files.pop("peg_metadata")
        resp = api_client.post("/api/peg/validate-files", files=files)
    finally:
        _logout()
    assert resp.status_code == 422


def test_validate_files_rejects_bad_filename(api_client: TestClient):
    _login(PEG_USER)
    try:
        files = helpers.as_multipart(helpers.valid_files())
        name, data, ct = files["peg_list"]
        files["peg_list"] = ("../evil.tsv", data, ct)
        resp = api_client.post("/api/peg/validate-files", files=files)
    finally:
        _logout()
    assert resp.status_code == 400


def test_validate_files_requires_auth(api_client: TestClient):
    resp = api_client.post("/api/peg/validate-files", files=helpers.as_multipart(helpers.valid_files()))
    assert resp.status_code == 401
