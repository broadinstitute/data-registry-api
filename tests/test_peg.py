import uuid

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


def test_validate_files_rejects_long_filename(api_client: TestClient):
    _login(PEG_USER)
    try:
        files = helpers.as_multipart(helpers.valid_files())
        name, data, ct = files["peg_list"]
        files["peg_list"] = ("a" * 296 + ".tsv", data, ct)
        resp = api_client.post("/api/peg/validate-files", files=files)
    finally:
        _logout()
    assert resp.status_code == 400


def test_validate_files_requires_auth(api_client: TestClient):
    resp = api_client.post("/api/peg/validate-files", files=helpers.as_multipart(helpers.valid_files()))
    assert resp.status_code == 401


STUDY_BODY = {
    "name": "Toy study",
    "metadata": {
        "study_author": "Toy Author",
        "phenotype": "Toy phenotype",
        "gwas_source": "GCST000001",
        "gwas_source_type": "gwas_catalog",
        "published": "yes",
    },
}


def _create_study(api_client, user=PEG_USER):
    _login(user)
    try:
        resp = api_client.post("/api/peg/studies", json=STUDY_BODY)
    finally:
        _logout()
    assert resp.status_code == 200, resp.text
    return resp.json()["id"]


def _s3_keys():
    s3 = boto3.client("s3", region_name="us-east-1")
    objs = s3.list_objects_v2(Bucket="dig-data-registry").get("Contents", [])
    return sorted(o["Key"] for o in objs)


@mock_aws
def test_store_files_rejects_broken_input_and_stores_nothing(api_client: TestClient):
    _set_up_moto_bucket()
    study_id = _create_study(api_client)
    _login(PEG_USER)
    try:
        files = helpers.broken_matrix_row(helpers.valid_files())
        resp = api_client.post(f"/api/peg/studies/{study_id}/files", files=helpers.as_multipart(files))
    finally:
        _logout()
    assert resp.status_code == 422, resp.text
    body = resp.json()
    assert body["detail"] == "PEG files failed validation"
    assert body["report"]["status"] == "error"
    assert query.get_peg_files(peg.engine, study_id) == []
    assert _s3_keys() == []


@mock_aws
def test_store_files_saves_three_records_and_objects(api_client: TestClient):
    _set_up_moto_bucket()
    study_id = _create_study(api_client)
    _login(PEG_USER)
    try:
        resp = api_client.post(f"/api/peg/studies/{study_id}/files", files=helpers.as_multipart(helpers.valid_files()))
    finally:
        _logout()
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["report"]["status"] == "success"
    assert sorted(f["file_type"] for f in body["files"]) == ["peg_list", "peg_matrix", "peg_metadata"]

    rows = query.get_peg_files(peg.engine, study_id)
    assert sorted(r["file_type"] for r in rows) == ["peg_list", "peg_matrix", "peg_metadata"]
    dashed = str(uuid.UUID(study_id))
    assert _s3_keys() == sorted([
        f"peg/{dashed}/peg_list/{helpers.LIST_NAME}",
        f"peg/{dashed}/peg_matrix/{helpers.MATRIX_NAME}",
        f"peg/{dashed}/peg_metadata/{helpers.METADATA_NAME}",
    ])

    # body["files"] should match GET /peg/studies/{id}/files: dash-free study_id, uploaded_at present.
    dashfree = dashed.replace('-', '')
    assert sorted(f["id"] for f in body["files"]) == sorted(r["id"] for r in rows)
    assert all(f["study_id"] == dashfree for f in body["files"])
    assert all(f["uploaded_at"] for f in body["files"])


@mock_aws
def test_store_files_replaces_previous_upload(api_client: TestClient):
    _set_up_moto_bucket()
    study_id = _create_study(api_client)
    _login(PEG_USER)
    try:
        first = api_client.post(f"/api/peg/studies/{study_id}/files", files=helpers.as_multipart(helpers.valid_files()))
        assert first.status_code == 200, first.text
        renamed = helpers.valid_files()
        renamed["list"] = ("list_v2.tsv", renamed["list"][1])
        second = api_client.post(f"/api/peg/studies/{study_id}/files", files=helpers.as_multipart(renamed))
    finally:
        _logout()
    assert second.status_code == 200, second.text
    rows = query.get_peg_files(peg.engine, study_id)
    assert len(rows) == 3
    list_row = next(r for r in rows if r["file_type"] == "peg_list")
    assert list_row["file_name"] == "list_v2.tsv"
    dashed = str(uuid.UUID(study_id))
    assert f"peg/{dashed}/peg_list/{helpers.LIST_NAME}" not in _s3_keys()


@mock_aws
def test_store_files_forbidden_for_non_owner(api_client: TestClient):
    _set_up_moto_bucket()
    study_id = _create_study(api_client)
    _login(OTHER_USER)
    try:
        resp = api_client.post(f"/api/peg/studies/{study_id}/files", files=helpers.as_multipart(helpers.valid_files()))
    finally:
        _logout()
    assert resp.status_code == 403


@mock_aws
def test_store_files_allowed_for_reviewer(api_client: TestClient):
    _set_up_moto_bucket()
    study_id = _create_study(api_client)
    _login(REVIEWER)
    try:
        resp = api_client.post(f"/api/peg/studies/{study_id}/files", files=helpers.as_multipart(helpers.valid_files()))
    finally:
        _logout()
    assert resp.status_code == 200, resp.text


def test_store_files_unknown_study_404(api_client: TestClient):
    _login(PEG_USER)
    try:
        resp = api_client.post(
            "/api/peg/studies/00000000-0000-0000-0000-000000000000/files",
            files=helpers.as_multipart(helpers.valid_files()),
        )
    finally:
        _logout()
    assert resp.status_code == 404


def test_per_file_upload_endpoints_are_gone(api_client: TestClient):
    _login(PEG_USER)
    try:
        study_id = _create_study(api_client)
        _login(PEG_USER)
        for suffix in ("peg-list", "peg-matrix", "peg-metadata"):
            resp = api_client.post(f"/api/peg/studies/{study_id}/{suffix}", files={"file": ("x.tsv", b"a\tb\n", "text/plain")})
            assert resp.status_code in (404, 405), suffix
    finally:
        _logout()


def _create_study_with_published(api_client, published, name):
    body = {**STUDY_BODY, "name": name, "metadata": {**STUDY_BODY["metadata"], "published": published}}
    _login(PEG_USER)
    try:
        resp = api_client.post("/api/peg/studies", json=body)
    finally:
        _logout()
    assert resp.status_code == 200, resp.text
    return resp.json()["id"]


def test_public_studies_lists_only_published_without_auth(api_client: TestClient):
    published_id = _create_study_with_published(api_client, "published", "Published study")
    _create_study_with_published(api_client, "pre-published", "Preprint study")
    _create_study_with_published(api_client, "unpublished", "Unpublished study")

    resp = api_client.get("/api/peg/public/studies")

    assert resp.status_code == 200, resp.text
    studies = resp.json()
    assert [s["id"] for s in studies] == [published_id]
    assert studies[0]["name"] == "Published study"
    assert studies[0]["accession_id"].startswith("PEGSt")
    assert studies[0]["metadata"]["published"] == "published"


def test_public_studies_omits_submitter_identity(api_client: TestClient):
    _create_study_with_published(api_client, "published", "Published study")

    resp = api_client.get("/api/peg/public/studies")

    assert resp.status_code == 200, resp.text
    assert "created_by" not in resp.json()[0]
    assert resp.json()[0]["metadata"]["study_author"] == "Toy Author"


def test_public_studies_returns_empty_list_when_none_published(api_client: TestClient):
    _create_study_with_published(api_client, "unpublished", "Unpublished study")

    resp = api_client.get("/api/peg/public/studies")

    assert resp.status_code == 200, resp.text
    assert resp.json() == []


def _upload_valid_files(api_client, study_id):
    _login(PEG_USER)
    try:
        resp = api_client.post(f"/api/peg/studies/{study_id}/files", files=helpers.as_multipart(helpers.valid_files()))
    finally:
        _logout()
    assert resp.status_code == 200, resp.text
    return resp.json()["files"]


@mock_aws
def test_public_studies_include_file_download_urls(api_client: TestClient):
    _set_up_moto_bucket()
    study_id = _create_study_with_published(api_client, "published", "Published study")
    uploaded = _upload_valid_files(api_client, study_id)

    resp = api_client.get("/api/peg/public/studies")

    assert resp.status_code == 200, resp.text
    files = resp.json()[0]["files"]
    assert sorted(f["file_type"] for f in files) == ["peg_list", "peg_matrix", "peg_metadata"]
    by_type = {f["file_type"]: f for f in files}
    assert by_type["peg_metadata"]["file_name"] == helpers.METADATA_NAME
    assert by_type["peg_metadata"]["file_size"] > 0
    ids_by_type = {f["file_type"]: f["id"] for f in uploaded}
    for file_type, f in by_type.items():
        assert f["download_url"] == f"/api/peg/public/files/{ids_by_type[file_type]}"
        assert "file_path" not in f


def test_public_studies_without_uploads_have_empty_files(api_client: TestClient):
    _create_study_with_published(api_client, "published", "Published study")

    resp = api_client.get("/api/peg/public/studies")

    assert resp.status_code == 200, resp.text
    assert resp.json()[0]["files"] == []


def _public_file_url(api_client, file_type):
    files = api_client.get("/api/peg/public/studies").json()[0]["files"]
    return next(f["download_url"] for f in files if f["file_type"] == file_type)


@mock_aws
def test_public_file_download_streams_file_contents(api_client: TestClient):
    _set_up_moto_bucket()
    study_id = _create_study_with_published(api_client, "published", "Published study")
    _upload_valid_files(api_client, study_id)

    resp = api_client.get(_public_file_url(api_client, "peg_metadata"), follow_redirects=False)

    assert resp.status_code == 200, resp.text
    assert resp.content == helpers.valid_files()["metadata"][1]
    assert resp.headers["content-type"] == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    assert resp.headers["content-length"] == str(len(resp.content))
    assert resp.headers["content-disposition"] == f'attachment; filename="{helpers.METADATA_NAME}"'


@mock_aws
def test_public_file_download_carries_cors_headers_for_portal_origin(api_client: TestClient):
    _set_up_moto_bucket()
    study_id = _create_study_with_published(api_client, "published", "Published study")
    _upload_valid_files(api_client, study_id)

    resp = api_client.get(
        _public_file_url(api_client, "peg_list"),
        headers={"Origin": "https://hugeamp.org"},
        follow_redirects=False,
    )

    assert resp.status_code == 200, resp.text
    assert resp.headers["access-control-allow-origin"] == "https://hugeamp.org"
    assert resp.content == helpers.valid_files()["list"][1]


@mock_aws
def test_public_file_download_404_for_unpublished_study(api_client: TestClient):
    _set_up_moto_bucket()
    study_id = _create_study_with_published(api_client, "unpublished", "Unpublished study")
    uploaded = _upload_valid_files(api_client, study_id)

    resp = api_client.get(f"/api/peg/public/files/{uploaded[0]['id']}", follow_redirects=False)

    assert resp.status_code == 404


def test_public_file_download_404_for_unknown_file(api_client: TestClient):
    resp = api_client.get("/api/peg/public/files/00000000-0000-0000-0000-000000000000", follow_redirects=False)

    assert resp.status_code == 404
