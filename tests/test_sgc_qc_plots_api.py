import uuid
from unittest import mock

from fastapi.testclient import TestClient

from dataregistry.api.model import User
from dataregistry.api.sgc import get_sgc_user
from dataregistry.server import app


def _reviewer_user():
    return User(
        id="1",
        user_name="reviewer",
        email="reviewer@example.com",
        roles=["sgc-reviewer"],
        permissions=["sgc-review-data"],
    )


def _qc_row(file_id, *, status="SUCCEEDED", lambda_gc=1.04):
    return {
        "id": uuid.uuid4().hex, "file_id": file_id, "status": status,
        "lambda_gc": lambda_gc, "n_variants": 12_345_678,
        "n_sig_5e8": 12, "n_sig_1e5": 234,
        "manhattan_s3_key": f"sgc/qc/plots/{file_id}/manhattan.png",
        "qq_s3_key": f"sgc/qc/plots/{file_id}/qq.png",
        "dataset": "GEL_batch", "phenotype": "ACTINIC_KER", "ancestry": "EUR",
        "cohort_id": "EF" * 16, "file_name": "ACTINIC_KER-multi_all.regenie.gz",
        "batch_job_id": "job-1", "error_message": None,
        "created_at": "2026-05-24 12:00:00", "updated_at": "2026-05-24 12:10:00",
    }


def test_list_qc_plots_returns_rows(api_client: TestClient):
    rows = [_qc_row(uuid.uuid4().hex)]
    app.dependency_overrides[get_sgc_user] = _reviewer_user
    try:
        with mock.patch("dataregistry.api.sgc.query.get_sgc_plot_results", return_value=rows), \
             mock.patch("dataregistry.api.sgc.check_review_permissions", return_value=True):
            r = api_client.get("/api/sgc/qc/plots", headers={"Authorization": "Bearer test"})
    finally:
        app.dependency_overrides.pop(get_sgc_user, None)
    assert r.status_code == 200
    body = r.json()
    assert len(body) == 1
    assert body[0]["lambda_gc"] == 1.04


def test_manhattan_redirect_returns_presigned(api_client: TestClient):
    file_id = uuid.uuid4().hex
    app.dependency_overrides[get_sgc_user] = _reviewer_user
    try:
        with mock.patch("dataregistry.api.sgc.query.get_sgc_plot_results",
                        return_value=[_qc_row(file_id)]), \
             mock.patch("dataregistry.api.sgc.check_review_permissions", return_value=True), \
             mock.patch("dataregistry.api.sgc.boto3.client") as bc:
            bc.return_value.generate_presigned_url.return_value = "https://example.com/signed"
            r = api_client.get(f"/api/sgc/qc/plots/{file_id}/manhattan",
                               headers={"Authorization": "Bearer test"},
                               follow_redirects=False)
    finally:
        app.dependency_overrides.pop(get_sgc_user, None)
    assert r.status_code == 307
    assert "example.com" in r.headers["location"]


def test_qc_endpoints_require_review_permission(api_client: TestClient):
    app.dependency_overrides[get_sgc_user] = _reviewer_user
    try:
        with mock.patch("dataregistry.api.sgc.check_review_permissions", return_value=False):
            r = api_client.get("/api/sgc/qc/plots", headers={"Authorization": "Bearer test"})
    finally:
        app.dependency_overrides.pop(get_sgc_user, None)
    assert r.status_code == 403


def test_qc_plot_404_when_file_id_unknown(api_client: TestClient):
    app.dependency_overrides[get_sgc_user] = _reviewer_user
    try:
        with mock.patch("dataregistry.api.sgc.query.get_sgc_plot_results", return_value=[]), \
             mock.patch("dataregistry.api.sgc.check_review_permissions", return_value=True):
            r = api_client.get("/api/sgc/qc/plots/deadbeefdeadbeefdeadbeefdeadbeef/manhattan",
                               headers={"Authorization": "Bearer test"},
                               follow_redirects=False)
    finally:
        app.dependency_overrides.pop(get_sgc_user, None)
    assert r.status_code == 404


def test_qq_redirect_returns_presigned(api_client: TestClient):
    file_id = uuid.uuid4().hex
    app.dependency_overrides[get_sgc_user] = _reviewer_user
    try:
        with mock.patch("dataregistry.api.sgc.query.get_sgc_plot_results",
                        return_value=[_qc_row(file_id)]), \
             mock.patch("dataregistry.api.sgc.check_review_permissions", return_value=True), \
             mock.patch("dataregistry.api.sgc.boto3.client") as bc:
            bc.return_value.generate_presigned_url.return_value = "https://example.com/qq"
            r = api_client.get(f"/api/sgc/qc/plots/{file_id}/qq",
                               headers={"Authorization": "Bearer test"},
                               follow_redirects=False)
    finally:
        app.dependency_overrides.pop(get_sgc_user, None)
    assert r.status_code == 307
    assert "example.com/qq" in r.headers["location"]


def test_qc_json_returns_inline_body(api_client: TestClient):
    file_id = uuid.uuid4().hex
    fake_body = b'{"file_id": "abc", "lambda_gc": 1.04}'
    app.dependency_overrides[get_sgc_user] = _reviewer_user
    try:
        with mock.patch("dataregistry.api.sgc.query.get_sgc_plot_results",
                        return_value=[_qc_row(file_id)]), \
             mock.patch("dataregistry.api.sgc.check_review_permissions", return_value=True), \
             mock.patch("dataregistry.api.sgc.boto3.client") as bc:
            bc.return_value.get_object.return_value = {
                "Body": mock.MagicMock(read=lambda: fake_body)
            }
            r = api_client.get(f"/api/sgc/qc/plots/{file_id}/json",
                               headers={"Authorization": "Bearer test"})
    finally:
        app.dependency_overrides.pop(get_sgc_user, None)
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("application/json")
    assert r.content == fake_body


def test_qc_json_404_when_s3_object_missing(api_client: TestClient):
    from botocore.exceptions import ClientError
    file_id = uuid.uuid4().hex
    app.dependency_overrides[get_sgc_user] = _reviewer_user
    try:
        with mock.patch("dataregistry.api.sgc.query.get_sgc_plot_results",
                        return_value=[_qc_row(file_id)]), \
             mock.patch("dataregistry.api.sgc.check_review_permissions", return_value=True), \
             mock.patch("dataregistry.api.sgc.boto3.client") as bc:
            err = ClientError({"Error": {"Code": "NoSuchKey", "Message": "missing"}}, "GetObject")
            bc.return_value.get_object.side_effect = err
            r = api_client.get(f"/api/sgc/qc/plots/{file_id}/json",
                               headers={"Authorization": "Bearer test"})
    finally:
        app.dependency_overrides.pop(get_sgc_user, None)
    assert r.status_code == 404
