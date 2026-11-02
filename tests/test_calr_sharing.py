import boto3
import pytest
from fastapi.testclient import TestClient
from moto import mock_aws
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from dataregistry.server import app
from dataregistry.api import calr, calr_query
from dataregistry.api.calr_model import CALRSubmission
from dataregistry.api.calr import _can_read, get_calr_user, get_calr_user_optional
from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api.model import User
from dataregistry.api.rate_limit import FailedLookupLimiter

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


@pytest.fixture(autouse=True)
def reset_shared_lookup_limiter():
    """The limiter is process-global; keep one test's 404s from counting against the next."""
    calr.shared_lookup_limiter.reset()
    yield
    calr.shared_lookup_limiter.reset()


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
def test_upload_assigns_calr_id(api_client: TestClient):
    set_up_moto_bucket()
    created = _upload_standard(api_client)
    calr_id = created["calr_id"]

    assert calr_id.startswith("CalR")
    assert len(calr_id) == len("CalR") + 8
    # base58: no 0/O/I/l, so a dictated or re-typed id cannot be ambiguous
    assert not set(calr_id[len("CalR"):]) & set("0OIl")

    # and it comes back on the listing, not just the create response
    assert api_client.get("/api/calr/files").json()[0]["calr_id"] == calr_id


@mock_aws
def test_calr_ids_are_distinct_across_submissions(api_client: TestClient):
    set_up_moto_bucket()
    first = _upload_standard(api_client, name="Experiment A")
    second = _upload_standard(api_client, name="Experiment B")
    assert first["calr_id"] != second["calr_id"]


@mock_aws
def test_shared_endpoint_resolves_by_calr_id(api_client: TestClient):
    set_up_moto_bucket()
    created = _upload_standard(api_client)
    api_client.patch(f"/api/calr/files/{created['submission_id']}?shared=true")

    resp = api_client.get(f"/api/calr/shared/{created['calr_id']}")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["id"] == created["submission_id"]
    assert body["calr_id"] == created["calr_id"]


@mock_aws
def test_shared_endpoint_resolves_public_experiment_without_share_flag(api_client: TestClient):
    """A public experiment is addressable by link even with sharing off — that is
    what /analysis/<calr_id> relies on for published datasets."""
    set_up_moto_bucket()
    created = _upload_standard(api_client)
    api_client.patch(f"/api/calr/files/{created['submission_id']}?public=true")

    resp = api_client.get(f"/api/calr/shared/{created['calr_id']}")
    assert resp.status_code == 200, resp.text
    assert resp.json()["public"] is True


@mock_aws
def test_shared_endpoint_404_for_private_calr_id(api_client: TestClient):
    """Private and nonexistent must look identical, or the id space is enumerable."""
    set_up_moto_bucket()
    created = _upload_standard(api_client)
    private = api_client.get(f"/api/calr/shared/{created['calr_id']}")
    unknown = api_client.get("/api/calr/shared/CalRzzzzzzzz")
    assert private.status_code == 404
    assert unknown.status_code == 404
    assert private.json() == unknown.json()


@mock_aws
def test_metadata_patch_cannot_set_experiment_id(api_client: TestClient):
    """experiment_id is no longer a writable field; calr_id replaces it."""
    set_up_moto_bucket()
    created = _upload_standard(api_client)
    sid = created["submission_id"]

    resp = api_client.patch(
        f"/api/calr/submissions/{sid}/metadata",
        json={"experiment_id": "CalRhandtyped", "strain": "C57BL/6J"},
    )
    assert resp.status_code == 200, resp.text

    sub = api_client.get("/api/calr/files").json()[0]
    assert sub["metadata"].get("strain") == "C57BL/6J"
    assert "experiment_id" not in sub["metadata"]
    assert sub["calr_id"] == created["calr_id"]


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


def _scripted_calr_ids(monkeypatch, ids):
    """Make generate_calr_id return `ids` in order, then fresh ids; returns the call log.

    The fresh-id fallback means a retry the code should not make shows up as a wrong
    call count, not as an IndexError from an exhausted script.
    """
    remaining = list(ids)
    calls = []

    def fake_generate():
        calls.append(1)
        return remaining.pop(0) if remaining else f"CalRfresh{len(calls):03d}"

    monkeypatch.setattr("dataregistry.api.calr_query.generate_calr_id", fake_generate)
    return calls


def _new_submission(name="Experiment", **kwargs):
    return CALRSubmission(name=name, uploaded_by=CALR_USER.user_name, **kwargs)


def test_insert_retries_on_calr_id_collision(monkeypatch):
    calls = _scripted_calr_ids(monkeypatch, ["CalRaaaaaaaa", "CalRaaaaaaaa", "CalRbbbbbbbb"])
    _, first_calr_id = calr_query.insert_calr_submission(engine, _new_submission("First"))
    assert first_calr_id == "CalRaaaaaaaa"
    del calls[:]

    sub_id, calr_id = calr_query.insert_calr_submission(engine, _new_submission("Second"))

    assert calr_id == "CalRbbbbbbbb"
    assert len(calls) == 2  # the colliding id, then one fresh id
    with engine.connect() as conn:
        stored = conn.execute(text("SELECT calr_id FROM calr_submissions WHERE id = :id"), {"id": sub_id}).scalar()
    assert stored == "CalRbbbbbbbb"


def test_insert_gives_up_after_three_calr_id_collisions(monkeypatch):
    calls = _scripted_calr_ids(
        monkeypatch,
        ["CalRaaaaaaaa", "CalRaaaaaaaa", "CalRaaaaaaaa", "CalRaaaaaaaa"],
    )
    calr_query.insert_calr_submission(engine, _new_submission("First"))
    del calls[:]

    with pytest.raises(IntegrityError):
        calr_query.insert_calr_submission(engine, _new_submission("Second"))

    assert len(calls) == calr_query.CALR_ID_ATTEMPTS == 3
    with engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM calr_submissions")).scalar() == 1


def test_insert_does_not_retry_other_integrity_errors(monkeypatch):
    calls = _scripted_calr_ids(monkeypatch, ["CalRaaaaaaaa", "CalRbbbbbbbb"])
    existing_id = calr_query.insert_calr_submission(engine, _new_submission("First"))[0]
    del calls[:]

    # Re-using a submission id is a primary-key clash, not a calr_id collision:
    # fresh calr_ids cannot fix it, so retrying would only burn attempts.
    duplicate = _new_submission("Second", id=existing_id)
    with pytest.raises(IntegrityError):
        calr_query.insert_calr_submission(engine, duplicate)

    assert len(calls) == 1
    with engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM calr_submissions")).scalar() == 1


def test_is_calr_id_collision_requires_duplicate_errno_and_calr_id_key():
    key = "calr_submissions.calr_submissions_calr_id_uniq"
    collision = IntegrityError("INSERT", {}, Exception(1062, f"Duplicate entry 'CalRx' for key '{key}'"))
    other_key = IntegrityError("INSERT", {}, Exception(1062, "Duplicate entry 'x' for key 'calr_submissions.PRIMARY'"))
    other_errno = IntegrityError("INSERT", {}, Exception(1048, f"Column 'calr_id' cannot be null ({key})"))
    no_args = IntegrityError("INSERT", {}, Exception())

    assert calr_query._is_calr_id_collision(collision) is True
    assert calr_query._is_calr_id_collision(other_key) is False
    assert calr_query._is_calr_id_collision(other_errno) is False
    assert calr_query._is_calr_id_collision(no_args) is False


def test_calr_ids_differing_only_by_case_are_distinct(api_client: TestClient, monkeypatch):
    _scripted_calr_ids(monkeypatch, ["CalRabcdefgh", "CalRABCDEFGH"])
    lower_sub = calr_query.insert_calr_submission(engine, _new_submission("Lower"))[0]
    upper_sub = calr_query.insert_calr_submission(engine, _new_submission("Upper"))[0]
    calr_query.set_calr_submission_public(engine, lower_sub, True)
    calr_query.set_calr_submission_public(engine, upper_sub, True)

    lower = api_client.get("/api/calr/shared/CalRabcdefgh")
    upper = api_client.get("/api/calr/shared/CalRABCDEFGH")

    assert lower.status_code == 200, lower.text
    assert upper.status_code == 200, upper.text
    assert lower.json()["id"] == lower_sub
    assert upper.json()["id"] == upper_sub
    # a third casing is neither id
    assert api_client.get("/api/calr/shared/CALRABCDEFGH").status_code == 404


@mock_aws
def test_file_info_includes_calr_id(api_client: TestClient):
    set_up_moto_bucket()
    created = _upload_standard(api_client)
    calr_query.set_calr_submission_shared(engine, created["submission_id"], True)

    resp = api_client.get(f"/api/calr/files/{created['file_id']}/info")

    assert resp.status_code == 200, resp.text
    assert resp.json()["calr_id"] == created["calr_id"]


@mock_aws
def test_replace_response_includes_calr_id(api_client: TestClient):
    set_up_moto_bucket()
    created = _upload_standard(api_client)

    resp = api_client.put(
        f"/api/calr/files/{created['submission_id']}",
        files={"standard_file": ("replacement.csv", b"col1,col2\n3,4\n", "text/csv")},
    )

    assert resp.status_code == 200, resp.text
    assert resp.json()["calr_id"] == created["calr_id"]


def _install_limiter(monkeypatch, max_failures=3, window_seconds=60.0):
    """Swap in a limiter on a settable clock; returns (limiter, clock) where
    clock.advance(seconds) moves time forward."""
    class Clock:
        now = 1000.0

        def __call__(self):
            return self.now

        def advance(self, seconds):
            self.now += seconds

    clock = Clock()
    limiter = FailedLookupLimiter(max_failures=max_failures, window_seconds=window_seconds, clock=clock)
    monkeypatch.setattr(calr, 'shared_lookup_limiter', limiter)
    return limiter, clock


def test_shared_lookup_blocks_after_repeated_failed_lookups(api_client: TestClient, monkeypatch):
    _install_limiter(monkeypatch, max_failures=3)

    for i in range(3):
        assert api_client.get(f"/api/calr/shared/CalRmissing{i}").status_code == 404

    blocked = api_client.get("/api/calr/shared/CalRmissing3")
    assert blocked.status_code == 429
    assert blocked.headers["Retry-After"] == "60"
    assert blocked.json()["detail"] == "Too many failed lookups. Try again later."


def test_shared_lookup_unblocks_after_the_window(api_client: TestClient, monkeypatch):
    _, clock = _install_limiter(monkeypatch, max_failures=3, window_seconds=60.0)
    for i in range(3):
        api_client.get(f"/api/calr/shared/CalRmissing{i}")
    assert api_client.get("/api/calr/shared/CalRmissing3").status_code == 429

    clock.advance(60)

    assert api_client.get("/api/calr/shared/CalRmissing4").status_code == 404


def test_rejected_requests_do_not_extend_the_block(api_client: TestClient, monkeypatch):
    _, clock = _install_limiter(monkeypatch, max_failures=3, window_seconds=60.0)
    for i in range(3):
        api_client.get(f"/api/calr/shared/CalRmissing{i}")

    clock.advance(30)
    rejected = api_client.get("/api/calr/shared/CalRmissing3")
    assert rejected.status_code == 429
    assert rejected.headers["Retry-After"] == "30"

    clock.advance(30)   # 60s after the real failures, 30s after the rejected request
    assert api_client.get("/api/calr/shared/CalRmissing4").status_code == 404


@mock_aws
def test_successful_lookups_are_never_rate_limited(api_client: TestClient, monkeypatch):
    _install_limiter(monkeypatch, max_failures=3)
    set_up_moto_bucket()
    created = _upload_standard(api_client)
    calr_query.set_calr_submission_shared(engine, created["submission_id"], True)

    for _ in range(10):
        resp = api_client.get(f"/api/calr/shared/{created['calr_id']}")
        assert resp.status_code == 200, resp.text

    # none of that counted: a full allowance of failures is still available
    for i in range(3):
        assert api_client.get(f"/api/calr/shared/CalRmissing{i}").status_code == 404


@mock_aws
def test_blocked_client_is_refused_even_for_a_valid_key(api_client: TestClient, monkeypatch):
    _install_limiter(monkeypatch, max_failures=3)
    set_up_moto_bucket()
    created = _upload_standard(api_client)
    calr_query.set_calr_submission_shared(engine, created["submission_id"], True)
    for i in range(3):
        api_client.get(f"/api/calr/shared/CalRmissing{i}")

    resp = api_client.get(f"/api/calr/shared/{created['calr_id']}")

    assert resp.status_code == 429
    assert "Retry-After" in resp.headers


def test_shared_lookup_limit_ignores_x_forwarded_for(api_client: TestClient, monkeypatch):
    """There is no proxy in front of the API, so X-Forwarded-For is attacker-controlled:
    rotating it must not dodge the limit."""
    _install_limiter(monkeypatch, max_failures=3)
    for i in range(3):
        resp = api_client.get(f"/api/calr/shared/CalRmissing{i}", headers={"X-Forwarded-For": f"203.0.113.{i}"})
        assert resp.status_code == 404

    resp = api_client.get("/api/calr/shared/CalRmissing3", headers={"X-Forwarded-For": "198.51.100.7"})

    assert resp.status_code == 429
