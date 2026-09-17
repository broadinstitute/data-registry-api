from fastapi.testclient import TestClient

from dataregistry.api import peg


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
