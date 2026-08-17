"""Tests for user-service auth failure translation (get_sgc_user / get_hcm_user)."""
import asyncio

import fastapi
import httpx
import pytest

from dataregistry.api import hcm, sgc
from dataregistry.api.user_service_auth import SESSION_EXPIRED_DETAIL, auth_failure_exception


def run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


# --- auth_failure_exception (pure) ---

def test_upstream_401_maps_to_session_expired():
    exc = auth_failure_exception(httpx.Response(401, json={
        "detail": "Given token not valid for any token type",
        "messages": [{"message": "Token is invalid or expired"}],
    }))
    assert exc.status_code == 401
    assert exc.detail == SESSION_EXPIRED_DETAIL


def test_upstream_403_error_body_is_forwarded():
    exc = auth_failure_exception(httpx.Response(403, json={
        "error": "Token was not issued for group: hcm-prod",
    }))
    assert exc.status_code == 401
    assert exc.detail == "Token was not issued for group: hcm-prod"


def test_upstream_403_detail_body_is_forwarded():
    exc = auth_failure_exception(httpx.Response(403, json={
        "detail": "User does not belong to group",
    }))
    assert exc.status_code == 401
    assert exc.detail == "User does not belong to group"


def test_upstream_non_json_falls_back_to_invalid_token():
    exc = auth_failure_exception(httpx.Response(500, text="<html>oops</html>"))
    assert exc.status_code == 401
    assert exc.detail == "Invalid token"


def test_upstream_non_dict_json_falls_back_to_invalid_token():
    exc = auth_failure_exception(httpx.Response(403, json=["unexpected", "list", "body"]))
    assert exc.status_code == 401
    assert exc.detail == "Invalid token"


# --- wiring into the dependency functions ---

class FakeAsyncClient:
    def __init__(self, response):
        self._response = response

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def get(self, *args, **kwargs):
        return self._response


def _patch_client(monkeypatch, response):
    # sgc.httpx and hcm.httpx are the same module object; one patch covers both.
    monkeypatch.setattr(sgc.httpx, "AsyncClient", lambda: FakeAsyncClient(response))


def test_get_sgc_user_forwards_group_error(monkeypatch):
    _patch_client(monkeypatch, httpx.Response(403, json={
        "error": "Token was not issued for group: sgc-prod",
    }))
    with pytest.raises(fastapi.HTTPException) as exc_info:
        run(sgc.get_sgc_user(authorization="Bearer abc"))
    assert exc_info.value.status_code == 401
    assert exc_info.value.detail == "Token was not issued for group: sgc-prod"


def test_get_hcm_user_expired_token_message(monkeypatch):
    _patch_client(monkeypatch, httpx.Response(401, json={
        "detail": "Given token not valid for any token type",
    }))
    with pytest.raises(fastapi.HTTPException) as exc_info:
        run(hcm.get_hcm_user(authorization="Bearer abc"))
    assert exc_info.value.status_code == 401
    assert exc_info.value.detail == SESSION_EXPIRED_DETAIL


def test_get_hcm_user_success_path_still_returns_user(monkeypatch):
    _patch_client(monkeypatch, httpx.Response(200, json={
        "user": {"id": 1, "username": "anuj", "email": "a@ox.ac.uk",
                 "roles": ["reviewer"], "permissions": ["hcm-review-data"]},
    }))
    user = run(hcm.get_hcm_user(authorization="Bearer abc"))
    assert user.user_name == "anuj"
    assert "hcm-review-data" in user.permissions
