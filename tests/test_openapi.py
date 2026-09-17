from fastapi.testclient import TestClient


def test_openapi_schema_builds(api_client: TestClient):
    """Building the OpenAPI schema instantiates every request/response model.
    It is the cheapest whole-app check that the pydantic models are valid."""
    resp = api_client.get("/openapi.json")
    assert resp.status_code == 200
    spec = resp.json()
    assert len(spec["paths"]) > 100
    assert "components" in spec and len(spec["components"]["schemas"]) > 50
