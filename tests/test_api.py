import os

from fastapi.testclient import TestClient

api_key = os.getenv('DATA_REGISTRY_API_KEY')


api_path = '/api/records'
def test_get_records(api_client: TestClient):
    response = api_client.get(api_path, headers={"access_token": api_key})
    assert response.status_code == 200
    assert len(response.json()) == 0


def test_post_records(api_client: TestClient):
    response = api_client.post(api_path,
                               headers={"access_token": api_key},
                               json={"name": "foobar", "metadata": {"foo": 11}})
    assert response.status_code == 200


def test_post_then_delete_records(api_client: TestClient):
    response = api_client.post(api_path,
                               headers={"access_token": api_key},
                               json={"name": "to-delete", "metadata": {"foo": 11}})
    assert response.status_code == 200
    records_in_db = api_client.get(api_path, headers={"access_token": api_key}).json()
    to_delete = next((record for record in records_in_db if record['name'] == 'to-delete'), None)
    assert to_delete is not None
    response = api_client.delete(f"{api_path}/{to_delete['id']}", headers={"access_token": api_key})
    assert response.status_code == 200
    records_in_db = api_client.get(api_path, headers={"access_token": api_key}).json()
    to_delete = next((record for record in records_in_db if record['name'] == 'to-delete'), None)
    assert to_delete is None