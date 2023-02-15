import os

import pytest
from fastapi.testclient import TestClient
from starlette.status import HTTP_422_UNPROCESSABLE_ENTITY, HTTP_200_OK

from dataregistry.api.model import Ancestry, DataFormat

api_key = os.getenv('DATA_REGISTRY_API_KEY')

api_path = '/api/records'

example_json = {
    "name": "Cade2021_SleepApnea_Mixed_Female",
    "data_source_type": "file",
    "data_source": "??",
    "data_type": "wgs",
    "genome_build": "hg19",
    "ancestry": "EA",
    "data_submitter": "Jennifer Doudna",
    "data_submitter_email": "researcher@institute.org",
    "institution": "UCSD",
    "sex": "female",
    "global_sample_size": 11,
    "t1d_sample_size": 12,
    "bmi_adj_sample_size": 19,
    "status": "open",
    "additional_data": "Lorem ipsum..",
    "metadata": {"foo": 11}
}


def test_get_records(api_client: TestClient):
    response = api_client.get(api_path, headers={"access_token": api_key})
    assert response.status_code == HTTP_200_OK
    assert len(response.json()) == 0


def test_post_records(api_client: TestClient):
    response = api_client.post(api_path,
                               headers={"access_token": api_key},
                               json=example_json)
    assert response.status_code == HTTP_200_OK


def test_post_then_delete_records(api_client: TestClient):
    new_record = example_json.copy()
    new_record['name'] = 'to-delete'
    response = api_client.post(api_path,
                               headers={"access_token": api_key},
                               json=new_record)
    assert response.status_code == HTTP_200_OK
    records_in_db = api_client.get(api_path, headers={"access_token": api_key}).json()
    to_delete = next((record for record in records_in_db if record['name'] == 'to-delete'), None)
    assert to_delete is not None
    response = api_client.delete(f"{api_path}/{to_delete['id']}", headers={"access_token": api_key})
    assert response.status_code == HTTP_200_OK
    records_in_db = api_client.get(api_path, headers={"access_token": api_key}).json()
    to_delete = next((record for record in records_in_db if record['name'] == 'to-delete'), None)
    assert to_delete is None


def test_post_then_retrieve_by_id(api_client: TestClient):
    new_record = example_json.copy()
    new_record['name'] = 'to-retrieve'
    response = api_client.post(api_path,
                               headers={"access_token": api_key},
                               json=new_record)
    assert response.status_code == HTTP_200_OK
    records_in_db = api_client.get(api_path, headers={"access_token": api_key}).json()
    to_retrieve = next((record for record in records_in_db if record['name'] == 'to-retrieve'), None)
    response = api_client.get(f"{api_path}/{to_retrieve['id']}", headers={"access_token": api_key})
    assert response.status_code == HTTP_200_OK


@pytest.mark.parametrize("df", DataFormat.__members__.values())
def test_valid_data_formats_post(api_client: TestClient, df: DataFormat):
    new_record = example_json.copy()
    new_record['data_format'] = df
    response = api_client.post(api_path, headers={"access_token": api_key}, json=new_record)
    assert response.status_code == HTTP_200_OK


def test_invalid_record_post(api_client: TestClient):
    new_record = example_json.copy()
    new_record['ancestry'] = 'bad-ancestry'
    response = api_client.post(api_path, headers={"access_token": api_key}, json=new_record)
    assert response.status_code == HTTP_422_UNPROCESSABLE_ENTITY
