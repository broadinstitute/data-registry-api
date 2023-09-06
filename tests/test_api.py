import os

import boto3
import pytest
from fastapi.testclient import TestClient
from moto import mock_s3
from starlette.status import HTTP_422_UNPROCESSABLE_ENTITY, HTTP_200_OK, HTTP_404_NOT_FOUND

from dataregistry.api.model import DataFormat

ACCESS_TOKEN = "access-token"

api_key = os.getenv('DATA_REGISTRY_API_KEY')

dataset_api_path = '/api/datasets'
study_api_path = '/api/studies'

example_study_json = {
    "name": "Test Study",
    "institution": "UCSF"
}

example_dataset_json = {
    "name": "Cade2021_SleepApnea_Mixed_Female",
    "data_source_type": "file",
    "data_type": "wgs",
    "genome_build": "hg19",
    "ancestry": "EA",
    "data_submitter": "Jennifer Doudna",
    "data_submitter_email": "researcher@institute.org",
    "sex": "female",
    "global_sample_size": 11,
    "status": "open",
    "description": "Lorem ipsum..",
    "publicly_available": False
}


@pytest.fixture(autouse=True)
def check_that_api_key_is_set():
    assert api_key is not None and api_key.strip(), "Please set the DATA_REGISTRY_API_KEY environment variable"


def test_get_datasets(api_client: TestClient):
    response = api_client.get(dataset_api_path, headers={ACCESS_TOKEN: api_key})
    assert response.status_code == HTTP_200_OK
    assert len(response.json()) == 0


@mock_s3
def test_post_dataset(api_client: TestClient):
    set_up_moto_bucket()
    study_id = save_study(api_client)
    example_dataset_json.update({'study_id': study_id})
    response = api_client.post(dataset_api_path,
                               headers={ACCESS_TOKEN: api_key},
                               json=example_dataset_json)
    assert response.status_code == HTTP_200_OK


@mock_s3
def test_update_dataset(api_client: TestClient):
    set_up_moto_bucket()
    study_id = save_study(api_client)
    copy = example_dataset_json.copy()
    copy.update({'study_id': study_id})
    response = api_client.post(dataset_api_path,
                               headers={ACCESS_TOKEN: api_key},
                               json=copy)
    assert response.status_code == HTTP_200_OK
    copy.update({'id': response.json()['dataset_id']})
    response = api_client.patch(dataset_api_path, headers={ACCESS_TOKEN: api_key}, json=copy)
    assert response.status_code == HTTP_200_OK


def save_study(api_client):
    response = api_client.post(study_api_path, headers={ACCESS_TOKEN: api_key}, json=example_study_json)
    assert response.status_code == HTTP_200_OK
    study_id = response.json()['study_id']
    return study_id


def set_up_moto_bucket():
    # We need to create the bucket since this is all in Moto's 'virtual' AWS account
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket="dig-data-registry")


@mock_s3
def test_post_then_retrieve_by_id(api_client: TestClient):
    set_up_moto_bucket()
    new_dataset = example_dataset_json.copy()
    study_id = save_study(api_client)
    new_dataset.update({'study_id': study_id, 'name': 'to-retrieve'})
    response = api_client.post(dataset_api_path,
                               headers={ACCESS_TOKEN: api_key},
                               json=new_dataset)
    assert response.status_code == HTTP_200_OK
    new_ds_id = response.json()['dataset_id']
    response = api_client.get(f"{dataset_api_path}/{new_ds_id}", headers={ACCESS_TOKEN: api_key})
    assert response.status_code == HTTP_200_OK


@mock_s3
def test_upload_file(api_client: TestClient):
    new_record = add_ds_with_file(api_client)
    s3_conn = boto3.resource("s3", region_name="us-east-1")
    file_text = s3_conn.Object("dig-data-registry", f"{new_record['name']}/t1d/sample_upload.txt").get()["Body"].read() \
        .decode("utf-8")
    assert file_text == "The answer is 47!\n"


@mock_s3
def test_uploaded_file_is_not_public(api_client: TestClient):
    new_record = add_ds_with_file(api_client)
    response = api_client.get(f"/api/files/{new_record['phenotype_data_set_id']}/t1d/data/sample_upload.txt?filepath={new_record['name']}/t1d/sample_upload.txt",
                              headers={ACCESS_TOKEN: api_key})
    assert response.status_code == HTTP_404_NOT_FOUND


@mock_s3
def test_uploaded_file_is_public(api_client: TestClient):
    new_record = add_ds_with_file(api_client, public=True)
    response = api_client.get(
        f"/api/files/{new_record['phenotype_data_set_id']}/t1d/data/sample_upload.txt",
        headers={ACCESS_TOKEN: api_key})
    assert response.status_code == HTTP_200_OK

@mock_s3
def test_list_files(api_client: TestClient):
    new_record = add_ds_with_file(api_client, public=True)
    response = api_client.get(f"/api/filelist/{new_record['id']}", headers={ACCESS_TOKEN: api_key})
    assert response.status_code == HTTP_200_OK
    result = response.json()[0]
    assert result['path'] == f"files/{new_record['phenotype_data_set_id']}/t1d/data/sample_upload.txt"


def add_ds_with_file(api_client, public=False):
    set_up_moto_bucket()
    new_record = example_dataset_json.copy()
    record_name = 'file_upload_test'
    study_id = save_study(api_client)
    new_record.update({'study_id': study_id, 'name': record_name})
    if public:
        new_record.update({'publicly_available': True})
    create_record_res = api_client.post(dataset_api_path, headers={ACCESS_TOKEN: api_key}, json=new_record)
    assert create_record_res.status_code == HTTP_200_OK
    with open("tests/sample_upload.txt", "rb") as f:
        dataset_id = create_record_res.json()['dataset_id']
        upload_response = api_client.post(f"/api/uploadfile/{dataset_id}/t1d/true/10",
                                          headers={ACCESS_TOKEN: api_key, "Filename": "sample_upload.txt"},
                                          files={"file": f})
        assert upload_response.status_code == HTTP_200_OK
    new_record.update({'id': dataset_id, 'phenotype_data_set_id': upload_response.json()['phenotype_data_set_id']})
    return new_record


@mock_s3
def test_upload_credible_set(api_client: TestClient):
    ds = add_ds_with_file(api_client)
    with open("tests/sample_upload.txt", "rb") as f:
        credible_set_name = "credible_set"
        upload_response = api_client.post(f"/api/crediblesetupload/{ds['phenotype_data_set_id']}"
                                          f"/{credible_set_name}",
                                          headers={ACCESS_TOKEN: api_key, "Filename": "sample_upload.txt"},
                                          files={"file": f})
        assert upload_response.status_code == HTTP_200_OK
    saved_dataset = api_client.get(f"{dataset_api_path}/{ds['id']}", headers={ACCESS_TOKEN: api_key})
    json = saved_dataset.json()
    assert len(json['credible_sets']) == 1


@pytest.mark.parametrize("df", DataFormat.__members__.values())
@mock_s3
def test_valid_data_formats_post(api_client: TestClient, df: DataFormat):
    set_up_moto_bucket()
    study_id = save_study(api_client)
    new_record = example_dataset_json.copy()
    new_record['data_type'] = df
    new_record['study_id'] = study_id
    response = api_client.post(dataset_api_path, headers={ACCESS_TOKEN: api_key}, json=new_record)
    assert response.status_code == HTTP_200_OK


def test_invalid_record_post(api_client: TestClient):
    new_record = example_dataset_json.copy()
    new_record['ancestry'] = 'bad-ancestry'
    response = api_client.post(dataset_api_path, headers={ACCESS_TOKEN: api_key}, json=new_record)
    assert response.status_code == HTTP_422_UNPROCESSABLE_ENTITY
