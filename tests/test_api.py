import re

import boto3
import pytest
from fastapi.testclient import TestClient
from moto import mock_s3
from starlette.status import HTTP_422_UNPROCESSABLE_ENTITY, HTTP_200_OK, HTTP_404_NOT_FOUND, HTTP_401_UNAUTHORIZED, \
    HTTP_400_BAD_REQUEST

from dataregistry.api.model import DataFormat, User
from dataregistry.api.jwt import get_encoded_jwt_data

AUTHORIZATION = "Authorization"

auth_token = f"Bearer {get_encoded_jwt_data(User(user_name='test', roles=['admin'], id=1))}"
view_only_token = f"Bearer {get_encoded_jwt_data(User(user_name='view', roles=['viewer'], id=2))}"

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


def test_get_datasets(api_client: TestClient):
    response = api_client.get(dataset_api_path, headers={AUTHORIZATION: auth_token})
    assert response.status_code == HTTP_200_OK
    assert len(response.json()) == 0


@mock_s3
def test_post_dataset(api_client: TestClient):
    create_new_dataset(api_client)


@mock_s3
def test_update_dataset(api_client: TestClient):
    copy, new_ds_id = create_new_dataset(api_client)
    copy.update({'id': new_ds_id, 'name': 'Updated Dataset Name'})
    response = api_client.patch(dataset_api_path, headers={AUTHORIZATION: auth_token}, json=copy)
    assert response.status_code == HTTP_200_OK
    response = api_client.get(f"{dataset_api_path}/{new_ds_id}", headers={AUTHORIZATION: auth_token})
    assert response.json()['dataset']['name'] == 'Updated Dataset Name'


@mock_s3
def test_update_dataset_fk_failure(api_client: TestClient):
    copy, new_ds_id = create_new_dataset(api_client)
    copy.update({'id': new_ds_id, 'study_id': 'missing_id'})
    response = api_client.patch(f"{dataset_api_path}", headers={AUTHORIZATION: auth_token}, json=copy)
    assert response.status_code == HTTP_400_BAD_REQUEST


def create_new_dataset(api_client, ds_info=example_dataset_json):
    set_up_moto_bucket()
    copy = ds_info.copy()
    if 'study_id' not in copy:
        study_id = save_study(api_client)
        copy.update({'study_id': study_id})
    create_response = api_client.post(dataset_api_path, headers={AUTHORIZATION: auth_token}, json=copy)
    assert create_response.status_code == HTTP_200_OK
    new_ds_id = create_response.json()['id']
    return copy, new_ds_id


def save_study(api_client):
    response = api_client.post(study_api_path, headers={AUTHORIZATION: auth_token}, json=example_study_json)
    assert response.status_code == HTTP_200_OK
    study_id = response.json()['id'].replace('-', '')
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
    copy, new_ds_id = create_new_dataset(api_client, new_dataset)
    response = api_client.get(f"{dataset_api_path}/{new_ds_id}", headers={AUTHORIZATION: auth_token})
    assert response.status_code == HTTP_200_OK


@mock_s3
def test_upload_file(api_client: TestClient):
    new_record = add_ds_with_file(api_client)
    s3_conn = boto3.resource("s3", region_name="us-east-1")
    file_text = s3_conn.Object("dig-data-registry", f"{new_record['dataset']['name']}/t1d/sample_upload.txt").get()[
        "Body"].read() \
        .decode("utf-8")
    assert file_text == "The answer is 47!\n"


@mock_s3
def test_uploaded_file_is_not_public(api_client: TestClient):
    new_record = add_ds_with_file(api_client)
    response = api_client.get(f"/api/d/{new_record['phenotypes'][0]['short_id']}",
                              headers={AUTHORIZATION: auth_token})
    assert response.status_code == HTTP_404_NOT_FOUND


@mock_s3
def test_uploaded_file_is_public(api_client: TestClient):
    new_record = add_ds_with_file(api_client, public=True)
    response = api_client.get(
        f"/api/d/{new_record['phenotypes'][0]['short_id']}",
        headers={AUTHORIZATION: auth_token})
    assert response.status_code == HTTP_200_OK


@mock_s3
def test_list_files(api_client: TestClient):
    new_record = add_ds_with_file(api_client, public=True)
    response = api_client.get(f"/api/filelist/{new_record['dataset']['id']}", headers={AUTHORIZATION: auth_token})
    assert response.status_code == HTTP_200_OK
    result = response.json()[0]
    assert re.match(r'd/[a-zA-Z0-9]{6}', result['path']) is not None


def add_ds_with_file(api_client, public=False):
    new_record = example_dataset_json.copy()
    record_name = 'file_upload_test'
    study_id = save_study(api_client)
    new_record.update({'study_id': study_id, 'name': record_name})
    if public:
        new_record.update({'publicly_available': True})
    copy, new_dataset_id = create_new_dataset(api_client, ds_info=new_record)
    with open("tests/sample_upload.txt", "rb") as f:
        upload_response = api_client.post(f"/api/uploadfile/{new_dataset_id.replace('-', '')}/true/10?phenotype=t1d",
                                          headers={AUTHORIZATION: auth_token, "Filename": "sample_upload.txt"},
                                          files={"file": f})
        assert upload_response.status_code == HTTP_200_OK
    new_record = api_client.get(f"/api/datasets/{new_dataset_id}", headers={AUTHORIZATION: auth_token}).json()
    return new_record


@mock_s3
def test_upload_credible_set(api_client: TestClient):
    ds = add_ds_with_file(api_client)
    with open("tests/sample_upload.txt", "rb") as f:
        credible_set_name = "credible_set"
        upload_response = api_client.post(
            f"/api/crediblesetupload/{str(ds['phenotypes'][0]['id']).replace('-', '')}/{credible_set_name}",
            headers={AUTHORIZATION: auth_token, "Filename": "sample_upload.txt"},
            files={"file": f})
        assert upload_response.status_code == HTTP_200_OK
    saved_dataset = api_client.get(f"{dataset_api_path}/{ds['dataset']['id']}", headers={AUTHORIZATION: auth_token})
    json = saved_dataset.json()
    assert len(json['credible_sets']) == 1


@pytest.mark.parametrize("df", DataFormat.__members__.values())
@mock_s3
def test_valid_data_formats_post(api_client: TestClient, df: DataFormat):
    new_record = example_dataset_json.copy()
    new_record['data_type'] = df
    create_new_dataset(api_client, new_record)


def test_invalid_record_post(api_client: TestClient):
    new_record = example_dataset_json.copy()
    new_record['ancestry'] = 'bad-ancestry'
    response = api_client.post(dataset_api_path, headers={AUTHORIZATION: auth_token}, json=new_record)
    assert response.status_code == HTTP_422_UNPROCESSABLE_ENTITY


def test_delete_dataset(api_client: TestClient):
    ds_with_file = add_ds_with_file(api_client)['dataset']
    ds_id = ds_with_file['id']
    del_response = api_client.delete(f"{dataset_api_path}/{ds_id}", headers={AUTHORIZATION: auth_token})
    assert del_response.status_code == HTTP_200_OK
    saved_dataset_response = api_client.get(f"{dataset_api_path}/{ds_id}", headers={AUTHORIZATION: auth_token})
    assert saved_dataset_response.status_code == HTTP_404_NOT_FOUND


def test_delete_dataset_without_auth(api_client: TestClient):
    ds_with_file = add_ds_with_file(api_client)['dataset']
    ds_id = ds_with_file['id']
    response = api_client.delete(f"{dataset_api_path}/{ds_id}")
    assert response.status_code == HTTP_401_UNAUTHORIZED


def test_delete_without_access(api_client: TestClient):
    ds_with_file = add_ds_with_file(api_client)['dataset']
    ds_id = ds_with_file['id']
    response = api_client.delete(f"{dataset_api_path}/{ds_id}", headers={AUTHORIZATION: view_only_token})
    assert response.status_code == HTTP_401_UNAUTHORIZED
