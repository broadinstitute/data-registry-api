import os

import boto3
import pytest
from fastapi.testclient import TestClient
from moto import mock_s3
from starlette.status import HTTP_422_UNPROCESSABLE_ENTITY, HTTP_200_OK, HTTP_400_BAD_REQUEST, HTTP_404_NOT_FOUND

from dataregistry.api.model import DataFormat

ACCESS_TOKEN = "access-token"

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
    response = api_client.get(api_path, headers={ACCESS_TOKEN: api_key})
    assert response.status_code == HTTP_200_OK
    assert len(response.json()) == 0


@mock_s3
def test_post_records(api_client: TestClient):
    set_up_moto_bucket()
    response = api_client.post(api_path,
                               headers={ACCESS_TOKEN: api_key},
                               json=example_json)
    assert response.status_code == HTTP_200_OK


def set_up_moto_bucket():
    # We need to create the bucket since this is all in Moto's 'virtual' AWS account
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket="dig-data-registry")


@mock_s3
def test_post_then_delete_records(api_client: TestClient):
    set_up_moto_bucket()
    new_record = example_json.copy()
    new_record['name'] = 'to-delete'
    response = api_client.post(api_path,
                               headers={ACCESS_TOKEN: api_key},
                               json=new_record)
    assert response.status_code == HTTP_200_OK
    record_id = response.json()['record_id']
    response = api_client.delete(f"{api_path}/{record_id}", headers={ACCESS_TOKEN: api_key})
    assert response.status_code == HTTP_200_OK
    get_by_id_res = api_client.get(f"{api_path}/{record_id}", headers={ACCESS_TOKEN: api_key})
    assert get_by_id_res.status_code == HTTP_404_NOT_FOUND


@mock_s3
def test_post_then_retrieve_by_id(api_client: TestClient):
    set_up_moto_bucket()
    new_record = example_json.copy()
    new_record['name'] = 'to-retrieve'
    response = api_client.post(api_path,
                               headers={ACCESS_TOKEN: api_key},
                               json=new_record)
    assert response.status_code == HTTP_200_OK
    new_record_id = response.json()['record_id']
    response = api_client.get(f"{api_path}/{new_record_id}", headers={ACCESS_TOKEN: api_key})
    assert response.status_code == HTTP_200_OK


@mock_s3
def test_upload_file(api_client: TestClient):
    set_up_moto_bucket()
    new_record = example_json.copy()
    record_name = 'file_upload_test'
    new_record['name'] = record_name
    api_client.post(api_path,
                    headers={ACCESS_TOKEN: api_key},
                    json=new_record)
    with open("tests/sample_upload.txt", "rb") as f:
        upload_response = api_client.post(f"/api/uploadfile/GWAS/t1d/{record_name}/1", headers={ACCESS_TOKEN: api_key},
                                          files={"file": f})
        assert upload_response.status_code == 200
    s3_conn = boto3.resource("s3", region_name="us-east-1")
    file_text = s3_conn.Object("dig-data-registry", f"GWAS/{record_name}/t1d/sample_upload.txt").get()["Body"].read()\
        .decode("utf-8")
    assert file_text == "The answer is 47!\n"


@pytest.mark.parametrize("df", DataFormat.__members__.values())
@mock_s3
def test_valid_data_formats_post(api_client: TestClient, df: DataFormat):
    set_up_moto_bucket()
    new_record = example_json.copy()
    new_record['data_format'] = df
    response = api_client.post(api_path, headers={ACCESS_TOKEN: api_key}, json=new_record)
    assert response.status_code == HTTP_200_OK


def test_invalid_record_post(api_client: TestClient):
    new_record = example_json.copy()
    new_record['ancestry'] = 'bad-ancestry'
    response = api_client.post(api_path, headers={ACCESS_TOKEN: api_key}, json=new_record)
    assert response.status_code == HTTP_422_UNPROCESSABLE_ENTITY
