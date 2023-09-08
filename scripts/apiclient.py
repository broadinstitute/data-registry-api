import os
from typing import List

import requests
from pydantic import parse_obj_as

from dataregistry.api.model import SavedStudy, SavedDataset

ACCESS_HEADER = {"access-token": os.getenv('DATA_REGISTRY_API_KEY')}
JSON_HEADER = {"Content-Type": "application/json"}
SERVER_URL = os.getenv('DATA_REGISTRY_API_SERVER', "http://localhost:5000")
STUDIES_URL = f"{SERVER_URL}/api/studies"
DATASETS_URL = f"{SERVER_URL}/api/datasets"


def save_study(study):
    response = requests.post(STUDIES_URL, data=study.json(), headers={**ACCESS_HEADER, **JSON_HEADER})
    return parse_obj_as(SavedStudy, response.json())


def save_dataset(dataset):
    response = requests.post(DATASETS_URL, data=dataset.json(), headers={**ACCESS_HEADER, **JSON_HEADER})
    return parse_obj_as(SavedDataset, response.json())


def get_datasets() -> list:
    response = requests.get(DATASETS_URL, headers={**ACCESS_HEADER, **JSON_HEADER})
    return parse_obj_as(List[SavedDataset], response.json())


def get_studies() -> list:
    response = requests.get(STUDIES_URL, headers={**ACCESS_HEADER, **JSON_HEADER})
    return parse_obj_as(List[SavedStudy], response.json())


def upload_phenotype(data_set_id, phenotype, dichotomous, sample_size, filename, filepath,
                     url=f"{SERVER_URL}/api/uploadfile", cases=None, controls=None):
    full_url = f"{url}/{data_set_id}/{phenotype}/{dichotomous}/{sample_size}"
    if cases and controls:
        full_url += f"?cases={cases}&controls={controls}"

    file_name_header = {
        'Filename': filename,
    }

    with open(filepath, 'rb') as f:
        files = {'file': f}
        response = requests.post(full_url, headers={**file_name_header, **ACCESS_HEADER}, files=files)

    return response.json()
