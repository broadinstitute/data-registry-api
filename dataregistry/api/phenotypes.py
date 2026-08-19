from functools import lru_cache

import requests

from dataregistry.api.config import bioindex_base_url


@lru_cache
def get_phenotypes() -> dict:
    http_res = requests.get(f'{bioindex_base_url()}/api/portal/phenotypes', timeout=30)
    json = http_res.json()
    phenos = json['data']
    result = {pheno['name']: pheno for pheno in phenos}
    return result
