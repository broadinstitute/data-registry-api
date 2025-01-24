from functools import lru_cache

import requests


@lru_cache
def get_phenotypes() -> dict:
    http_res = requests.get('https://bioindex.hugeamp.org/api/portal/phenotypes')
    json = http_res.json()
    phenos = json['data']
    result = {pheno['name']: pheno for pheno in phenos}
    return result
