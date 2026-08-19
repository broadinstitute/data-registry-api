"""Cached bioindex portal-groups lookup (same pattern as phenotypes.get_phenotypes)."""
from functools import lru_cache

import requests

from dataregistry.api.config import bioindex_base_url


@lru_cache
def get_portals() -> list:
    res = requests.get(f'{bioindex_base_url()}/api/portal/groups', timeout=30)
    res.raise_for_status()
    return sorted(g['name'] for g in res.json()['data'])
