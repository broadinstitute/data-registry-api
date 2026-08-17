"""Cached bioindex portal-groups lookup (same pattern as phenotypes.get_phenotypes)."""
from functools import lru_cache

import requests


@lru_cache
def get_portals() -> list:
    res = requests.get('https://bioindex.hugeamp.org/api/portal/groups', timeout=30)
    res.raise_for_status()
    return sorted(g['name'] for g in res.json()['data'])
