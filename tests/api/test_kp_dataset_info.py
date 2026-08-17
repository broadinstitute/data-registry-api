import responses

from dataregistry.api.jwt_utils import get_encoded_jwt_data
from dataregistry.api.model import User
from tests.conftest import client

AUTH = {'Authorization': f"Bearer {get_encoded_jwt_data(User(user_name='test', roles=['admin'], id=1))}"}


@responses.activate
def test_kp_portals_lists_bioindex_groups():
    from dataregistry.api import portals
    portals.get_portals.cache_clear()
    responses.get('https://bioindex.hugeamp.org/api/portal/groups',
                  json={'data': [{'name': 'md'}, {'name': 'a2f'}, {'name': 'cvd'}]})
    resp = client.get('/api/kp-portals', headers=AUTH)
    assert resp.status_code == 200
    assert resp.json() == ['a2f', 'cvd', 'md']


def test_kp_portals_requires_auth():
    from dataregistry.api import portals
    portals.get_portals.cache_clear()
    assert client.get('/api/kp-portals').status_code == 401
