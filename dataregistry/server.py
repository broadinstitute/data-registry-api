import fastapi
from fastapi import Depends
from fastapi.middleware.cors import CORSMiddleware

from dataregistry.api import api
from dataregistry.api.api import get_current_user

ROUTES_WITHOUT_AUTH = {'stream_file', 'version', 'login', 'google_login'}

# create web server
app = fastapi.FastAPI(title='DataRegistry', redoc_url=None)

for route in api.router.routes:
    if route.name not in ROUTES_WITHOUT_AUTH:
        route.dependencies.append(Depends(get_current_user))

# all the various routers for each api
app.include_router(api.router, prefix='/api', tags=['api'])

origins = [
    "http://localhost:3000",
    "http://local.kpndataregistry.org:3000",
    "https://kpndataregistry.org",
    "https://local.kpndataregistry.org",
    "https://kpndataregistry.org:8000",
]
# enable cross-origin resource sharing
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)
