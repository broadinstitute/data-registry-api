import os

from fastapi import Header, HTTPException, Depends

from dataregistry.api import api
import fastapi
from fastapi.middleware.cors import CORSMiddleware

from dataregistry.api.config import get_sensitive_config

valid_api_key = os.getenv('DATA_REGISTRY_API_KEY') if os.getenv('DATA_REGISTRY_API_KEY') \
    else get_sensitive_config()['apiKey']


async def verify_token(access_token: str = Header()):
    if access_token != valid_api_key:
        raise HTTPException(status_code=403, detail="access-token header invalid")


# create web server
app = fastapi.FastAPI(title='DataRegistry', redoc_url=None)

for route in api.router.routes:
    if route.name != 'stream_file':
        route.dependencies.append(Depends(verify_token))

# all the various routers for each api
app.include_router(api.router, prefix='/api', tags=['api'])

origins = [
    "http://localhost:3000",
    "http://localhost",
    "http://data-registry-vue.s3-website-us-east-1.amazonaws.com",
    "https://dcicue2yl1fq.cloudfront.net",
    "https://d68r3fsifmwz.cloudfront.net",
    "https://kpndataregistry.org",
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
