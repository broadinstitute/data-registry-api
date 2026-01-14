import fastapi
from fastapi import Depends
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

from dataregistry.api import api, sgc, peg, mskkp
from dataregistry.api.api import get_current_user
from dataregistry.api.sgc import get_sgc_user


SGC_ROUTES_WITHOUT_AUTH = {'hello_sgc'}
ROUTES_WITHOUT_AUTH = {'stream_file', 'version', 'login', 'google_login', 'start_aggregator', 'search_phenotypes', 'search_terms', 'preview_files', 'download_sgc_phenotypes'}

# Middleware to handle large file uploads
class LargeUploadMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # For multipart uploads, pre-parse with increased limits
        if request.headers.get("content-type", "").startswith("multipart/form-data"):
            # Increase limits to handle large GWAS files (up to 10GB)
            # max_part_size: max size of individual form field (10GB for file uploads)
            # max_files: max number of files (default 1000 is fine)
            # max_fields: max number of form fields (default 1000 is fine)  
            await request.form(
                max_files=1000,
                max_fields=1000,
                max_part_size=10 * 1024 * 1024 * 1024  # 10GB
            )
        
        response = await call_next(request)
        return response

# create web server
app = fastapi.FastAPI(title='DataRegistry', redoc_url=None)

# Add middleware for large uploads BEFORE other middleware
app.add_middleware(LargeUploadMiddleware)

for route in api.router.routes:
    if route.name not in ROUTES_WITHOUT_AUTH:
        route.dependencies.append(Depends(get_current_user))

for route in sgc.router.routes:
    if route.name not in SGC_ROUTES_WITHOUT_AUTH:
        route.dependencies.append(Depends(get_sgc_user))

# all the various routers for each api
app.include_router(peg.router, prefix='/api', tags=['peg'])
app.include_router(sgc.router, prefix='/api', tags=['sgc'])
app.include_router(mskkp.router, prefix='/api', tags=['mskkp'])
app.include_router(api.router, prefix='/api', tags=['api'])

origins = [
    "http://localhost:3000",
    "http://localhost:8090",
    "https://dev.cfdeknowledge.org",
    "https://cfdeknowledge.org",
    "http://local.kpndataregistry.org:3000",
    "https://kpndataregistry.org",
    "https://local.kpndataregistry.org:8000",
    "https://hermes.kpndataregistry.org:8000",
    "https://hermes.local.kpndataregistry.org:8000",
    "http://hermes.local.kpndataregistry.org:3000",
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
