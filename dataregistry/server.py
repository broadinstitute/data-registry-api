from dataregistry.api import api
import fastapi
from fastapi.middleware.cors import CORSMiddleware


# create web server
app = fastapi.FastAPI(title='DataRegistry', redoc_url=None)

# all the various routers for each api
app.include_router(api.router, prefix='/api', tags=['api'])

# enable cross-origin resource sharing
app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)
