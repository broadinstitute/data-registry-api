# data-registry-api

## Running the server
### Running Locally
You will need python 3.8 or above installed and docker if you wish to use local docker db server.
You can start a local mysql via `./docker_db/docker_db.sh start` and the run `alembic upgrade head` to create the db tables.
You'll likely want/need to specify the following environment variables:
- `DATA_REGISTRY_DB_CONNECTION` - the connection string to the database, e.g. `export DATA_REGISTRY_DB_CONNECTION=mysql+pymysql://dataregistry:dataregistry@localhost:3307/dataregistry`will work if you're using the docker_db script
- `DATA_REGISTRY_S3_BUCKET` - the s3 bucket to use for storing the files (use dig-data-registry-qa unless you need prod data)
- `DATA_REGISTRY_API_KEY` - this is the api key that the server will look for in nearly all requests and return a 403
- `GOOGLE_OAUTH_CLIENT_ID` - this necessary if you will be testing authentication look this up https://console.cloud.google.com/apis/credentials?project=kpn-data-registry
- `GOOGLE_OAUTH_CLIENT_SECRET` - same as above
- `GOOGLE_OAUTH_REDIRECT_URI` - http://local.kpndataregistry.org:3000/login/callback

To set up dependencies via virtual environment:
`python -m venv venv`
`source venv/bin/activate`
`pip install -r requirements.txt`

Override the db via an environment variable if you are using a local db:
`export DATA_REGISTRY_DB_CONNECTION=mysql+pymysql://dataregistry:dataregistry@localhost:3307/dataregistry`

To run the server simply:
`python3.8 -m dataregistry.main serve`

FastAPI will automatically generate a swagger/open page at http://localhost:5000/docs which you can use to test/explore all the API endpoints.

For unit tests:
- Start a local mysql via `./docker_db/docker_db.sh start`
- if you want to override the test db's url, skip the above and set the env var `DATA_REGISTRY_TEST_DB_CONNECTION`
- From project root run `pytest` or you can run tests from Pycharm so long as you set the working directory to the project root.

### Running Docker
Python dependencies can be a pain.  If you just need a running a API server so you can do front end work, using Docker
is a decent option.  Depending on your needs you will need to specify some or all of the environments variables listed above.
You can do that by creating an .env in the dataregistry directory (keep this out of source control) or specifying the 
variables with the -e option in the docker run invocation below.

`docker build -t <name> .`

`docker run --rm --name dr_container -p 5000:5000 <name>`

With some environment variables specified: `docker run -e DATA_REGISTRY_DB_CONNECTION=<> --rm --name dr_container -p 5000:5000 <name>`

## Deploying the server
- This [Jenkins Job](http://107.22.69.235:8080/view/Data%20Registry/job/DR%20-%20Backend%20-%20Dev/) will deploy the head of the main branch to http://ec2-3-84-156-50.compute-1.amazonaws.com:5000/docs
- Pushes/PR merges to main will deploy to QA via Github actions.
- Publishing a tag pointing to a commit in the main branch will deploy that version to production

