# data-registry-api

# Running the server

At the moment there are no env vars. The AWS credentials are stored directly on the server it is running on.
The database configuration is fetched from AWS secrets manager.
And the s3 bucket and secrets id are both hardcoded at the moment.

To run the server simply:
`python3.8 -m dataregistry.main serve`

To test in a separate window (running it locally) you can use python to hit the server a la:
`requests.post('http://localhost:5000/api/records', json={"name": "test 13", "description": "test description"})`

For testing currently change the database and s3 bucket
TODO: These should default to a dev version and on the server itself it should have an override to point to prod
