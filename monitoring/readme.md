## Data Registry API Monitoring
 - We use AWS and their Canary functionality to monitor the API.  The canary config is in [monitoring.yml](monitoring.yml).
 - The monitor functionality is very basic and just looks for a 200 response from the API. This will alert us when the python process crashes and needs to be restarted and when our SSL cert expires.
### SSL Cert Expiry
 - We use let's encrypt to avoid paying for SSL certs.  They will issue certificates with a 90 day expiration.  Unfortunately because data registry api runs as a standalone python server, renewals need to be done manually.  
 - To renew the cert for api.kpndataregistry.org you'll need to login to our DNS registrar which is currently [squarespace](https://account.squarespace.com/domains).  
 - You can use the [acme tool](https://github.com/acmesh-official/acme.sh) to renew the cert.  Most recently I used the dockerized version: ``docker run --rm  -it  \
  -v "$(pwd)/out":/acme.sh  \
  --net=host \
  neilpang/acme.sh  --renew -d api.kpndataregistry.org --dns --yes-I-know-dns-manual-mode-enough-go-ahead-please
`` First, it will tell you to add txt dns record under the `_acme-challenge` subdomain.  You should see that subdomain already present in square space. Just update it with the new value.  Then run the command again, and it should output the cert and key files to the out directory.  You can then scp the files to the data registry api server and put them in the `/home/ec2-user/ssl` directory.
 - Finally, make note of the new expiration and set a calendar reminder to do this again in 90 days.  You can check the expiration of a cert with `openssl x509 -noout -dates -in /path/to/cert.pem`
