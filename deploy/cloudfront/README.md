# CloudFront downloads: setup and teardown

`stream_file` sends a share of downloads from `dig-data-registry` to CloudFront signed links
(`dataregistry/api/cloudfront.py`). Run these from the repo root with the infra venv; only boto3 1.44.0 has
`pricing-plan-manager`. Build it with `uv venv --python 3.12 deploy/cloudfront/.venv` then
`uv pip install --python deploy/cloudfront/.venv/bin/python -r deploy/cloudfront/requirements.txt`.

## Run order (`deploy/cloudfront/.venv/bin/python -m deploy.cloudfront.<module>`)
1. `setup_downloads_cdn`: creates or finds each resource below, safe to re-run, prints `DISTRIBUTION_ID=`,
   `DISTRIBUTION_ARN=`, `DOMAIN=`, `KEY_PAIR_ID=`, `WEB_ACL_ARN=`, `SUBSCRIPTION_ARN=`. It refuses (exit 1) if an
   existing distribution, key group, web ACL or bucket policy is not what it builds, or if it would rewrite the
   prd config while the prd percent is not `0`.
2. `check_filename_function`: runs the file-name function in CloudFront; expect `6/6 PASS`.
3. Raise `/data-registry/<env>/cloudfront-downloads-percent` to roll out. Setup leaves both at `0`.

## Resources
New distribution (comment `data-registry-downloads (stream_file)`), WAF web ACL, CloudFront function, key group, public
key, origin access control, Secrets Manager secret, log bucket `dig-data-registry-cf-logs`, a new bucket policy on
`dig-data-registry` (read for this distribution, plus a CloudFront-wide Deny of the private prefixes `hermes sgc hcm
peg qc mskkp calr kpn-cms-assets`), an inline policy on role `dig-data-registry-permissions`, four SSM parameters
(prd/dev config and percent, percents at `0`), and a PRO plan covering the distribution and web ACL (FREE was
refused). Setup creates the plan with manual approval and never approves it: it is not billed until a person runs
`approve_paid_subscription`, and a paid plan holds to the end of the billing cycle.

## Rollback
    aws ssm put-parameter --name /data-registry/prd/cloudfront-downloads-percent --type String --value 0 --overwrite
The API re-reads settings every 60 s; links already issued work for 2 hours.

## Key rotation
The API caches the signing key for the life of the process. Rotating needs a NEW secret id (new key name and
`key_secret_id` in the config parameter) or an API restart; overwriting the existing secret changes nothing live. After
a teardown and a new setup, restart the API too (the old key stays cached).

## Teardown (`teardown_downloads_cdn`)
- Default is a dry run that lists what would go; `--yes` removes it. It refuses unless the prd percent is `0` and
  was last changed over 2 hours ago. A missing percent also refuses while the distribution exists; once the
  distribution is gone it is allowed, so an interrupted teardown can finish.
- Cancels the subscription, deletes the distribution, then the rest in dependency order. The secret has a 7-day
  recovery window. The log bucket stays unless `--delete-logs`.
- The subscription holds until the billing month ends. CloudFront may refuse to delete the distribution until
  then; the script says so and stops, and a later `--yes` run finishes the job.
- The bucket policy is deleted only if it equals the one setup wrote. Setup never overwrites another bucket policy.
