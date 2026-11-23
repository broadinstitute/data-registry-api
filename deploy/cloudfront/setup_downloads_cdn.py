"""Create (or find) the AWS resources behind CloudFront signed downloads for the data registry.

Run from the repo root with the infra venv (pricing-plan-manager exists only in boto3 1.44.0):

    deploy/cloudfront/.venv/bin/python -m deploy.cloudfront.setup_downloads_cdn

Every step looks its resource up by name and reuses it, printing "exists" or "created", so the
script can be re-run after any failure. It prints DISTRIBUTION_ID=, DISTRIBUTION_ARN=, DOMAIN=,
KEY_PAIR_ID=, WEB_ACL_ARN= and SUBSCRIPTION_ARN= lines for the later rollout tasks.

The signing key is generated in memory and goes only into Secrets Manager. It is never written
to disk or printed. The values pinned for each resource live in cdn_config.py.

Setup fails closed. Before it grants the distribution read access to the origin bucket it checks that
an existing distribution, key group and web ACL are exactly what it would have built, and it refuses
(exit 1, nothing further written) on any difference. It also refuses to rewrite the prd config
parameter while the prd rollout percent is not "0".
"""
import argparse
import json
import sys
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Optional

import boto3
from botocore.exceptions import ClientError, WaiterError
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from deploy.cloudfront import cdn_config as cfg

FUNCTION_FILE = Path(__file__).with_name("filename_function.js")
FUNCTION_RUNTIME = "cloudfront-js-2.0"
PROJECT_TAG = {"Key": "project", "Value": "data-registry"}
ENVIRONMENTS = ("prd", "dev")
# cdn_config pins the comment inside distribution_config(); the lookup key is read from there.
DISTRIBUTION_COMMENT = cfg.distribution_config("", "", "", "", "")["Comment"]
WAIT_DELAY_SECONDS = 30
WAIT_MAX_ATTEMPTS = 120


class ScriptError(Exception):
    """A failure to report to the user; the scripts print it and exit non-zero."""


# --- helpers shared with the teardown and check scripts -------------------------------------


def error_code(e: ClientError) -> str:
    return e.response.get("Error", {}).get("Code", "")


def error_text(e: ClientError) -> str:
    return f"{error_code(e)}: {e.response.get('Error', {}).get('Message', '')}"


def get_or_none(call: Callable[..., Any], not_found: tuple[str, ...], **kwargs: Any) -> Optional[Any]:
    """call(**kwargs), or None when AWS says the thing does not exist (one of the `not_found` codes)."""
    try:
        return call(**kwargs)
    except ClientError as e:
        if error_code(e) in not_found:
            return None
        raise


def make_clients() -> SimpleNamespace:
    """Build every client up front, so a wrong venv fails before anything is created."""
    session = boto3.Session(region_name=cfg.REGION)
    if "pricing-plan-manager" not in session.get_available_services():
        raise ScriptError("this boto3 has no pricing-plan-manager client; use deploy/cloudfront/.venv "
                          "(boto3 1.44.0), e.g. deploy/cloudfront/.venv/bin/python -m deploy.cloudfront.<script>")
    return SimpleNamespace(
        sts=session.client("sts"), cf=session.client("cloudfront"), waf=session.client("wafv2"),
        s3=session.client("s3"), iam=session.client("iam"), ssm=session.client("ssm"),
        secrets=session.client("secretsmanager"), plans=session.client("pricing-plan-manager"))


def verify_account(c: SimpleNamespace) -> None:
    """Stop before any change if the credentials belong to another account."""
    account = c.sts.get_caller_identity()["Account"]
    if account != cfg.ACCOUNT_ID:
        raise ScriptError(f"credentials are for account {account}, expected {cfg.ACCOUNT_ID}")
    print(f"account {account}, region {cfg.REGION}", flush=True)


def parameter_names(env: str) -> tuple[str, str]:
    """The SSM parameter names (config JSON, percent) for `env`.

    Same names as dataregistry.api.cloudfront.parameter_names, which this venv cannot import
    (it needs fastapi); test_scripts.py pins the two together.
    """
    return f"/data-registry/{env}/cloudfront-downloads", f"/data-registry/{env}/cloudfront-downloads-percent"


def normalize_policy(policy: dict) -> dict:
    """A policy in a canonical shape, so a stored policy compares equal to the one we built.

    Statement is a list and Action/NotAction/Resource/NotResource are sorted lists, whichever form
    the service handed back.
    """
    statements = policy.get("Statement", [])
    if isinstance(statements, dict):
        statements = [statements]
    out = []
    for statement in statements:
        statement = dict(statement)
        for key in ("Action", "NotAction", "Resource", "NotResource"):
            if key in statement:
                values = [statement[key]] if isinstance(statement[key], str) else statement[key]
                statement[key] = sorted(values)
        out.append(statement)
    return {**policy, "Statement": out}


def same_policy(a: dict, b: dict) -> bool:
    return normalize_policy(a) == normalize_policy(b)


def find_distribution(cf: Any, comment: str = DISTRIBUTION_COMMENT) -> Optional[dict]:
    matches = [d for page in cf.get_paginator("list_distributions").paginate()
               for d in page["DistributionList"].get("Items", []) if d["Comment"] == comment]
    if len(matches) > 1:
        raise ScriptError(f"{len(matches)} distributions have the comment {comment!r}: "
                          f"{', '.join(d['Id'] for d in matches)}; ask the user which one is ours")
    return matches[0] if matches else None


def find_public_key(cf: Any, name: str) -> Optional[dict]:
    for page in cf.get_paginator("list_public_keys").paginate():
        for key in page["PublicKeyList"].get("Items", []):
            if key["Name"] == name:
                return key
    return None


def find_key_group(cf: Any, name: str) -> Optional[dict]:
    kwargs: dict = {}
    while True:
        listing = cf.list_key_groups(**kwargs)["KeyGroupList"]
        for item in listing.get("Items", []):
            if item["KeyGroup"]["KeyGroupConfig"]["Name"] == name:
                return item["KeyGroup"]
        if not listing.get("NextMarker"):
            return None
        kwargs["Marker"] = listing["NextMarker"]


def find_origin_access_control(cf: Any, name: str) -> Optional[dict]:
    for page in cf.get_paginator("list_origin_access_controls").paginate():
        for oac in page["OriginAccessControlList"].get("Items", []):
            if oac["Name"] == name:
                return oac
    return None


def describe_function(cf: Any, name: str, stage: str) -> Optional[dict]:
    return get_or_none(cf.describe_function, ("NoSuchFunctionExists",), Name=name, Stage=stage)


def find_web_acl(waf: Any, name: str) -> Optional[dict]:
    kwargs: dict = {"Scope": "CLOUDFRONT", "Limit": 100}
    while True:
        listing = waf.list_web_acls(**kwargs)
        for acl in listing["WebACLs"]:
            if acl["Name"] == name:
                return acl
        if not listing.get("NextMarker") or not listing["WebACLs"]:
            return None
        kwargs["NextMarker"] = listing["NextMarker"]


def wait_deployed(cf: Any, distribution_id: str) -> None:
    print(f"    waiting for distribution {distribution_id} to reach Deployed (several minutes)", flush=True)
    try:
        cf.get_waiter("distribution_deployed").wait(
            Id=distribution_id, WaiterConfig={"Delay": WAIT_DELAY_SECONDS, "MaxAttempts": WAIT_MAX_ATTEMPTS})
    except WaiterError as e:
        raise ScriptError(f"distribution {distribution_id} did not reach Deployed in time ({e}); "
                          "re-run once it has") from None


def say(step: int, what: str, state: str) -> None:
    print(f"{step:>2}. {what}: {state}", flush=True)


# --- steps ----------------------------------------------------------------------------------


def _secret_pem(secrets: Any) -> bytes:
    response = secrets.get_secret_value(SecretId=cfg.SECRET_ID)
    if "SecretString" in response:
        return response["SecretString"].encode()
    return bytes(response["SecretBinary"])


def _public_pem(private_pem: bytes) -> str:
    """The SubjectPublicKeyInfo PEM for a stored RSA private key. Never echoes the key in errors."""
    try:
        key = serialization.load_pem_private_key(private_pem, password=None)
    except (ValueError, TypeError):
        raise ScriptError(f"secret {cfg.SECRET_ID} does not hold an unencrypted PEM private key") from None
    if not isinstance(key, rsa.RSAPrivateKey):
        raise ScriptError(f"secret {cfg.SECRET_ID} does not hold an RSA key")
    return key.public_key().public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo).decode()


def same_public_key(pem_a: str, pem_b: str) -> bool:
    def der(pem: str) -> bytes:
        return serialization.load_pem_public_key(pem.encode()).public_bytes(
            serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
    try:
        return der(pem_a) == der(pem_b)
    except ValueError:
        return False


def ensure_secret(secrets: Any) -> tuple[str, str]:
    """Step 1. Returns (secret ARN, public key PEM derived from the stored private key)."""
    described = get_or_none(secrets.describe_secret, ("ResourceNotFoundException",), SecretId=cfg.SECRET_ID)
    if described is not None and described.get("DeletedDate"):
        raise ScriptError(f"secret {cfg.SECRET_ID} is scheduled for deletion; restore it with "
                          f"`aws secretsmanager restore-secret --secret-id {cfg.SECRET_ID}` "
                          "or wait out the recovery window, then re-run")
    if described is None:
        private_pem = rsa.generate_private_key(public_exponent=65537, key_size=2048).private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption()).decode()
        arn = secrets.create_secret(
            Name=cfg.SECRET_ID, Description="Private key for signed CloudFront download links (data-registry)",
            SecretString=private_pem, Tags=[PROJECT_TAG])["ARN"]
        state = "created"
    else:
        arn, state = described["ARN"], "exists"
    say(1, f"secret {cfg.SECRET_ID}", state)
    return arn, _public_pem(_secret_pem(secrets))


def ensure_key_pair(cf: Any, public_pem: str) -> tuple[str, str]:
    """Step 2. Returns (public key id = KEY_PAIR_ID, key group id)."""
    key = find_public_key(cf, cfg.PUBLIC_KEY_NAME)
    if key is not None:
        if not same_public_key(key["EncodedKey"], public_pem):
            raise ScriptError(f"CloudFront public key {cfg.PUBLIC_KEY_NAME} ({key['Id']}) is not the public half "
                              f"of secret {cfg.SECRET_ID}; ask the user (rotation needs a new key name and secret)")
        key_id, state = key["Id"], "exists"
    else:
        key_id = cf.create_public_key(PublicKeyConfig={
            "CallerReference": f"{cfg.PUBLIC_KEY_NAME}-{uuid.uuid4()}", "Name": cfg.PUBLIC_KEY_NAME,
            "EncodedKey": public_pem, "Comment": "data-registry downloads signing key"})["PublicKey"]["Id"]
        state = "created"
    say(2, f"public key {cfg.PUBLIC_KEY_NAME}", f"{state} ({key_id})")

    group = find_key_group(cf, cfg.NAME)
    if group is not None:
        if group["KeyGroupConfig"]["Items"] != [key_id]:
            # Any other key in the group could sign links the distribution would honour.
            raise ScriptError(f"key group {cfg.NAME} ({group['Id']}) holds {group['KeyGroupConfig']['Items']}, "
                              f"expected exactly [{key_id!r}]; ask the user")
        group_id, state = group["Id"], "exists"
    else:
        group_id = cf.create_key_group(KeyGroupConfig={
            "Name": cfg.NAME, "Items": [key_id], "Comment": "data-registry downloads"})["KeyGroup"]["Id"]
        state = "created"
    say(2, f"key group {cfg.NAME}", f"{state} ({group_id})")
    return key_id, group_id


def ensure_oac(cf: Any) -> str:
    """Step 3. Returns the origin access control id."""
    wanted = {"SigningProtocol": "sigv4", "SigningBehavior": "always", "OriginAccessControlOriginType": "s3"}
    oac = find_origin_access_control(cf, cfg.NAME)
    if oac is not None:
        differs = {k: oac.get(k) for k, v in wanted.items() if oac.get(k) != v}
        if differs:
            raise ScriptError(f"origin access control {cfg.NAME} ({oac['Id']}) has {differs}, "
                              f"expected {wanted}; ask the user")
        oac_id, state = oac["Id"], "exists"
    else:
        oac_id = cf.create_origin_access_control(OriginAccessControlConfig={
            "Name": cfg.NAME, "Description": "data-registry downloads", **wanted})["OriginAccessControl"]["Id"]
        state = "created"
    say(3, f"origin access control {cfg.NAME}", f"{state} ({oac_id})")
    return oac_id


def _same_code(a: bytes, b: bytes) -> bool:
    """Function code compared without caring about line-ending or trailing-whitespace normalisation."""
    return a.replace(b"\r\n", b"\n").strip() == b.replace(b"\r\n", b"\n").strip()


def _stage_code(cf: Any, stage: str) -> bytes:
    return cf.get_function(Name=cfg.NAME, Stage=stage)["FunctionCode"].read()


def ensure_function(cf: Any) -> str:
    """Step 4. Make DEVELOPMENT and LIVE match filename_function.js. Returns the function ARN."""
    code = FUNCTION_FILE.read_bytes()
    function_config = {"Comment": "Sets Content-Disposition on data-registry downloads", "Runtime": FUNCTION_RUNTIME}
    state = "exists"
    development = describe_function(cf, cfg.NAME, "DEVELOPMENT")
    if development is None:
        cf.create_function(Name=cfg.NAME, FunctionConfig=function_config, FunctionCode=code)
        state = "created"
    elif (not _same_code(_stage_code(cf, "DEVELOPMENT"), code)
          or development["FunctionSummary"]["FunctionConfig"]["Runtime"] != FUNCTION_RUNTIME):
        cf.update_function(Name=cfg.NAME, IfMatch=development["ETag"], FunctionConfig=function_config,
                           FunctionCode=code)
        state = "updated"
    live = describe_function(cf, cfg.NAME, "LIVE")
    if state != "exists" or live is None or not _same_code(_stage_code(cf, "LIVE"), code):
        etag = describe_function(cf, cfg.NAME, "DEVELOPMENT")["ETag"]
        cf.publish_function(Name=cfg.NAME, IfMatch=etag)
        state = f"{state}, published to LIVE" if state != "exists" else "published to LIVE"
    arn = describe_function(cf, cfg.NAME, "LIVE")["FunctionSummary"]["FunctionMetadata"]["FunctionARN"]
    say(4, f"function {cfg.NAME}", state)
    return arn


def ensure_web_acl(waf: Any) -> str:
    """Step 5. Returns the web ACL ARN."""
    acl = find_web_acl(waf, cfg.NAME)
    if acl is not None:
        detail = waf.get_web_acl(Name=cfg.NAME, Scope="CLOUDFRONT", Id=acl["Id"])["WebACL"]
        differences = []
        if set(detail.get("DefaultAction") or {}) != {"Allow"}:
            differences.append(f"its default action is {sorted(detail.get('DefaultAction') or {})}, expected Allow")
        if detail.get("Rules"):
            differences.append(f"it has {len(detail['Rules'])} rules, expected none")
        if differences:
            raise ScriptError(f"web ACL {cfg.NAME} ({acl['Id']}) is not the empty allow-all ACL setup creates: "
                              f"{'; '.join(differences)}; it would sit in front of the distribution; ask the user")
        arn, state = acl["ARN"], "exists"
    else:
        arn = waf.create_web_acl(
            Name=cfg.NAME, Scope="CLOUDFRONT", DefaultAction={"Allow": {}}, Rules=[],
            Description="data-registry downloads, no rules yet",
            VisibilityConfig={"SampledRequestsEnabled": True, "CloudWatchMetricsEnabled": True,
                              "MetricName": cfg.NAME})["Summary"]["ARN"]
        state = "created"
    say(5, f"web ACL {cfg.NAME}", state)
    return arn


def ensure_log_bucket(s3: Any) -> None:
    """Step 6. The bucket, then each setting that is missing or different."""
    bucket = cfg.LOG_BUCKET
    try:
        s3.head_bucket(Bucket=bucket)
        created = False
    except ClientError as e:
        if error_code(e) not in ("404", "NoSuchBucket", "NotFound"):
            raise ScriptError(f"bucket {bucket} exists but cannot be used ({error_text(e)}); ask the user") from None
        s3.create_bucket(Bucket=bucket)  # us-east-1: no LocationConstraint
        created = True

    applied = []

    # Legacy CloudFront logging writes through ACLs, which BucketOwnerEnforced (the default) disables.
    ownership = get_or_none(s3.get_bucket_ownership_controls, ("OwnershipControlsNotFoundError",), Bucket=bucket)
    if ownership is None or [r["ObjectOwnership"] for r in ownership["OwnershipControls"]["Rules"]] != [
            "BucketOwnerPreferred"]:
        s3.put_bucket_ownership_controls(
            Bucket=bucket, OwnershipControls={"Rules": [{"ObjectOwnership": "BucketOwnerPreferred"}]})
        applied.append("ownership controls")

    public_access = get_or_none(s3.get_public_access_block, ("NoSuchPublicAccessBlockConfiguration",), Bucket=bucket)
    if public_access is None or not all(public_access["PublicAccessBlockConfiguration"].get(k) is True for k in (
            "BlockPublicAcls", "IgnorePublicAcls", "BlockPublicPolicy", "RestrictPublicBuckets")):
        s3.put_public_access_block(Bucket=bucket, PublicAccessBlockConfiguration={
            "BlockPublicAcls": True, "IgnorePublicAcls": True, "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True})
        applied.append("public access block")

    encryption = get_or_none(s3.get_bucket_encryption, ("ServerSideEncryptionConfigurationNotFoundError",),
                             Bucket=bucket)
    if encryption is None or not any(
            r.get("ApplyServerSideEncryptionByDefault", {}).get("SSEAlgorithm") == "AES256"
            for r in encryption["ServerSideEncryptionConfiguration"]["Rules"]):
        s3.put_bucket_encryption(Bucket=bucket, ServerSideEncryptionConfiguration={
            "Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]})
        applied.append("SSE-S3")

    lifecycle = get_or_none(s3.get_bucket_lifecycle_configuration, ("NoSuchLifecycleConfiguration",), Bucket=bucket)
    if lifecycle is None or not any(
            r["Status"] == "Enabled" and r.get("Expiration", {}).get("Days") == cfg.LOG_RETENTION_DAYS
            and not r.get("Filter", {}).get("Prefix") and set(r.get("Filter", {})) <= {"Prefix"}
            for r in lifecycle["Rules"]):
        s3.put_bucket_lifecycle_configuration(Bucket=bucket, LifecycleConfiguration={"Rules": [{
            "ID": "expire-access-logs", "Status": "Enabled", "Filter": {"Prefix": ""},
            "Expiration": {"Days": cfg.LOG_RETENTION_DAYS},
            "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 7}}]})
        applied.append(f"{cfg.LOG_RETENTION_DAYS}-day expiry")

    state = "created" if created else "exists"
    say(6, f"log bucket {bucket}", f"{state}" + (f"; applied {', '.join(applied)}" if applied else ""))


def distribution_differences(config: dict, oac_id: str, key_group_id: str, function_arn: str,
                             web_acl_arn: str) -> tuple[list[str], list[str]]:
    """Compare an existing distribution's config with the one setup builds: (differences, warnings).

    The differences are everything that decides who may fetch from the origin bucket and how: they refuse
    the run. The warnings are settings that make a distribution less useful but no less safe.
    """
    wanted = cfg.distribution_config("", oac_id, key_group_id, function_arn, web_acl_arn)
    want_origin, want_behavior = wanted["Origins"]["Items"][0], wanted["DefaultCacheBehavior"]
    behavior = config.get("DefaultCacheBehavior") or {}
    differences: list[str] = []

    def need(what: str, actual: Any, expected: Any) -> None:
        if actual != expected:
            differences.append(f"{what} is {actual!r}, expected {expected!r}")

    need("Enabled", config.get("Enabled"), True)
    need("PriceClass", config.get("PriceClass"), wanted["PriceClass"])
    need("WebACLId", config.get("WebACLId"), web_acl_arn)

    origins = (config.get("Origins") or {}).get("Items") or []
    if len(origins) != 1:
        differences.append(f"it has {len(origins)} origins, expected exactly 1")
    else:
        origin = origins[0]
        need("origin DomainName", origin.get("DomainName"), want_origin["DomainName"])
        need("origin OriginAccessControlId", origin.get("OriginAccessControlId"), oac_id)
        need("origin S3OriginConfig.OriginAccessIdentity",
             (origin.get("S3OriginConfig") or {}).get("OriginAccessIdentity"), "")
        need("DefaultCacheBehavior.TargetOriginId", behavior.get("TargetOriginId"), origin.get("Id"))
    extra_behaviors = config.get("CacheBehaviors") or {}
    if extra_behaviors.get("Quantity") or extra_behaviors.get("Items"):
        differences.append(f"CacheBehaviors has {extra_behaviors.get('Quantity') or len(extra_behaviors['Items'])} "
                           "entries, expected none")

    key_groups = behavior.get("TrustedKeyGroups") or {}
    need("TrustedKeyGroups.Enabled", key_groups.get("Enabled"), True)
    need("TrustedKeyGroups.Items", key_groups.get("Items"), [key_group_id])
    if (behavior.get("TrustedSigners") or {}).get("Enabled"):
        differences.append("TrustedSigners is enabled, expected it off")
    for name in ("ViewerProtocolPolicy", "CachePolicyId", "OriginRequestPolicyId"):
        need(f"DefaultCacheBehavior.{name}", behavior.get(name), want_behavior[name])
    need("DefaultCacheBehavior.Compress", behavior.get("Compress", False), want_behavior["Compress"])
    need("AllowedMethods", sorted((behavior.get("AllowedMethods") or {}).get("Items") or []),
         sorted(want_behavior["AllowedMethods"]["Items"]))
    need("FunctionAssociations",
         [(a.get("EventType"), a.get("FunctionARN")) for a in (behavior.get("FunctionAssociations") or {}).get(
             "Items") or []],
         [(a["EventType"], a["FunctionARN"]) for a in want_behavior["FunctionAssociations"]["Items"]])

    warnings: list[str] = []
    logging, want_logging = config.get("Logging") or {}, wanted["Logging"]
    for name in ("Enabled", "Bucket", "Prefix"):
        if logging.get(name) != want_logging[name]:
            warnings.append(f"Logging.{name} is {logging.get(name)!r}, setup builds {want_logging[name]!r}")
    for name in ("HttpVersion", "IsIPV6Enabled"):
        if config.get(name) != wanted[name]:
            warnings.append(f"{name} is {config.get(name)!r}, setup builds {wanted[name]!r}")
    if origins and origins[0].get("OriginPath"):
        warnings.append(f"origin OriginPath is {origins[0]['OriginPath']!r}, setup builds none")
    if (behavior.get("LambdaFunctionAssociations") or {}).get("Items"):
        warnings.append("DefaultCacheBehavior has Lambda@Edge associations, setup builds none")
    return differences, warnings


def verify_distribution(cf: Any, distribution: dict, oac_id: str, key_group_id: str, function_arn: str,
                        web_acl_arn: str) -> None:
    """Raise ScriptError unless an existing distribution is the one setup would build.

    It is found by comment alone, and the next step grants it read access to the origin bucket, so a
    lookalike (or ours, edited since) must stop the run here. The distribution is not modified.
    """
    config = cf.get_distribution_config(Id=distribution["Id"])["DistributionConfig"]
    differences, warnings = distribution_differences(config, oac_id, key_group_id, function_arn, web_acl_arn)
    for warning in warnings:
        print(f"    WARNING: distribution {distribution['Id']}: {warning}; not modified", file=sys.stderr, flush=True)
    if differences:
        raise ScriptError(f"existing distribution {distribution['Id']} ({distribution['DomainName']}) is not what "
                          f"setup builds, so it is not given read access to {cfg.ORIGIN_BUCKET}; the distribution was "
                          f"not modified. Differences: {'; '.join(differences)}; ask the user")


def ensure_distribution(cf: Any, oac_id: str, key_group_id: str, function_arn: str, web_acl_arn: str) -> dict:
    """Step 7. Returns the distribution summary (Id, ARN, DomainName, ...)."""
    distribution = find_distribution(cf)
    if distribution is not None:
        verify_distribution(cf, distribution, oac_id, key_group_id, function_arn, web_acl_arn)
        state = "exists"
    else:
        # A fresh CallerReference per creation; a re-run finds the distribution by its comment instead.
        config = cfg.distribution_config(f"{cfg.NAME}-{uuid.uuid4()}", oac_id, key_group_id, function_arn,
                                         web_acl_arn)
        distribution = cf.create_distribution_with_tags(DistributionConfigWithTags={
            "DistributionConfig": config,
            "Tags": {"Items": [{"Key": "Name", "Value": cfg.NAME}, PROJECT_TAG]}})["Distribution"]
        state = "created"
    say(7, f"distribution {distribution['Id']}", f"{state} ({distribution['DomainName']})")
    return distribution


def ensure_bucket_policy(s3: Any, distribution_arn: str) -> None:
    """Step 8. Put ours if the bucket has no policy; leave ours; never touch anyone else's."""
    ours = cfg.bucket_policy(distribution_arn)
    existing = get_or_none(s3.get_bucket_policy, ("NoSuchBucketPolicy",), Bucket=cfg.ORIGIN_BUCKET)
    if existing is None:
        s3.put_bucket_policy(Bucket=cfg.ORIGIN_BUCKET, Policy=json.dumps(ours))
        state = "created"
    elif same_policy(json.loads(existing["Policy"]), ours):
        state = "exists"
    else:
        sids = [s.get("Sid", "(no Sid)") for s in normalize_policy(json.loads(existing["Policy"]))["Statement"]]
        raise ScriptError(f"existing bucket policy on {cfg.ORIGIN_BUCKET} (statements: {', '.join(sids)}); "
                          "ask the user")
    say(8, f"bucket policy on {cfg.ORIGIN_BUCKET}", state)


def ensure_role_policy(iam: Any, secret_arn: str) -> None:
    """Step 9. The inline policy is ours by name; it is rewritten if the secret ARN changed."""
    ours = cfg.api_role_policy(secret_arn)
    existing = get_or_none(iam.get_role_policy, ("NoSuchEntity",),
                           RoleName=cfg.API_ROLE, PolicyName=cfg.API_ROLE_POLICY_NAME)
    if existing is not None and same_policy(existing["PolicyDocument"], ours):
        state = "exists"
    else:
        iam.put_role_policy(RoleName=cfg.API_ROLE, PolicyName=cfg.API_ROLE_POLICY_NAME,
                            PolicyDocument=json.dumps(ours))
        state = "created" if existing is None else "updated"
    say(9, f"role policy {cfg.API_ROLE_POLICY_NAME} on {cfg.API_ROLE}", state)


def refuse_to_rewrite_live_rollout(stored_config: Optional[str], stored_percent: Optional[str],
                                   new_config: Optional[str]) -> None:
    """Raise ScriptError if the prd config would change while the prd percent is not "0".

    Rewriting the config under a live rollout switches the share of downloads already on CloudFront to
    another domain or key. `stored_*` are the current values (None = the parameter does not exist) and
    `new_config` is what setup would write (None = something different from any stored value).
    """
    if stored_config is None or stored_percent is None or stored_config == new_config:
        return
    if stored_percent.strip() != "0":
        config_name, percent_name = parameter_names("prd")
        raise ScriptError(f"{percent_name} is {stored_percent!r}, not '0', and {config_name} would change; "
                          "rewriting it would move links already on CloudFront to another domain or key. "
                          "The parameters were not changed. Set the percent to 0 and wait 2 hours, or ask the user")


def check_before_writing(c: SimpleNamespace) -> None:
    """Read-only refusals that must come before the first write, so a refusal leaves nothing half-done."""
    config_name, percent_name = parameter_names("prd")
    stored_config = get_or_none(c.ssm.get_parameter, ("ParameterNotFound",), Name=config_name)
    stored_percent = get_or_none(c.ssm.get_parameter, ("ParameterNotFound",), Name=percent_name)
    if stored_config is None or stored_percent is None:
        return
    distribution, key = find_distribution(c.cf), find_public_key(c.cf, cfg.PUBLIC_KEY_NAME)
    # With no distribution or key yet, setup would make new ones, so the stored config would differ.
    new_config = (cfg.settings_parameter_value(distribution["DomainName"], key["Id"])
                  if distribution is not None and key is not None else None)
    refuse_to_rewrite_live_rollout(stored_config["Parameter"]["Value"], stored_percent["Parameter"]["Value"],
                                   new_config)


def ensure_parameters(ssm: Any, domain: str, key_pair_id: str) -> None:
    """Step 10. The config parameter is overwritten; the percent is created at "0" and never overwritten.

    The prd config is not overwritten while the prd percent is live, and nothing is written in that case.
    """
    value = cfg.settings_parameter_value(domain, key_pair_id)
    stored = {name: get_or_none(ssm.get_parameter, ("ParameterNotFound",), Name=name)
              for env in ENVIRONMENTS for name in parameter_names(env)}

    def stored_value(name: str) -> Optional[str]:
        return None if stored[name] is None else stored[name]["Parameter"]["Value"]

    prd_config_name, prd_percent_name = parameter_names("prd")
    refuse_to_rewrite_live_rollout(stored_value(prd_config_name), stored_value(prd_percent_name), value)

    for env in ENVIRONMENTS:
        config_name, percent_name = parameter_names(env)
        current = stored[config_name]
        if current is not None and current["Parameter"]["Value"] == value:
            state = "exists"
        else:
            ssm.put_parameter(Name=config_name, Value=value, Type="String", Overwrite=True)
            state = "created" if current is None else "updated"
        say(10, config_name, state)

        percent = stored[percent_name]
        if percent is None:
            try:
                ssm.put_parameter(Name=percent_name, Value="0", Type="String", Overwrite=False)
                state = "created (0)"
            except ClientError as e:  # created by someone else since the lookup
                if error_code(e) != "ParameterAlreadyExists":
                    raise
                state = "exists (left unchanged)"
        else:
            state = f"exists (value {percent['Parameter']['Value']}, left unchanged)"
        say(10, percent_name, state)


def ensure_subscription(plans: Any, distribution_arn: str, web_acl_arn: str) -> str:
    """Step 12. A FREE plan if nothing covers the distribution. Any refusal is reported, not worked around."""
    for page in plans.get_paginator("list_subscriptions").paginate():
        for sub in page["subscriptionSummaries"]:
            if distribution_arn not in sub["resourceArns"]:
                continue
            if sub["status"] == "FAILED":
                raise ScriptError(f"subscription {sub['arn']} covers the distribution but is FAILED "
                                  f"({sub.get('statusReason', 'no reason given')}); ask the user")
            pending = sub.get("scheduledChange", {}).get("changeType")
            say(12, "subscription", f"exists ({sub['planTier']}, {sub['status']}"
                                    + (f", {pending} scheduled" if pending else "") + ")")
            return sub["arn"]
    try:
        created = plans.create_subscription(planFamily="CloudFront", planTier="FREE", approvalMode="IMMEDIATE",
                                            resourceArns=[distribution_arn, web_acl_arn])
    except ClientError as e:
        raise ScriptError(f"create_subscription (FREE) was refused: {error_text(e)}\n"
                          "No settings were changed to chase eligibility; report this to the user. "
                          "If it is a limit on Free plans, Task 6 creates PRO directly.") from None
    sub = created["subscription"]
    say(12, "subscription", f"created ({sub['planTier']}, {sub['status']})")
    return sub["arn"]


def run(c: SimpleNamespace) -> None:
    verify_account(c)
    check_before_writing(c)
    secret_arn, public_pem = ensure_secret(c.secrets)
    key_pair_id, key_group_id = ensure_key_pair(c.cf, public_pem)
    oac_id = ensure_oac(c.cf)
    function_arn = ensure_function(c.cf)
    web_acl_arn = ensure_web_acl(c.waf)
    ensure_log_bucket(c.s3)
    distribution = ensure_distribution(c.cf, oac_id, key_group_id, function_arn, web_acl_arn)
    ensure_bucket_policy(c.s3, distribution["ARN"])
    ensure_role_policy(c.iam, secret_arn)
    ensure_parameters(c.ssm, distribution["DomainName"], key_pair_id)
    wait_deployed(c.cf, distribution["Id"])
    say(11, f"distribution {distribution['Id']}", "Deployed")
    print(f"DISTRIBUTION_ID={distribution['Id']}")
    print(f"DISTRIBUTION_ARN={distribution['ARN']}")
    print(f"DOMAIN={distribution['DomainName']}")
    print(f"KEY_PAIR_ID={key_pair_id}")
    print(f"WEB_ACL_ARN={web_acl_arn}", flush=True)
    print(f"SUBSCRIPTION_ARN={ensure_subscription(c.plans, distribution['ARN'], web_acl_arn)}")


def main(argv: Optional[list[str]] = None) -> None:
    argparse.ArgumentParser(prog="python -m deploy.cloudfront.setup_downloads_cdn",
                            description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
                            ).parse_args(argv)
    try:
        run(make_clients())
    except ScriptError as e:
        sys.exit(f"error: {e}")
    except ClientError as e:
        sys.exit(f"error: AWS refused a call: {error_text(e)} ({e.operation_name})")


if __name__ == "__main__":
    main()
