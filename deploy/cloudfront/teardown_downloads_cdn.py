"""Remove the CloudFront downloads resources that setup_downloads_cdn.py created.

    deploy/cloudfront/.venv/bin/python -m deploy.cloudfront.teardown_downloads_cdn            # dry run
    deploy/cloudfront/.venv/bin/python -m deploy.cloudfront.teardown_downloads_cdn --yes      # remove

The default is a dry run: it reads from AWS, lists what --yes would remove and changes nothing.
--yes refuses to start unless the prd rollout percent is "0" and was last changed over 2 hours ago
(signed links live 2 hours); a missing percent parameter also blocks while the distribution exists.
The log bucket is kept unless --delete-logs is also given.

Order: cancel the plan subscription; disable, wait for, and delete the distribution; delete the
origin bucket policy (only if it is ours); then the role policy, web ACL, function, key group,
public key and origin access control; then the secret (7-day recovery window) and SSM parameters.
A flat-rate plan can hold the distribution until its billing month ends; if CloudFront refuses
the disable or delete, the script says so and stops, and a later --yes run finishes the job.
"""
import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, Callable, Optional

from botocore.exceptions import ClientError

from deploy.cloudfront import cdn_config as cfg
from deploy.cloudfront.setup_downloads_cdn import (
    DISTRIBUTION_COMMENT, ScriptError, describe_function, error_code, error_text, find_distribution,
    find_key_group, find_origin_access_control, find_public_key, find_web_acl, get_or_none, make_clients,
    normalize_policy, parameter_names, same_policy, verify_account, wait_deployed)

QUIET_PERIOD = timedelta(hours=2)
SECRET_RECOVERY_DAYS = 7


def percent_blocker(value: Optional[str], modified: Optional[datetime], now: datetime,
                    distribution_exists: bool = True) -> Optional[str]:
    """Why --yes must refuse, given the prd percent parameter's value and last-modified time; else None.

    A missing parameter blocks while the distribution exists: deleting it turns the rollout off, but
    signed links already handed out keep working for up to 2 more hours. Once the distribution is gone
    a missing parameter is fine, so a half-finished teardown (which deletes the parameters last) can finish.
    """
    if value is None or modified is None:
        if not distribution_exists:
            return None
        return ("the prd rollout percent parameter is missing but the distribution still exists, so links signed "
                "earlier may still be live; recreate it at 0 (aws ssm put-parameter --name "
                f"{parameter_names('prd')[1]} --type String --value 0) and wait 2 hours")
    if value.strip() != "0":
        return f"the prd rollout percent is {value!r}, not '0'; set it to 0 and wait 2 hours (see the README)"
    modified = modified.astimezone(timezone.utc)
    quiet_since = now - modified
    if quiet_since < QUIET_PERIOD:
        return (f"the prd rollout percent was changed {int(quiet_since.total_seconds() // 60)} minutes ago; "
                f"links signed before then live 2 hours, so wait until {(modified + QUIET_PERIOD):%Y-%m-%d %H:%M} UTC")
    return None


def _allow_source_arn(policy: dict) -> Optional[str]:
    """The distribution ARN in the policy's one Allow statement; None if there is not exactly one, or it is no string.

    Only the Allow carries the ARN: bucket_policy()'s Deny of the private prefixes has no Condition, so
    reading every statement would see a missing ARN there. A list-valued SourceArn is not ours.
    """
    allows = [s for s in normalize_policy(policy)["Statement"] if s.get("Effect") == "Allow"]
    if len(allows) != 1:
        return None
    condition = allows[0].get("Condition")
    equals = condition.get("StringEquals") if isinstance(condition, dict) else None
    arn = equals.get("AWS:SourceArn") if isinstance(equals, dict) else None
    return arn if isinstance(arn, str) else None


def policy_is_ours(policy: dict, distribution_arn: Optional[str]) -> bool:
    """True if the whole `policy` equals bucket_policy() for the distribution.

    When the distribution is already gone (a re-run), the ARN is read from the policy's Allow statement,
    and must be a distribution in this account. Either way every statement must match, the Deny included.
    """
    if distribution_arn is None:
        distribution_arn = _allow_source_arn(policy)
        if distribution_arn is None or not distribution_arn.startswith(
                f"arn:aws:cloudfront::{cfg.ACCOUNT_ID}:distribution/"):
            return False
    return same_policy(policy, cfg.bucket_policy(distribution_arn))


def _empty_and_delete_bucket(s3: Any, bucket: str) -> None:
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
        objects = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if objects:
            result = s3.delete_objects(Bucket=bucket, Delete={"Objects": objects, "Quiet": True})
            if result.get("Errors"):
                raise ScriptError(f"could not delete {len(result['Errors'])} objects in {bucket}, e.g. "
                                  f"{result['Errors'][0]['Key']}: {result['Errors'][0]['Message']}")
    s3.delete_bucket(Bucket=bucket)


def _remove_distribution(cf: Any, distribution_id: str, held_note: Callable[[], str]) -> None:
    try:
        current = cf.get_distribution_config(Id=distribution_id)
        if current["DistributionConfig"]["Enabled"]:
            cf.update_distribution(Id=distribution_id, IfMatch=current["ETag"],
                                   DistributionConfig={**current["DistributionConfig"], "Enabled": False})
            print("    disabled", flush=True)
        wait_deployed(cf, distribution_id)
        etag = cf.get_distribution_config(Id=distribution_id)["ETag"]
        cf.delete_distribution(Id=distribution_id, IfMatch=etag)
    except ClientError as e:
        if error_code(e) == "NoSuchDistribution":
            return
        raise ScriptError(f"CloudFront refused to disable or delete {distribution_id}: {error_text(e)} "
                          f"({e.operation_name}). {held_note()} Nothing after this step was changed.") from None


def _is_vanished_distribution(cf: Any, arn: str) -> bool:
    """True for the ARN of a CloudFront distribution in this account that no longer exists."""
    prefix = f"arn:aws:cloudfront::{cfg.ACCOUNT_ID}:distribution/"
    return arn.startswith(prefix) and get_or_none(
        cf.get_distribution, ("NoSuchDistribution",), Id=arn[len(prefix):]) is None


def _plan(c: SimpleNamespace, delete_logs: bool, state: SimpleNamespace,
          distribution: Optional[dict]) -> list[tuple[str, bool, Callable[[], None]]]:
    """Look up everything (read-only) and return (label, present, remove) in teardown order."""
    cf, waf, s3, iam, ssm, secrets, plans = c.cf, c.waf, c.s3, c.iam, c.ssm, c.secrets, c.plans
    items: list[tuple[str, bool, Callable[[], None]]] = []
    nothing: Callable[[], None] = lambda: None

    web_acl = find_web_acl(waf, cfg.NAME)
    ours = {a for a in (distribution and distribution["ARN"], web_acl and web_acl["ARN"]) if a}

    # 1. subscriptions that cover our resources
    for page in plans.get_paginator("list_subscriptions").paginate():
        for sub in page["subscriptionSummaries"]:
            if not ours & set(sub["resourceArns"]):
                continue
            change = sub.get("scheduledChange") or {}
            if change.get("changeType") == "CANCELLATION":  # nothing would be cancelled, so nothing to protect
                state.effective = change.get("effectiveDate")
                items.append((f"subscription {sub['arn']}: cancellation already scheduled for {state.effective}",
                              False, nothing))
                continue
            others = {a for a in set(sub["resourceArns"]) - ours if not _is_vanished_distribution(cf, a)}
            if others:
                state.blockers.append(f"subscription {sub['arn']} also covers {sorted(others)}; "
                                      "cancelling it would affect them (ask the user)")
                continue

            def cancel(sub: dict = sub) -> None:
                result = plans.cancel_subscription(arn=sub["arn"], ifMatch=sub["eTag"])
                state.effective = (result["subscription"].get("scheduledChange") or {}).get("effectiveDate")
                print(f"    cancellation takes effect {state.effective} (the end of the billing period)", flush=True)

            items.append((f"subscription {sub['arn']} ({sub['planTier']}): cancel, effective at the end of the "
                          "billing period", True, cancel))

    # 2. distribution
    def held_note() -> str:
        when = f" (cancellation effective {state.effective})" if state.effective else ""
        return (f"A flat-rate plan keeps its distribution until the billing month ends{when}; "
                "re-run --yes after that to finish.")

    if distribution is not None:
        items.append((f"distribution {distribution['Id']} ({distribution['DomainName']}): disable, wait, delete",
                      True, lambda: _remove_distribution(cf, distribution["Id"], held_note)))
    else:
        items.append((f"distribution with comment {DISTRIBUTION_COMMENT!r}", False, nothing))

    # 3. origin bucket policy, only if ours
    existing = get_or_none(s3.get_bucket_policy, ("NoSuchBucketPolicy",), Bucket=cfg.ORIGIN_BUCKET)
    if existing is None:
        items.append((f"bucket policy on {cfg.ORIGIN_BUCKET}", False, nothing))
    elif policy_is_ours(json.loads(existing["Policy"]), distribution and distribution["ARN"]):
        items.append((f"bucket policy on {cfg.ORIGIN_BUCKET} (ours)", True,
                      lambda: s3.delete_bucket_policy(Bucket=cfg.ORIGIN_BUCKET)))
    else:
        items.append((f"bucket policy on {cfg.ORIGIN_BUCKET} is NOT ours: left in place", False, nothing))

    # 4. role policy, web ACL, function, key group, public key, origin access control
    role_policy = get_or_none(iam.get_role_policy, ("NoSuchEntity",),
                              RoleName=cfg.API_ROLE, PolicyName=cfg.API_ROLE_POLICY_NAME)
    items.append((f"role policy {cfg.API_ROLE_POLICY_NAME} on {cfg.API_ROLE}", role_policy is not None,
                  lambda: iam.delete_role_policy(RoleName=cfg.API_ROLE, PolicyName=cfg.API_ROLE_POLICY_NAME)))
    items.append((f"web ACL {cfg.NAME}", web_acl is not None, lambda: waf.delete_web_acl(
        Name=cfg.NAME, Scope="CLOUDFRONT", Id=web_acl["Id"], LockToken=web_acl["LockToken"])))
    function = describe_function(cf, cfg.NAME, "DEVELOPMENT")
    items.append((f"function {cfg.NAME}", function is not None, lambda: cf.delete_function(
        Name=cfg.NAME, IfMatch=describe_function(cf, cfg.NAME, "DEVELOPMENT")["ETag"])))
    key_group = find_key_group(cf, cfg.NAME)
    items.append((f"key group {cfg.NAME}", key_group is not None, lambda: cf.delete_key_group(
        Id=key_group["Id"], IfMatch=cf.get_key_group(Id=key_group["Id"])["ETag"])))
    public_key = find_public_key(cf, cfg.PUBLIC_KEY_NAME)
    items.append((f"public key {cfg.PUBLIC_KEY_NAME}", public_key is not None, lambda: cf.delete_public_key(
        Id=public_key["Id"], IfMatch=cf.get_public_key(Id=public_key["Id"])["ETag"])))
    oac = find_origin_access_control(cf, cfg.NAME)
    items.append((f"origin access control {cfg.NAME}", oac is not None, lambda: cf.delete_origin_access_control(
        Id=oac["Id"], IfMatch=cf.get_origin_access_control(Id=oac["Id"])["ETag"])))

    # 5. secret and SSM parameters
    secret = get_or_none(secrets.describe_secret, ("ResourceNotFoundException",), SecretId=cfg.SECRET_ID)
    if secret is not None and secret.get("DeletedDate"):
        items.append((f"secret {cfg.SECRET_ID}: already scheduled for deletion", False, nothing))
    else:
        items.append((f"secret {cfg.SECRET_ID} ({SECRET_RECOVERY_DAYS}-day recovery window)", secret is not None,
                      lambda: secrets.delete_secret(SecretId=cfg.SECRET_ID,
                                                    RecoveryWindowInDays=SECRET_RECOVERY_DAYS)))
    names = [n for env in ("prd", "dev") for n in parameter_names(env)]
    found = [p["Name"] for p in ssm.get_parameters(Names=names)["Parameters"]]
    items.append((f"SSM parameters {', '.join(found) or ', '.join(names)}", bool(found),
                  lambda: ssm.delete_parameters(Names=found)))

    # 6. log bucket, only on request
    if delete_logs:
        has_bucket = get_or_none(s3.head_bucket, ("404", "NoSuchBucket", "NotFound"), Bucket=cfg.LOG_BUCKET) is not None
        items.append((f"log bucket {cfg.LOG_BUCKET} and all its objects", has_bucket,
                      lambda: _empty_and_delete_bucket(s3, cfg.LOG_BUCKET)))
    return items


def run(c: SimpleNamespace, execute: bool, delete_logs: bool) -> int:
    verify_account(c)
    state = SimpleNamespace(blockers=[], effective=None)

    distribution = find_distribution(c.cf)
    _, percent_name = parameter_names("prd")
    percent = get_or_none(c.ssm.get_parameter, ("ParameterNotFound",), Name=percent_name)
    if percent is None:
        print(f"prd rollout percent: {percent_name} is absent")
        reason = percent_blocker(None, None, datetime.now(timezone.utc), distribution is not None)
    else:
        value, modified = percent["Parameter"]["Value"], percent["Parameter"]["LastModifiedDate"]
        print(f"prd rollout percent: {value!r}, last changed {modified.astimezone(timezone.utc):%Y-%m-%d %H:%M} UTC")
        reason = percent_blocker(value, modified, datetime.now(timezone.utc), distribution is not None)
    if reason:
        state.blockers.append(reason)

    items = _plan(c, delete_logs, state, distribution)
    print(("REMOVING" if execute else "DRY RUN (nothing is changed; --yes removes)") + ":")
    for label, present, _ in items:
        print(f"  {('will remove' if execute else 'would remove') if present else 'skip (nothing to do)'}: {label}")
    if not delete_logs:
        print(f"  keep: log bucket {cfg.LOG_BUCKET} (pass --delete-logs to remove it)")

    if state.blockers:
        for blocker in state.blockers:
            print(f"{'REFUSING' if execute else 'NOTE: --yes would refuse'}: {blocker}", file=sys.stderr)
        if execute:
            return 2
    if not execute:
        return 0

    for label, present, remove in items:
        if present:
            print(f"removing: {label}", flush=True)
            remove()
            print("  done", flush=True)
    print("teardown complete" + ("" if delete_logs else f"; log bucket {cfg.LOG_BUCKET} kept"))
    return 0


def main(argv: Optional[list[str]] = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m deploy.cloudfront.teardown_downloads_cdn", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="list what would be removed (the default)")
    mode.add_argument("--yes", action="store_true", help="remove the resources")
    parser.add_argument("--delete-logs", action="store_true", help="also empty and delete the log bucket")
    args = parser.parse_args(argv)
    try:
        sys.exit(run(make_clients(), execute=args.yes, delete_logs=args.delete_logs))
    except ScriptError as e:
        sys.exit(f"error: {e}")
    except ClientError as e:
        sys.exit(f"error: AWS refused a call: {error_text(e)} ({e.operation_name})")


if __name__ == "__main__":
    main()
