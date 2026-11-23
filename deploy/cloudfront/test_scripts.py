"""Offline tests for the setup, teardown and check scripts. No AWS is called: the clients are MagicMocks."""
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from dataregistry.api import cloudfront as api_cloudfront
from deploy.cloudfront import (
    cdn_config, check_filename_function, setup_downloads_cdn as setup, teardown_downloads_cdn as teardown)

DIST_ARN = "arn:aws:cloudfront::005901288866:distribution/E1ABC"


def client_error(code: str, operation: str = "Op") -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": "x"}}, operation)


def test_parameter_names_match_the_api():
    for env in ("prd", "dev"):
        assert setup.parameter_names(env) == api_cloudfront.parameter_names(env)


def test_the_apps_s3_only_prefixes_equal_the_bucket_policys_denied_prefixes():
    # The app cannot import deploy, so it keeps its own copy; this keeps the two from drifting apart.
    assert api_cloudfront.S3_ONLY_PREFIXES == cdn_config.PRIVATE_PREFIXES


@pytest.mark.parametrize("module", [setup, teardown, check_filename_function])
def test_help_makes_no_aws_calls(module, monkeypatch, capsys):
    if hasattr(module, "make_clients"):
        monkeypatch.setattr(module, "make_clients", MagicMock(side_effect=AssertionError("called AWS")))
    monkeypatch.setattr(module, "boto3", MagicMock(side_effect=AssertionError("called AWS")), raising=False)
    with pytest.raises(SystemExit) as exit_info:
        module.main(["--help"])
    assert exit_info.value.code == 0
    assert "usage:" in capsys.readouterr().out


def test_policy_comparison_ignores_scalar_versus_list_and_order():
    ours = cdn_config.bucket_policy(DIST_ARN)
    stored = json.loads(json.dumps(ours))
    stored["Statement"][0]["Action"] = ["s3:GetObject"]  # a one-element list for a scalar
    stored["Statement"][1]["Resource"].reverse()  # the Deny's prefixes in another order

    assert setup.same_policy(stored, ours)
    assert setup.normalize_policy({"Statement": {"Effect": "Deny", "Action": "s3:*", "Resource": "*"}})[
        "Statement"] == [{"Effect": "Deny", "Action": ["s3:*"], "Resource": ["*"]}]  # a lone statement object
    stored_other = json.loads(json.dumps(ours))
    stored_other["Statement"][0]["Action"] = "s3:*"
    assert not setup.same_policy(stored_other, ours)


def policy_variants() -> dict:
    """Policies that look like ours but are not, by what is different."""
    def edited(change) -> dict:
        policy = json.loads(json.dumps(cdn_config.bucket_policy(DIST_ARN)))
        change(policy)
        return policy

    old_single_statement = edited(lambda p: p["Statement"].pop())
    return {
        "old policy without the Deny": old_single_statement,
        "Deny only": edited(lambda p: p["Statement"].pop(0)),
        "Deny missing a prefix": edited(lambda p: p["Statement"][1]["Resource"].pop()),
        "Deny with an extra prefix": edited(
            lambda p: p["Statement"][1]["Resource"].append("arn:aws:s3:::dig-data-registry/other/*")),
        "Deny with another prefix": edited(
            lambda p: p["Statement"][1]["Resource"].__setitem__(0, "arn:aws:s3:::dig-data-registry/hermes-x/*")),
        "Deny on the whole bucket": edited(
            lambda p: p["Statement"][1].update(Resource="arn:aws:s3:::dig-data-registry/*")),
        "Deny limited to one distribution": edited(
            lambda p: p["Statement"][1].update(Condition={"StringEquals": {"AWS:SourceArn": DIST_ARN}})),
        "Deny for another principal": edited(lambda p: p["Statement"][1].update(Principal="*")),
        "Deny turned into Allow": edited(lambda p: p["Statement"][1].update(Effect="Allow")),
        "extra statement": edited(lambda p: p["Statement"].append(
            {"Sid": "Extra", "Effect": "Allow", "Principal": "*", "Action": "s3:GetObject", "Resource": "*"})),
        "Allow for another action": edited(lambda p: p["Statement"][0].update(Action="s3:*")),
    }


def test_the_bucket_policy_is_an_allow_for_this_distribution_then_a_deny_of_the_private_prefixes():
    policy = cdn_config.bucket_policy(DIST_ARN)

    assert [s["Sid"] for s in policy["Statement"]] == ["CloudFrontDownloadsReadOnly", "CloudFrontNeverPrivatePrefixes"]
    assert policy["Statement"][1]["Resource"] == [f"arn:aws:s3:::dig-data-registry/{p}/*" for p in (
        "hermes", "sgc", "hcm", "peg", "qc", "mskkp", "calr", "kpn-cms-assets")]
    assert "Condition" not in policy["Statement"][1]  # it must cover any distribution


def test_policy_is_ours_with_and_without_the_distribution_arn():
    ours = cdn_config.bucket_policy(DIST_ARN)
    other_account = cdn_config.bucket_policy("arn:aws:cloudfront::111111111111:distribution/E1ABC")
    foreign = {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": "*", "Action": "s3:GetObject",
                                                       "Resource": "arn:aws:s3:::dig-data-registry/*"}]}

    assert teardown.policy_is_ours(ours, DIST_ARN)
    assert teardown.policy_is_ours(ours, None)  # distribution already gone: ARN read from the policy's Allow
    assert not teardown.policy_is_ours(ours, "arn:aws:cloudfront::005901288866:distribution/E2OTHER")
    assert not teardown.policy_is_ours(other_account, None)
    assert not teardown.policy_is_ours(foreign, None)
    assert not teardown.policy_is_ours(foreign, DIST_ARN)


@pytest.mark.parametrize("variant", list(policy_variants().values()), ids=list(policy_variants()))
def test_a_policy_that_differs_in_any_statement_is_not_ours(variant):
    assert not teardown.policy_is_ours(variant, DIST_ARN)
    assert not teardown.policy_is_ours(variant, None)  # the ARN is still read, but the whole policy must match


@pytest.mark.parametrize("source_arn", [[DIST_ARN], [DIST_ARN, "arn:aws:cloudfront::005901288866:distribution/E2"],
                                        [], {"x": DIST_ARN}, None, 7, ""])
def test_a_source_arn_that_is_not_a_string_is_not_ours_and_does_not_raise(source_arn):
    policy = cdn_config.bucket_policy(DIST_ARN)
    policy["Statement"][0]["Condition"]["StringEquals"]["AWS:SourceArn"] = source_arn

    assert teardown.policy_is_ours(policy, None) is False
    del policy["Statement"][0]["Condition"]  # and no Condition at all
    assert teardown.policy_is_ours(policy, None) is False
    policy["Statement"][0]["Condition"] = ["not", "a", "dict"]
    assert teardown.policy_is_ours(policy, None) is False


def test_the_arn_is_read_from_the_allow_not_from_whatever_statement_has_one():
    """The Deny has no Condition; a stray SourceArn on another statement must not stand in for the Allow's."""
    policy = cdn_config.bucket_policy(DIST_ARN)
    del policy["Statement"][0]["Condition"]
    policy["Statement"][1]["Condition"] = {"StringEquals": {"AWS:SourceArn": DIST_ARN}}

    assert not teardown.policy_is_ours(policy, None)


def test_percent_blocker():
    now = datetime(2026, 11, 23, 12, 0, tzinfo=timezone.utc)
    quiet = now - timedelta(hours=2, minutes=1)

    assert teardown.percent_blocker("0", quiet, now) is None
    # A missing parameter blocks while the distribution exists (links may still be live), not once it is gone.
    assert "recreate it at 0" in teardown.percent_blocker(None, None, now, distribution_exists=True)
    assert "recreate it at 0" in teardown.percent_blocker(None, None, now)
    assert teardown.percent_blocker(None, None, now, distribution_exists=False) is None
    assert "not '0'" in teardown.percent_blocker("10", quiet, now)
    assert "not '0'" in teardown.percent_blocker("0.5", quiet, now)
    assert "minutes ago" in teardown.percent_blocker("0", now - timedelta(hours=1, minutes=59), now)
    assert "minutes ago" in teardown.percent_blocker("0", now, now)


def test_bucket_policy_step_puts_skips_or_refuses(capsys):
    ours = cdn_config.bucket_policy(DIST_ARN)
    s3 = MagicMock()

    s3.get_bucket_policy.side_effect = client_error("NoSuchBucketPolicy")
    setup.ensure_bucket_policy(s3, DIST_ARN)
    s3.put_bucket_policy.assert_called_once_with(Bucket="dig-data-registry", Policy=json.dumps(ours))
    put = json.loads(s3.put_bucket_policy.call_args.kwargs["Policy"])
    assert [(s["Effect"], s["Sid"]) for s in put["Statement"]] == [
        ("Allow", "CloudFrontDownloadsReadOnly"), ("Deny", "CloudFrontNeverPrivatePrefixes")]

    s3.reset_mock()
    s3.get_bucket_policy.side_effect = None
    s3.get_bucket_policy.return_value = {"Policy": json.dumps(ours)}
    setup.ensure_bucket_policy(s3, DIST_ARN)
    s3.put_bucket_policy.assert_not_called()
    assert "exists" in capsys.readouterr().out

    s3.get_bucket_policy.return_value = {"Policy": json.dumps({"Version": "2012-10-17", "Statement": [
        {"Sid": "TheirStatement", "Effect": "Allow", "Principal": "*", "Action": "s3:GetObject", "Resource": "*"}]})}
    with pytest.raises(setup.ScriptError, match="existing bucket policy.*ask the user"):
        setup.ensure_bucket_policy(s3, DIST_ARN)
    s3.put_bucket_policy.assert_not_called()


@pytest.mark.parametrize("variant", list(policy_variants().values()), ids=list(policy_variants()))
def test_setup_refuses_any_bucket_policy_that_is_not_exactly_ours_the_old_single_statement_one_included(variant):
    s3 = MagicMock()
    s3.get_bucket_policy.return_value = {"Policy": json.dumps(variant)}

    with pytest.raises(setup.ScriptError, match="existing bucket policy.*ask the user"):
        setup.ensure_bucket_policy(s3, DIST_ARN)

    s3.put_bucket_policy.assert_not_called()
    s3.delete_bucket_policy.assert_not_called()


def test_setup_skips_an_equal_policy_however_s3_spells_it():
    stored = json.loads(json.dumps(cdn_config.bucket_policy(DIST_ARN)))
    stored["Statement"][0]["Action"] = ["s3:GetObject"]
    stored["Statement"][1]["Resource"].reverse()
    s3 = MagicMock()
    s3.get_bucket_policy.return_value = {"Policy": json.dumps(stored)}

    setup.ensure_bucket_policy(s3, DIST_ARN)

    s3.put_bucket_policy.assert_not_called()


def test_percent_parameters_are_never_overwritten():
    ssm = MagicMock()
    prd_percent = setup.parameter_names("prd")[1]
    stored = {prd_percent: "25"}  # a live rollout percent; everything else is absent

    def get_parameter(Name):
        if Name not in stored:
            raise client_error("ParameterNotFound")
        return {"Parameter": {"Value": stored[Name]}}

    ssm.get_parameter.side_effect = get_parameter
    setup.ensure_parameters(ssm, "d1.cloudfront.net", "K1")

    puts = {c.kwargs["Name"]: c.kwargs for c in ssm.put_parameter.call_args_list}
    assert prd_percent not in puts  # present: not even a put attempt
    dev_percent = setup.parameter_names("dev")[1]
    assert puts[dev_percent]["Overwrite"] is False and puts[dev_percent]["Value"] == "0"  # absent: created at 0
    for env in ("prd", "dev"):
        name = setup.parameter_names(env)[0]
        assert puts[name]["Overwrite"] is True
        assert puts[name]["Value"] == cdn_config.settings_parameter_value("d1.cloudfront.net", "K1")


def test_percent_created_by_someone_else_in_the_meantime_is_left_alone():
    ssm = MagicMock()
    ssm.get_parameter.side_effect = client_error("ParameterNotFound")  # nothing exists at lookup time
    ssm.put_parameter.side_effect = lambda **kw: (
        (_ for _ in ()).throw(client_error("ParameterAlreadyExists")) if kw["Name"].endswith("-percent") else None)

    setup.ensure_parameters(ssm, "d1.cloudfront.net", "K1")  # must not raise

    percent_puts = [c.kwargs for c in ssm.put_parameter.call_args_list if c.kwargs["Name"].endswith("-percent")]
    assert percent_puts and all(p["Overwrite"] is False for p in percent_puts)


def test_secret_is_generated_stored_as_pkcs8_and_never_printed(capsys):
    stored = {}
    secrets = MagicMock()
    secrets.describe_secret.side_effect = client_error("ResourceNotFoundException")

    def create_secret(**kwargs):
        stored.update(kwargs)
        return {"ARN": "arn:aws:secretsmanager:us-east-1:005901288866:secret:x-AbCdEf"}

    secrets.create_secret.side_effect = create_secret
    secrets.get_secret_value.side_effect = lambda SecretId: {"SecretString": stored["SecretString"]}

    arn, public_pem = setup.ensure_secret(secrets)

    assert arn.endswith("x-AbCdEf")
    assert stored["Name"] == "data-registry-cloudfront-signing-key"
    assert stored["Tags"] == [{"Key": "project", "Value": "data-registry"}]
    assert stored["SecretString"].startswith("-----BEGIN PRIVATE KEY-----")  # PKCS#8
    assert public_pem.startswith("-----BEGIN PUBLIC KEY-----")
    out = capsys.readouterr()
    assert "PRIVATE KEY" not in out.out + out.err
    assert setup.same_public_key(public_pem, public_pem)


def test_filename_check_cases_and_header_parsing():
    assert len(check_filename_function.CASES) == 6
    for uri, status, _ in check_filename_function.CASES:
        event = check_filename_function.viewer_response_event(uri, status)
        assert json.loads(json.dumps(event))["request"]["uri"] == uri

    wrapped = {"FunctionOutput": json.dumps({"response": {"headers": {"content-disposition": {"value": "v"}}}})}
    bare = {"FunctionOutput": json.dumps({"headers": {"content-disposition": {"value": "v"}}})}
    absent = {"FunctionOutput": json.dumps({"response": {"headers": {}}})}
    assert check_filename_function.content_disposition(wrapped) == "v"
    assert check_filename_function.content_disposition(bare) == "v"
    assert check_filename_function.content_disposition(absent) is None
    with pytest.raises(RuntimeError):
        check_filename_function.content_disposition({"FunctionErrorMessage": "boom", "FunctionOutput": "{}"})


# --- setup fails closed on a distribution (or web ACL, or key group) that is not ours -------------------
# These use MagicMock clients, so they check the decisions the scripts make, not the AWS request shapes.

OAC_ID, KEY_GROUP_ID, KEY_ID = "OAC1", "KG1", "K1"
FUNCTION_ARN = f"arn:aws:cloudfront::{cdn_config.ACCOUNT_ID}:function/data-registry-downloads"
WEB_ACL_ARN = f"arn:aws:wafv2:us-east-1:{cdn_config.ACCOUNT_ID}:global/webacl/data-registry-downloads/abc"
CLEAN_SUMMARY = {"Id": "E1ABC", "ARN": DIST_ARN, "DomainName": "e1abc.cloudfront.net",
                 "Comment": setup.DISTRIBUTION_COMMENT}
WRITES = ("create", "put", "update", "publish", "delete", "cancel")


def clean_config() -> dict:
    return cdn_config.distribution_config("ref", OAC_ID, KEY_GROUP_ID, FUNCTION_ARN, WEB_ACL_ARN)


def pagers(**pages) -> MagicMock:
    """A get_paginator that answers each paginator name with one page."""
    client = MagicMock()
    client.get_paginator.side_effect = lambda name: MagicMock(paginate=MagicMock(return_value=[pages[name]]))
    return client


def cloudfront_with(config: dict, **more_pages) -> MagicMock:
    cf = pagers(list_distributions={"DistributionList": {"Items": [CLEAN_SUMMARY]}}, **more_pages)
    cf.get_distribution_config.return_value = {"DistributionConfig": config, "ETag": "ETAG"}
    return cf


def writes(*clients: MagicMock) -> list[str]:
    return [name for client in clients for name, _, _ in client.method_calls if name.startswith(WRITES)]


def set_behavior(**changes):
    return lambda config: config["DefaultCacheBehavior"].update(changes)


def set_origin(**changes):
    return lambda config: config["Origins"]["Items"][0].update(changes)


DRIFTS = {
    "disabled": (lambda c: c.update(Enabled=False), "Enabled"),
    "other price class": (lambda c: c.update(PriceClass="PriceClass_100"), "PriceClass"),
    "other web ACL": (lambda c: c.update(WebACLId="arn:aws:wafv2:us-east-1:1:global/webacl/x/y"), "WebACLId"),
    "no web ACL": (lambda c: c.pop("WebACLId"), "WebACLId"),
    "key group enforcement off": (
        set_behavior(TrustedKeyGroups={"Enabled": False, "Quantity": 1, "Items": [KEY_GROUP_ID]}), "TrustedKeyGroups"),
    "extra key group": (
        set_behavior(TrustedKeyGroups={"Enabled": True, "Quantity": 2, "Items": [KEY_GROUP_ID, "KG2"]}),
        "TrustedKeyGroups"),
    "other key group": (
        set_behavior(TrustedKeyGroups={"Enabled": True, "Quantity": 1, "Items": ["KG2"]}), "TrustedKeyGroups"),
    "no key group": (lambda c: c["DefaultCacheBehavior"].pop("TrustedKeyGroups"), "TrustedKeyGroups"),
    "trusted signers on": (
        set_behavior(TrustedSigners={"Enabled": True, "Quantity": 1, "Items": ["self"]}), "TrustedSigners"),
    "extra cache behavior": (lambda c: c.update(CacheBehaviors={"Quantity": 1, "Items": [{
        "PathPattern": "/public/*", "TargetOriginId": "dig-data-registry", "ViewerProtocolPolicy": "allow-all"}]}),
        "CacheBehaviors"),
    "other origin domain": (set_origin(DomainName="other-bucket.s3.us-east-1.amazonaws.com"), "origin DomainName"),
    "other OAC": (set_origin(OriginAccessControlId="OAC2"), "OriginAccessControlId"),
    "no OAC": (lambda c: c["Origins"]["Items"][0].pop("OriginAccessControlId"), "OriginAccessControlId"),
    "origin access identity": (
        set_origin(S3OriginConfig={"OriginAccessIdentity": "origin-access-identity/cloudfront/E1"}),
        "OriginAccessIdentity"),
    "custom origin": (lambda c: c["Origins"]["Items"][0].pop("S3OriginConfig"), "OriginAccessIdentity"),
    "second origin": (lambda c: c["Origins"].update(Quantity=2, Items=[*c["Origins"]["Items"], {
        "Id": "other", "DomainName": "example.com", "CustomOriginConfig": {}}]), "origins"),
    "behavior targets another origin": (set_behavior(TargetOriginId="other"), "TargetOriginId"),
    "http allowed": (set_behavior(ViewerProtocolPolicy="allow-all"), "ViewerProtocolPolicy"),
    "writes allowed": (set_behavior(AllowedMethods={"Quantity": 7, "Items": [
        "GET", "HEAD", "OPTIONS", "PUT", "POST", "PATCH", "DELETE"]}), "AllowedMethods"),
    "options allowed": (set_behavior(AllowedMethods={"Quantity": 3, "Items": ["GET", "HEAD", "OPTIONS"]}),
                        "AllowedMethods"),
    "compressing": (set_behavior(Compress=True), "Compress"),
    "other cache policy": (set_behavior(CachePolicyId="658327ea-f89d-4fab-a63d-7e88639e58f6"), "CachePolicyId"),
    "other origin request policy": (set_behavior(OriginRequestPolicyId="other"), "OriginRequestPolicyId"),
    "no function": (set_behavior(FunctionAssociations={"Quantity": 0}), "FunctionAssociations"),
    "other function": (set_behavior(FunctionAssociations={"Quantity": 1, "Items": [
        {"EventType": "viewer-response", "FunctionARN": FUNCTION_ARN + "-other"}]}), "FunctionAssociations"),
    "function on the wrong event": (set_behavior(FunctionAssociations={"Quantity": 1, "Items": [
        {"EventType": "viewer-request", "FunctionARN": FUNCTION_ARN}]}), "FunctionAssociations"),
    "second function": (set_behavior(FunctionAssociations={"Quantity": 2, "Items": [
        {"EventType": "viewer-response", "FunctionARN": FUNCTION_ARN},
        {"EventType": "viewer-request", "FunctionARN": FUNCTION_ARN + "-other"}]}), "FunctionAssociations"),
}


def test_a_clean_existing_distribution_passes_and_is_never_modified(capsys):
    cf = cloudfront_with(clean_config())

    distribution = setup.ensure_distribution(cf, OAC_ID, KEY_GROUP_ID, FUNCTION_ARN, WEB_ACL_ARN)

    assert distribution == CLEAN_SUMMARY
    assert "exists" in capsys.readouterr().out
    assert writes(cf) == []


def test_a_clean_distribution_as_cloudfront_returns_it_passes():
    """GetDistributionConfig fills defaults and omits empty members; the check must not mistake that for drift."""
    config = clean_config()
    behavior = config["DefaultCacheBehavior"]
    del config["CallerReference"], behavior["Compress"]  # absent means False
    behavior.update(SmoothStreaming=False, FieldLevelEncryptionId="", LambdaFunctionAssociations={"Quantity": 0},
                    TrustedSigners={"Enabled": False, "Quantity": 0})
    config["CacheBehaviors"] = {"Quantity": 0}
    config["Origins"]["Items"][0].update(OriginPath="", CustomHeaders={"Quantity": 0})
    cf = cloudfront_with(config)

    assert setup.ensure_distribution(cf, OAC_ID, KEY_GROUP_ID, FUNCTION_ARN, WEB_ACL_ARN) == CLEAN_SUMMARY


@pytest.mark.parametrize("mutate,field", list(DRIFTS.values()), ids=list(DRIFTS))
def test_a_drifted_existing_distribution_is_refused_and_not_modified(mutate, field):
    config = clean_config()
    mutate(config)
    cf = cloudfront_with(config)

    with pytest.raises(setup.ScriptError, match="E1ABC") as raised:
        setup.ensure_distribution(cf, OAC_ID, KEY_GROUP_ID, FUNCTION_ARN, WEB_ACL_ARN)

    assert field in str(raised.value) and "ask the user" in str(raised.value)
    assert writes(cf) == []


def test_every_drift_is_listed_in_one_refusal():
    config = clean_config()
    for mutate, _ in (DRIFTS["disabled"], DRIFTS["extra cache behavior"], DRIFTS["other OAC"]):
        mutate(config)

    with pytest.raises(setup.ScriptError) as raised:
        setup.ensure_distribution(cloudfront_with(config), OAC_ID, KEY_GROUP_ID, FUNCTION_ARN, WEB_ACL_ARN)

    assert all(word in str(raised.value) for word in ("Enabled", "CacheBehaviors", "OriginAccessControlId"))


def test_other_differences_only_warn(capsys):
    config = clean_config()
    config["HttpVersion"] = "http1.1"
    config["Logging"]["Enabled"] = False

    assert setup.ensure_distribution(cloudfront_with(config), OAC_ID, KEY_GROUP_ID, FUNCTION_ARN,
                                     WEB_ACL_ARN) == CLEAN_SUMMARY

    err = capsys.readouterr().err
    assert "WARNING" in err and "HttpVersion" in err and "Logging" in err


def stub_steps(monkeypatch, order: list, cf: MagicMock) -> SimpleNamespace:
    """setup.run with every step before the distribution stubbed out; the later steps just record themselves."""
    monkeypatch.setattr(setup, "verify_account", lambda c: None)
    monkeypatch.setattr(setup, "check_before_writing", lambda c: None)
    monkeypatch.setattr(setup, "ensure_secret", lambda secrets: ("arn:secret", "PUBLIC"))
    monkeypatch.setattr(setup, "ensure_key_pair", lambda cf_, pem: (KEY_ID, KEY_GROUP_ID))
    monkeypatch.setattr(setup, "ensure_oac", lambda cf_: OAC_ID)
    monkeypatch.setattr(setup, "ensure_function", lambda cf_: FUNCTION_ARN)
    monkeypatch.setattr(setup, "ensure_web_acl", lambda waf: WEB_ACL_ARN)
    monkeypatch.setattr(setup, "ensure_log_bucket", lambda s3: None)
    for name in ("ensure_bucket_policy", "ensure_role_policy", "ensure_parameters", "wait_deployed",
                 "ensure_subscription"):
        monkeypatch.setattr(setup, name, lambda *args, _name=name, **kwargs: order.append(_name))
    return SimpleNamespace(cf=cf, waf=MagicMock(), s3=MagicMock(), iam=MagicMock(), ssm=MagicMock(),
                           secrets=MagicMock(), plans=MagicMock(), sts=MagicMock())


def test_a_drifted_distribution_stops_setup_before_the_bucket_policy(monkeypatch, capsys):
    order: list = []
    config = clean_config()
    DRIFTS["extra key group"][0](config)
    c = stub_steps(monkeypatch, order, cloudfront_with(config))

    with pytest.raises(setup.ScriptError, match="TrustedKeyGroups"):
        setup.run(c)

    assert order == []  # no bucket policy, no role policy, no parameters, no subscription
    assert writes(c.cf, c.s3, c.iam, c.ssm) == []


def test_a_clean_distribution_lets_setup_go_on_to_the_bucket_policy(monkeypatch, capsys):
    order: list = []
    c = stub_steps(monkeypatch, order, cloudfront_with(clean_config()))

    setup.run(c)

    assert order[0] == "ensure_bucket_policy" and "ensure_subscription" in order


def test_what_setup_creates_passes_its_own_check(capsys):
    cf = pagers(list_distributions={"DistributionList": {}})
    cf.create_distribution_with_tags.return_value = {"Distribution": CLEAN_SUMMARY}
    setup.ensure_distribution(cf, OAC_ID, KEY_GROUP_ID, FUNCTION_ARN, WEB_ACL_ARN)
    sent = cf.create_distribution_with_tags.call_args.kwargs["DistributionConfigWithTags"]["DistributionConfig"]

    again = setup.ensure_distribution(cloudfront_with(sent), OAC_ID, KEY_GROUP_ID, FUNCTION_ARN, WEB_ACL_ARN)

    assert again == CLEAN_SUMMARY
    assert capsys.readouterr().err == ""  # not even a warning


def existing_web_acl(default_action: dict, rules: list) -> MagicMock:
    waf = MagicMock()
    waf.list_web_acls.return_value = {"WebACLs": [{"Name": cdn_config.NAME, "Id": "ACLID", "ARN": WEB_ACL_ARN,
                                                   "LockToken": "LOCK"}]}
    waf.get_web_acl.return_value = {"WebACL": {"Name": cdn_config.NAME, "Id": "ACLID", "ARN": WEB_ACL_ARN,
                                               "DefaultAction": default_action, "Rules": rules}}
    return waf


def test_an_existing_web_acl_with_default_allow_and_no_rules_is_reused(capsys):
    waf = existing_web_acl({"Allow": {}}, [])

    assert setup.ensure_web_acl(waf) == WEB_ACL_ARN

    waf.get_web_acl.assert_called_once_with(Name=cdn_config.NAME, Scope="CLOUDFRONT", Id="ACLID")
    assert writes(waf) == []


@pytest.mark.parametrize("default_action,rules,wanted", [
    ({"Block": {}}, [], "default action"),
    ({}, [], "default action"),
    ({"Allow": {}}, [{"Name": "rate-limit", "Priority": 0}], "rules"),
    ({"Block": {}}, [{"Name": "allow-ours", "Priority": 0}], "default action"),
])
def test_an_existing_web_acl_that_blocks_or_has_rules_is_refused(default_action, rules, wanted):
    waf = existing_web_acl(default_action, rules)

    with pytest.raises(setup.ScriptError, match=wanted) as raised:
        setup.ensure_web_acl(waf)

    assert "ask the user" in str(raised.value)
    assert writes(waf) == []


def test_a_new_web_acl_is_created_default_allow_with_no_rules():
    waf = MagicMock()
    waf.list_web_acls.return_value = {"WebACLs": []}
    waf.create_web_acl.return_value = {"Summary": {"ARN": WEB_ACL_ARN}}

    assert setup.ensure_web_acl(waf) == WEB_ACL_ARN

    sent = waf.create_web_acl.call_args.kwargs
    assert sent["DefaultAction"] == {"Allow": {}} and sent["Rules"] == []
    waf.get_web_acl.assert_not_called()


def key_pair_client(group_items):
    """CloudFront stub holding our public key (matching the secret) and a key group with `group_items`."""
    private = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public_pem = private.public_key().public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo).decode()
    cf = pagers(list_public_keys={"PublicKeyList": {"Items": [
        {"Id": KEY_ID, "Name": cdn_config.PUBLIC_KEY_NAME, "EncodedKey": public_pem}]}})
    cf.list_key_groups.return_value = {"KeyGroupList": {"Items": [{"KeyGroup": {
        "Id": KEY_GROUP_ID, "KeyGroupConfig": {"Name": cdn_config.NAME, "Items": group_items}}}]}}
    return cf, public_pem


def test_a_key_group_holding_exactly_our_key_is_reused():
    cf, public_pem = key_pair_client([KEY_ID])

    assert setup.ensure_key_pair(cf, public_pem) == (KEY_ID, KEY_GROUP_ID)
    assert writes(cf) == []


@pytest.mark.parametrize("items", [[KEY_ID, "KEXTRA"], ["KEXTRA", KEY_ID], ["KOTHER"], []])
def test_a_key_group_that_is_not_exactly_our_key_is_refused(items):
    cf, public_pem = key_pair_client(items)

    with pytest.raises(setup.ScriptError, match="exactly") as raised:
        setup.ensure_key_pair(cf, public_pem)

    assert "ask the user" in str(raised.value)
    assert writes(cf) == []


# --- the live prd rollout is never rewritten ----------------------------------------------------------

NEW_CONFIG = cdn_config.settings_parameter_value("e1abc.cloudfront.net", KEY_ID)
OLD_CONFIG = cdn_config.settings_parameter_value("eold.cloudfront.net", "KOLD")


def parameter_store(stored: dict) -> MagicMock:
    ssm = MagicMock()

    def get_parameter(Name):
        if Name not in stored:
            raise client_error("ParameterNotFound")
        return {"Parameter": {"Value": stored[Name]}}

    ssm.get_parameter.side_effect = get_parameter
    return ssm


PRD_CONFIG, PRD_PERCENT = setup.parameter_names("prd")


@pytest.mark.parametrize("percent", ["1", "25", "100", "abc", "0.5", ""])
def test_the_prd_config_is_not_rewritten_while_the_rollout_is_live(percent):
    ssm = parameter_store({PRD_CONFIG: OLD_CONFIG, PRD_PERCENT: percent})

    with pytest.raises(setup.ScriptError, match="percent") as raised:
        setup.ensure_parameters(ssm, "e1abc.cloudfront.net", KEY_ID)

    assert PRD_PERCENT in str(raised.value) and repr(percent) in str(raised.value)
    assert writes(ssm) == []  # not even the dev parameters


@pytest.mark.parametrize("percent", ["0", " 0\n"])
def test_the_prd_config_is_rewritten_when_the_rollout_is_off(percent):
    ssm = parameter_store({PRD_CONFIG: OLD_CONFIG, PRD_PERCENT: percent})

    setup.ensure_parameters(ssm, "e1abc.cloudfront.net", KEY_ID)

    puts = {c.kwargs["Name"]: c.kwargs for c in ssm.put_parameter.call_args_list}
    assert puts[PRD_CONFIG]["Value"] == NEW_CONFIG and PRD_PERCENT not in puts


def test_an_unchanged_prd_config_is_left_alone_at_any_percent():
    ssm = parameter_store({PRD_CONFIG: NEW_CONFIG, PRD_PERCENT: "25"})

    setup.ensure_parameters(ssm, "e1abc.cloudfront.net", KEY_ID)

    assert PRD_CONFIG not in {c.kwargs["Name"] for c in ssm.put_parameter.call_args_list}


def test_a_missing_prd_config_is_created_at_any_percent():
    ssm = parameter_store({PRD_PERCENT: "25"})

    setup.ensure_parameters(ssm, "e1abc.cloudfront.net", KEY_ID)

    puts = {c.kwargs["Name"]: c.kwargs for c in ssm.put_parameter.call_args_list}
    assert puts[PRD_CONFIG]["Value"] == NEW_CONFIG and PRD_PERCENT not in puts


def test_the_dev_config_is_rewritten_whatever_the_dev_percent():
    dev_config, dev_percent = setup.parameter_names("dev")
    ssm = parameter_store({dev_config: OLD_CONFIG, dev_percent: "50", PRD_CONFIG: NEW_CONFIG, PRD_PERCENT: "0"})

    setup.ensure_parameters(ssm, "e1abc.cloudfront.net", KEY_ID)

    assert {c.kwargs["Name"] for c in ssm.put_parameter.call_args_list} == {dev_config}


def run_clients(stored: dict, distribution: bool, public_key: bool) -> SimpleNamespace:
    cf = pagers(
        list_distributions={"DistributionList": {"Items": [CLEAN_SUMMARY] if distribution else []}},
        list_public_keys={"PublicKeyList": {"Items": [
            {"Id": KEY_ID, "Name": cdn_config.PUBLIC_KEY_NAME}] if public_key else []}})
    sts = MagicMock()
    sts.get_caller_identity.return_value = {"Account": cdn_config.ACCOUNT_ID}
    return SimpleNamespace(sts=sts, cf=cf, waf=MagicMock(), s3=MagicMock(), iam=MagicMock(),
                           ssm=parameter_store(stored), secrets=MagicMock(), plans=MagicMock())


@pytest.mark.parametrize("stored_config,distribution,public_key", [
    pytest.param(OLD_CONFIG, True, True, id="config names another domain and key"),
    pytest.param(NEW_CONFIG, False, True, id="distribution gone, a new domain would be created"),
    pytest.param(NEW_CONFIG, True, False, id="key gone, a new key would be created"),
])
def test_setup_refuses_before_any_write_when_the_live_prd_config_would_change(stored_config, distribution,
                                                                            public_key):
    c = run_clients({PRD_CONFIG: stored_config, PRD_PERCENT: "25"}, distribution, public_key)

    with pytest.raises(setup.ScriptError, match="25"):
        setup.run(c)

    assert writes(c.cf, c.waf, c.s3, c.iam, c.ssm, c.secrets, c.plans) == []
    assert c.secrets.method_calls == [] and c.waf.method_calls == [] and c.s3.method_calls == []


def test_setup_goes_ahead_when_the_live_prd_config_would_not_change(monkeypatch):
    class Reached(Exception):
        pass

    def step_one(secrets):
        raise Reached

    monkeypatch.setattr(setup, "ensure_secret", step_one)

    with pytest.raises(Reached):
        setup.run(run_clients({PRD_CONFIG: NEW_CONFIG, PRD_PERCENT: "25"}, distribution=True, public_key=True))


# --- the subscription step: PRO, never approved by the script ------------------------------------------

SUB_ARN = "arn:aws:pricing-plan-manager::005901288866:subscription/sub-1"


def plan_client(*subscriptions: dict) -> MagicMock:
    plans = pagers(list_subscriptions={"subscriptionSummaries": list(subscriptions)})
    plans.create_subscription.return_value = {"subscription": {
        "arn": SUB_ARN, "planTier": "PRO", "status": "PENDING_APPROVAL"}}
    return plans


def subscription(status: str, resource_arns=(DIST_ARN, WEB_ACL_ARN), **more) -> dict:
    return {"arn": SUB_ARN, "planTier": "PRO", "status": status, "resourceArns": list(resource_arns), **more}


def approval_lines(out: str) -> list[str]:
    return [line for line in out.splitlines() if "approve_paid_subscription" in line]


def test_with_no_subscription_setup_creates_a_pro_plan_awaiting_approval_and_never_approves(capsys):
    plans = plan_client(subscription("ACTIVE", resource_arns=["arn:aws:cloudfront::005901288866:distribution/OTHER"]))

    arn = setup.ensure_subscription(plans, DIST_ARN, WEB_ACL_ARN)

    assert arn == SUB_ARN
    plans.create_subscription.assert_called_once_with(
        planFamily="CloudFront", planTier="PRO", approvalMode="MANUAL", resourceArns=[DIST_ARN, WEB_ACL_ARN])
    plans.approve_paid_subscription.assert_not_called()
    out = capsys.readouterr().out
    assert "12. subscription: created (PRO, PENDING_APPROVAL)" in out
    (line,) = approval_lines(out)
    assert "not billed" in line and "end of the billing cycle" in line


def test_an_active_subscription_covering_the_distribution_is_reused(capsys):
    plans = plan_client(subscription("ACTIVE"))

    assert setup.ensure_subscription(plans, DIST_ARN, WEB_ACL_ARN) == SUB_ARN

    assert [name for name, _, _ in plans.method_calls] == ["get_paginator"]  # no create, no approve
    out = capsys.readouterr().out
    assert "12. subscription: exists (PRO, ACTIVE)" in out
    assert approval_lines(out) == []


def test_a_pending_subscription_is_reported_with_the_approval_note_not_treated_as_an_error(capsys):
    plans = plan_client(subscription("PENDING_APPROVAL"))

    assert setup.ensure_subscription(plans, DIST_ARN, WEB_ACL_ARN) == SUB_ARN

    assert [name for name, _, _ in plans.method_calls] == ["get_paginator"]
    out = capsys.readouterr().out
    assert "12. subscription: exists (PRO, PENDING_APPROVAL)" in out
    (line,) = approval_lines(out)
    assert "not billed" in line and "end of the billing cycle" in line


def test_a_failed_subscription_covering_the_distribution_is_an_error():
    plans = plan_client(subscription("FAILED", statusReason="card declined"))

    with pytest.raises(setup.ScriptError, match="FAILED.*card declined"):
        setup.ensure_subscription(plans, DIST_ARN, WEB_ACL_ARN)

    assert [name for name, _, _ in plans.method_calls] == ["get_paginator"]


def test_a_refused_pro_plan_is_reported_and_nothing_is_worked_around():
    plans = plan_client()
    plans.create_subscription.side_effect = client_error("ValidationException", "CreateSubscription")

    with pytest.raises(setup.ScriptError) as raised:
        setup.ensure_subscription(plans, DIST_ARN, WEB_ACL_ARN)

    assert "create_subscription (PRO) was refused: ValidationException: x" in str(raised.value)
    assert "No settings were changed to chase eligibility; report this to the user." in str(raised.value)
    plans.approve_paid_subscription.assert_not_called()
