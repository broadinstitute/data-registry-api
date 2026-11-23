"""Tests for the pure AWS config builders. No AWS, no database."""
from dataregistry.api.cloudfront import parse_settings
from deploy.cloudfront import cdn_config
from deploy.cloudfront.cdn_config import (
    api_role_policy, bucket_policy, distribution_config, settings_parameter_value)


def test_distribution_config_pins():
    c = distribution_config("ref", "oac1", "kg1", "arn:fn", "arn:waf")

    assert c["CallerReference"] == "ref"
    assert c["Comment"] == "data-registry-downloads (stream_file)"
    assert c["Enabled"] is True
    assert c["IsIPV6Enabled"] is True
    assert c["HttpVersion"] == "http2"
    assert c["PriceClass"] == "PriceClass_All"
    assert c["WebACLId"] == "arn:waf"
    assert c["ViewerCertificate"] == {"CloudFrontDefaultCertificate": True}
    assert c["Logging"] == {"Enabled": True, "IncludeCookies": False,
                            "Bucket": "dig-data-registry-cf-logs.s3.amazonaws.com", "Prefix": "downloads/"}

    assert c["Origins"]["Quantity"] == 1
    origin = c["Origins"]["Items"][0]
    assert origin["Id"] == "dig-data-registry"
    assert origin["DomainName"] == "dig-data-registry.s3.us-east-1.amazonaws.com"
    assert origin["S3OriginConfig"] == {"OriginAccessIdentity": ""}
    assert origin["OriginAccessControlId"] == "oac1"

    behavior = c["DefaultCacheBehavior"]
    assert behavior["TargetOriginId"] == "dig-data-registry"
    assert behavior["ViewerProtocolPolicy"] == "redirect-to-https"
    assert behavior["AllowedMethods"] == {"Quantity": 2, "Items": ["GET", "HEAD"],
                                          "CachedMethods": {"Quantity": 2, "Items": ["GET", "HEAD"]}}
    assert behavior["Compress"] is False
    assert behavior["CachePolicyId"] == "4135ea2d-6df8-44a3-9df3-4b5a84be39ad"
    assert behavior["OriginRequestPolicyId"] == "88a5eaf4-2fd4-4709-b370-b4c650ea3fcf"
    assert behavior["TrustedKeyGroups"] == {"Enabled": True, "Quantity": 1, "Items": ["kg1"]}
    assert behavior["FunctionAssociations"] == {
        "Quantity": 1, "Items": [{"EventType": "viewer-response", "FunctionARN": "arn:fn"}]}
    # A trusted key group is what makes links signed-only; trusted signers (account keys) must not be set.
    assert "TrustedSigners" not in behavior


def test_bucket_policy_allows_only_this_distribution_and_denies_the_private_prefixes():
    arn = "arn:aws:cloudfront::005901288866:distribution/E1"

    assert bucket_policy(arn) == {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "CloudFrontDownloadsReadOnly",
                "Effect": "Allow",
                "Principal": {"Service": "cloudfront.amazonaws.com"},
                "Action": "s3:GetObject",
                "Resource": "arn:aws:s3:::dig-data-registry/*",
                "Condition": {"StringEquals": {"AWS:SourceArn": arn}},
            },
            {
                "Sid": "CloudFrontNeverPrivatePrefixes",
                "Effect": "Deny",
                "Principal": {"Service": "cloudfront.amazonaws.com"},
                "Action": "s3:GetObject",
                "Resource": [
                    "arn:aws:s3:::dig-data-registry/hermes/*",
                    "arn:aws:s3:::dig-data-registry/sgc/*",
                    "arn:aws:s3:::dig-data-registry/hcm/*",
                    "arn:aws:s3:::dig-data-registry/peg/*",
                    "arn:aws:s3:::dig-data-registry/qc/*",
                    "arn:aws:s3:::dig-data-registry/mskkp/*",
                    "arn:aws:s3:::dig-data-registry/calr/*",
                    "arn:aws:s3:::dig-data-registry/kpn-cms-assets/*",
                ],
            },
        ],
    }


def test_private_prefixes_are_the_approved_list():
    assert cdn_config.PRIVATE_PREFIXES == ("hermes", "sgc", "hcm", "peg", "qc", "mskkp", "calr", "kpn-cms-assets")


def test_the_deny_has_no_condition_so_it_covers_any_distribution_and_names_only_the_cloudfront_service():
    deny = bucket_policy("arn:aws:cloudfront::005901288866:distribution/E1")["Statement"][1]

    assert "Condition" not in deny
    assert deny["Principal"] == {"Service": "cloudfront.amazonaws.com"}  # presigned S3 links are not that principal
    assert deny["Action"] == "s3:GetObject"


def test_api_role_policy_resources():
    secret_arn = "arn:aws:secretsmanager:us-east-1:005901288866:secret:data-registry-cloudfront-signing-key-AbCdEf"

    policy = api_role_policy(secret_arn)

    assert policy["Statement"] == [
        {"Effect": "Allow", "Action": "ssm:GetParameters",
         "Resource": "arn:aws:ssm:us-east-1:005901288866:parameter/data-registry/*/cloudfront-downloads*"},
        {"Effect": "Allow", "Action": "secretsmanager:GetSecretValue", "Resource": secret_arn},
    ]


def test_settings_parameter_value_round_trips_through_app_parser():
    s = parse_settings(settings_parameter_value("d1.cloudfront.net", "K1"), "0", lambda sid: b"pem")

    assert (s.domain, s.key_pair_id, s.bucket, s.percent) == ("d1.cloudfront.net", "K1", "dig-data-registry", 0)


def test_settings_parameter_value_names_the_signing_key_secret():
    asked = []

    parse_settings(settings_parameter_value("d1.cloudfront.net", "K1"), "0", lambda sid: asked.append(sid) or b"pem")

    assert asked == [cdn_config.SECRET_ID] == ["data-registry-cloudfront-signing-key"]


def test_constants():
    assert cdn_config.ACCOUNT_ID == "005901288866"
    assert cdn_config.REGION == "us-east-1"
    assert cdn_config.ORIGIN_BUCKET == "dig-data-registry"
    assert (cdn_config.LOG_BUCKET, cdn_config.LOG_PREFIX, cdn_config.LOG_RETENTION_DAYS) == (
        "dig-data-registry-cf-logs", "downloads/", 90)
    assert cdn_config.NAME == "data-registry-downloads"
    assert cdn_config.PUBLIC_KEY_NAME == "data-registry-downloads-2026-11"
    assert cdn_config.API_ROLE == "dig-data-registry-permissions"
    assert cdn_config.API_ROLE_POLICY_NAME == "cloudfront-downloads"
