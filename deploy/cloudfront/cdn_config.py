"""Pure builders for the AWS resources behind CloudFront signed downloads.

Nothing here calls AWS. The rollout script passes the ids and ARNs of the resources it
creates to these functions and sends the results to AWS, so every pinned value of the
design lives in one reviewable place and can be tested without credentials.

The CloudFront side of the app is dataregistry/api/cloudfront.py; settings_parameter_value()
builds the SSM parameter that module reads.
"""
import json

ACCOUNT_ID = "005901288866"
REGION = "us-east-1"

ORIGIN_BUCKET = "dig-data-registry"
LOG_BUCKET = "dig-data-registry-cf-logs"
LOG_PREFIX = "downloads/"
LOG_RETENTION_DAYS = 90

# The name of the origin access control, key group, function and web ACL, and the
# distribution's Name tag.
NAME = "data-registry-downloads"
PUBLIC_KEY_NAME = "data-registry-downloads-2026-11"
SECRET_ID = "data-registry-cloudfront-signing-key"

API_ROLE = "dig-data-registry-permissions"
API_ROLE_POLICY_NAME = "cloudfront-downloads"

# Top-level prefixes of the origin bucket that are not public downloads. stream_file serves nothing from them.
PRIVATE_PREFIXES = ("hermes", "sgc", "hcm", "peg", "qc", "mskkp", "calr", "kpn-cms-assets")

# AWS managed policies: Managed-CachingDisabled and Managed-CORS-S3Origin.
CACHING_DISABLED = "4135ea2d-6df8-44a3-9df3-4b5a84be39ad"
CORS_S3_ORIGIN = "88a5eaf4-2fd4-4709-b370-b4c650ea3fcf"

_ORIGIN_ID = ORIGIN_BUCKET


def distribution_config(caller_reference: str, oac_id: str, key_group_id: str, function_arn: str,
                        web_acl_arn: str) -> dict:
    """The DistributionConfig for the downloads distribution.

    S3 origin reached through an origin access control; GET/HEAD only; nothing cached
    (the files are huge and the viewer-response function sets the download name);
    viewers need a link signed by the key group; access logs go to LOG_BUCKET.
    """
    return {
        "CallerReference": caller_reference,
        "Comment": f"{NAME} (stream_file)",
        "Enabled": True,
        "IsIPV6Enabled": True,
        "HttpVersion": "http2",
        "PriceClass": "PriceClass_All",
        "WebACLId": web_acl_arn,
        "ViewerCertificate": {"CloudFrontDefaultCertificate": True},
        "Logging": {"Enabled": True, "IncludeCookies": False,
                    "Bucket": f"{LOG_BUCKET}.s3.amazonaws.com", "Prefix": LOG_PREFIX},
        "Origins": {
            "Quantity": 1,
            "Items": [{
                "Id": _ORIGIN_ID,
                "DomainName": f"{ORIGIN_BUCKET}.s3.{REGION}.amazonaws.com",
                # An empty OAI is how CloudFront is told this S3 origin uses an OAC instead.
                "S3OriginConfig": {"OriginAccessIdentity": ""},
                "OriginAccessControlId": oac_id,
            }],
        },
        "DefaultCacheBehavior": {
            "TargetOriginId": _ORIGIN_ID,
            "ViewerProtocolPolicy": "redirect-to-https",
            "AllowedMethods": {"Quantity": 2, "Items": ["GET", "HEAD"],
                               "CachedMethods": {"Quantity": 2, "Items": ["GET", "HEAD"]}},
            "Compress": False,
            "CachePolicyId": CACHING_DISABLED,
            "OriginRequestPolicyId": CORS_S3_ORIGIN,
            "TrustedKeyGroups": {"Enabled": True, "Quantity": 1, "Items": [key_group_id]},
            "FunctionAssociations": {
                "Quantity": 1,
                "Items": [{"EventType": "viewer-response", "FunctionARN": function_arn}],
            },
        },
    }


def bucket_policy(distribution_arn: str) -> dict:
    """The origin bucket policy: only this distribution may read objects, and no distribution may read PRIVATE_PREFIXES.

    The Deny is defense in depth. stream_file serves nothing from those prefixes, so it changes nothing
    today; it keeps any CloudFront distribution (this one, edited, or another) from ever serving them.
    It has no Condition so that it covers every distribution, and it names only the CloudFront service
    as principal, so it does not touch presigned links or any other S3 access.
    """
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "CloudFrontDownloadsReadOnly",
                "Effect": "Allow",
                "Principal": {"Service": "cloudfront.amazonaws.com"},
                "Action": "s3:GetObject",
                "Resource": f"arn:aws:s3:::{ORIGIN_BUCKET}/*",
                "Condition": {"StringEquals": {"AWS:SourceArn": distribution_arn}},
            },
            {
                "Sid": "CloudFrontNeverPrivatePrefixes",
                "Effect": "Deny",
                "Principal": {"Service": "cloudfront.amazonaws.com"},
                "Action": "s3:GetObject",
                "Resource": [f"arn:aws:s3:::{ORIGIN_BUCKET}/{prefix}/*" for prefix in PRIVATE_PREFIXES],
            },
        ],
    }


def api_role_policy(secret_arn: str) -> dict:
    """The inline policy letting the API read its CloudFront settings and the signing key."""
    return {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": "ssm:GetParameters",
             "Resource": f"arn:aws:ssm:{REGION}:{ACCOUNT_ID}:parameter/data-registry/*/cloudfront-downloads*"},
            {"Effect": "Allow", "Action": "secretsmanager:GetSecretValue", "Resource": secret_arn},
        ],
    }


def settings_parameter_value(domain: str, key_pair_id: str) -> str:
    """The value of the /data-registry/<env>/cloudfront-downloads SSM parameter."""
    return json.dumps({"domain": domain, "key_pair_id": key_pair_id, "bucket": ORIGIN_BUCKET,
                       "key_secret_id": SECRET_ID})
