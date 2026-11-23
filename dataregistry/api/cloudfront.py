"""CloudFront signed download links: runtime settings, key encoding and signing.

Settings live in AWS so the rollout percentage can be changed without a deploy:

  /data-registry/<env>/cloudfront-downloads          JSON: domain, key_pair_id, bucket, key_secret_id
  /data-registry/<env>/cloudfront-downloads-percent  "0" to "100"

and the signing key's PEM lives in the Secrets Manager secret named by key_secret_id.

load_settings() is what request handlers call. It asks AWS at most once per
SETTINGS_TTL_SECONDS and answers None when anything is wrong (missing parameters,
bad values, AWS unreachable), caching that None too, so a broken CloudFront setup
or an AWS outage degrades to the S3 link instead of failing downloads. The caller
turns None, and any exception from signed_url, into the S3 link.

download_url() is what stream_file calls: it picks CloudFront for the configured share of
downloads from the configured bucket, and the S3 link for everything else.

The cache is module level and unlocked: stream_file is async, so calls arrive on a
single event-loop thread. The private key is never logged.

For live checks, print one signed link (ignoring the rollout percent) with:

  python -m dataregistry.api.cloudfront sign KEY [--env prd|dev] [--expires-in SECONDS]
"""
import argparse
import json
import logging
import os
import random
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Callable, Optional
from urllib.parse import quote

import boto3
from botocore.config import Config
from botocore.signers import CloudFrontSigner
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from dataregistry.api import s3

logger = logging.getLogger(__name__)

LINK_LIFETIME_SECONDS: int = 7200
SETTINGS_TTL_SECONDS: float = 60.0

_REGION = "us-east-1"
# One try per AWS call, each with 1 s connect and read timeouts (about 2 s per call at worst; a refresh
# makes one SSM call, plus a Secrets Manager call the first time). AWS calls run on the event loop, so an
# outage must cost one request a couple of seconds, once a minute, never several seconds of retries.
_BOTO_CONFIG = Config(connect_timeout=1, read_timeout=1, retries={"total_max_attempts": 1})

_CONFIG_FIELDS = ("domain", "key_pair_id", "bucket", "key_secret_id")
# The distribution's own host name. A malformed one (scheme, path, spaces) would build a link that
# never reaches the S3 fallback, so it turns the feature off instead.
_DOMAIN_PATTERN = re.compile(r"[a-z0-9]+\.cloudfront\.net")


@dataclass(frozen=True)
class CloudFrontSettings:
    percent: int
    domain: str
    key_pair_id: str
    bucket: str
    # repr=False keeps the key out of log lines and tracebacks.
    private_key_pem: bytes = field(repr=False)


def environment() -> str:
    """The deployment environment: "prd" when running against the production database, else "dev"."""
    return "prd" if os.environ.get("DATA_REGISTRY_DB_NAME") == "dataregistry" else "dev"


def parameter_names(env: str) -> tuple[str, str]:
    """The SSM parameter names (config JSON, percent) for `env`."""
    return f"/data-registry/{env}/cloudfront-downloads", f"/data-registry/{env}/cloudfront-downloads-percent"


def parse_settings(config_json: str, percent_raw: str, read_secret: Callable[[str], bytes]) -> CloudFrontSettings:
    """Build settings from the two parameter values, reading the key through `read_secret`.

    Raises ValueError on any bad value.
    """
    config = json.loads(config_json)
    if not isinstance(config, dict):
        raise ValueError("cloudfront config is not a JSON object")
    for name in _CONFIG_FIELDS:
        value = config.get(name)
        if not isinstance(value, str) or not value:
            raise ValueError(f"cloudfront config field {name!r} must be a non-empty string")
    if not _DOMAIN_PATTERN.fullmatch(config["domain"]):
        raise ValueError(f"cloudfront config field 'domain' must look like d111111abcdef8.cloudfront.net, "
                         f"got {config['domain']!r}")
    percent = int(percent_raw.strip())
    if not 0 <= percent <= 100:
        raise ValueError(f"cloudfront percent {percent} is outside 0..100")
    return CloudFrontSettings(percent=percent, domain=config["domain"], key_pair_id=config["key_pair_id"],
                              bucket=config["bucket"], private_key_pem=read_secret(config["key_secret_id"]))


@lru_cache(maxsize=None)
def _read_secret(secret_id: str) -> bytes:
    """The signing key's PEM. Cached for the life of the process; failures are not cached."""
    secrets = boto3.client("secretsmanager", region_name=_REGION, config=_BOTO_CONFIG)
    response = secrets.get_secret_value(SecretId=secret_id)
    if "SecretString" in response:
        return response["SecretString"].encode()
    return bytes(response["SecretBinary"])


def fetch_settings(env: str) -> CloudFrontSettings:
    """Read the settings from AWS now, with no caching of the parameters. Raises on any error."""
    config_name, percent_name = parameter_names(env)
    ssm = boto3.client("ssm", region_name=_REGION, config=_BOTO_CONFIG)
    response = ssm.get_parameters(Names=[config_name, percent_name])
    values = {p["Name"]: p["Value"] for p in response["Parameters"]}
    missing = [name for name in (config_name, percent_name) if name not in values]
    if missing:
        raise LookupError(f"missing SSM parameters: {', '.join(missing)}")
    return parse_settings(values[config_name], values[percent_name], _read_secret)


# (monotonic time the entry expires, settings or None), and the last state we logged.
_cache: Optional[tuple[float, Optional[CloudFrontSettings]]] = None
_last_logged: Optional[str] = None


def load_settings(now: Callable[[], float] = time.monotonic) -> Optional[CloudFrontSettings]:
    """The current settings, or None when CloudFront downloads are off or unavailable.

    Asks AWS at most once per SETTINGS_TTL_SECONDS and caches a failure (None) the
    same way as a success. Logs one line each time the outcome changes.
    """
    global _cache, _last_logged
    current = now()
    if _cache is not None and current < _cache[0]:
        return _cache[1]
    env = environment()
    try:
        settings: Optional[CloudFrontSettings] = fetch_settings(env)
        message = f"cloudfront downloads: env={env} percent={settings.percent}"
    except Exception as e:
        settings = None
        message = f"cloudfront downloads: off ({type(e).__name__}: {e})"
    _cache = (current + SETTINGS_TTL_SECONDS, settings)
    if message != _last_logged:
        _last_logged = message
        # WARNING, not INFO: the app configures no logging, so only WARNING and above reach stderr
        # (Python's last-resort handler) and the journal, and this line is how operators see the state.
        logger.warning(message)
    return settings


def reset_settings_cache() -> None:
    """Forget the cached settings and the last logged state (for tests)."""
    global _cache, _last_logged
    _cache = None
    _last_logged = None


def encode_key(key: str) -> str:
    """Percent-encode an S3 key for use as a URL path, keeping "/" as the separator."""
    return quote(key, safe="/")


@lru_cache(maxsize=2)
def _load_private_key(pem: bytes):
    """The parsed key for `pem`. Parsing costs ~30 ms, so it happens once per key, not once per download.

    maxsize 2 covers a key rotation (old and new key in flight); a bad PEM raises and is not cached.
    """
    return serialization.load_pem_private_key(pem, password=None)


def signed_url(settings: CloudFrontSettings, key: str, expires_at: datetime) -> str:
    """A CloudFront URL for `key` that stops working at `expires_at` (canned policy)."""

    def rsa_signer(message: bytes) -> bytes:
        # CloudFront canned-policy signatures are RSA PKCS#1 v1.5 over SHA-1.
        return _load_private_key(settings.private_key_pem).sign(message, padding.PKCS1v15(), hashes.SHA1())

    return CloudFrontSigner(settings.key_pair_id, rsa_signer).generate_presigned_url(
        f"https://{settings.domain}/{encode_key(key)}", date_less_than=expires_at)


def download_url(bucket: str, key: str, rand: Callable[[], float] = random.random) -> str:
    """The link a download should redirect to: CloudFront for the configured share of
    downloads from the configured bucket, otherwise (and on any CloudFront error) the S3 link.
    """
    settings = load_settings()
    if settings is not None and bucket == settings.bucket and rand() * 100 < settings.percent:
        try:
            return signed_url(settings, key, datetime.now(timezone.utc) + timedelta(seconds=LINK_LIFETIME_SECONDS))
        except Exception:
            logger.exception("cloudfront signing failed; using S3 link")
    return s3.get_signed_url(bucket, key)


def main(argv: Optional[list[str]] = None) -> None:
    """Print one signed CloudFront URL for KEY, whatever the rollout percent is."""
    parser = argparse.ArgumentParser(prog="python -m dataregistry.api.cloudfront", description=main.__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    sign = commands.add_parser("sign", help="print a signed URL for an S3 key")
    sign.add_argument("key", metavar="KEY", help="S3 key in the CloudFront bucket")
    sign.add_argument("--env", choices=["prd", "dev"], default=None, help="settings to read (default: this environment)")
    sign.add_argument("--expires-in", type=int, default=LINK_LIFETIME_SECONDS, metavar="SECONDS",
                      help="seconds until the link expires; negative makes an already-expired link (default: 7200)")
    args = parser.parse_args(argv)
    settings = fetch_settings(args.env or environment())
    print(signed_url(settings, args.key, datetime.now(timezone.utc) + timedelta(seconds=args.expires_in)))


if __name__ == "__main__":
    main()
