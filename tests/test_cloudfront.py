import base64
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

import boto3
import pytest
from botocore.signers import CloudFrontSigner
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from moto import mock_aws

from dataregistry.api import cloudfront

_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
PEM = _KEY.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
CONFIG = {"domain": "dtest.cloudfront.net", "key_pair_id": "KTEST", "bucket": "dig-data-registry",
          "key_secret_id": "data-registry-cloudfront-signing-key"}


def make_settings(percent=100, pem=PEM):
    return cloudfront.CloudFrontSettings(percent=percent, domain="dtest.cloudfront.net", key_pair_id="KTEST",
                                         bucket="dig-data-registry", private_key_pem=pem)


def assert_valid_canned_signature(url):
    resource, query = url.split("?", 1)
    q = parse_qs(query)
    expires = datetime.fromtimestamp(int(q["Expires"][0]), tz=timezone.utc)
    policy = CloudFrontSigner("KTEST", None).build_policy(resource, expires).encode()
    sig = base64.b64decode(q["Signature"][0].replace("-", "+").replace("_", "=").replace("~", "/"))
    _KEY.public_key().verify(sig, policy, padding.PKCS1v15(), hashes.SHA1())
    assert q["Key-Pair-Id"] == ["KTEST"]


def seed_aws(percent="10", binary_secret=False):
    ssm = boto3.client("ssm", region_name="us-east-1")
    ssm.put_parameter(Name="/data-registry/prd/cloudfront-downloads", Value=json.dumps(CONFIG), Type="String")
    ssm.put_parameter(Name="/data-registry/prd/cloudfront-downloads-percent", Value=percent, Type="String")
    secret = {"SecretBinary": PEM} if binary_secret else {"SecretString": PEM.decode()}
    boto3.client("secretsmanager", region_name="us-east-1").create_secret(Name=CONFIG["key_secret_id"], **secret)
    return ssm


@pytest.fixture(autouse=True)
def _clean_caches():
    # api.py imports cloudfront, so conftest loads it before the alembic migration runs, and
    # migrations/env.py's fileConfig() disables every logger that already exists. Undo that here.
    cloudfront.logger.disabled = False
    cloudfront.reset_settings_cache()
    cloudfront._read_secret.cache_clear()
    cloudfront._load_private_key.cache_clear()
    yield
    cloudfront.reset_settings_cache()
    cloudfront._read_secret.cache_clear()
    cloudfront._load_private_key.cache_clear()


def test_environment(monkeypatch):
    monkeypatch.setenv("DATA_REGISTRY_DB_NAME", "dataregistry")
    assert cloudfront.environment() == "prd"
    monkeypatch.setenv("DATA_REGISTRY_DB_NAME", "dataregistry_qa")
    assert cloudfront.environment() == "dev"
    monkeypatch.delenv("DATA_REGISTRY_DB_NAME")
    assert cloudfront.environment() == "dev"


def test_parameter_names():
    assert cloudfront.parameter_names("prd") == ("/data-registry/prd/cloudfront-downloads",
                                                 "/data-registry/prd/cloudfront-downloads-percent")


def test_parse_settings_valid():
    settings = cloudfront.parse_settings(json.dumps(CONFIG), " 10\n",
                                         lambda sid: PEM if sid == CONFIG["key_secret_id"] else None)
    assert settings == make_settings(10)


@pytest.mark.parametrize("percent_raw,expected", [("0", 0), ("100", 100), (" 100\n", 100)])
def test_parse_settings_accepts_the_percent_bounds(percent_raw, expected):
    assert cloudfront.parse_settings(json.dumps(CONFIG), percent_raw, lambda sid: PEM).percent == expected


@pytest.mark.parametrize("percent_raw", ["abc", "150", "101", "-1", ""])
def test_parse_settings_rejects_bad_percent(percent_raw):
    with pytest.raises(ValueError):
        cloudfront.parse_settings(json.dumps(CONFIG), percent_raw, lambda sid: PEM)


@pytest.mark.parametrize("config_json", [
    json.dumps({k: v for k, v in CONFIG.items() if k != "domain"}),
    json.dumps({**CONFIG, "domain": ""}),
    "{",
    "[]",
])
def test_parse_settings_rejects_bad_config(config_json):
    with pytest.raises(ValueError):
        cloudfront.parse_settings(config_json, "10", lambda sid: PEM)


@pytest.mark.parametrize("domain", ["https://d1.cloudfront.net", " d1.cloudfront.net", "d1.cloudfront.net/",
                                    "d1.cloudfront.net\n", "d1.example.com", "D1.cloudfront.net",
                                    "d1.cloudfront.net:443", "d1.cloudfront.net.evil.com", ".cloudfront.net",
                                    "a.b.cloudfront.net"])
def test_parse_settings_rejects_a_domain_that_is_not_a_bare_cloudfront_host(domain):
    # A bad domain makes an unusable link that never reaches the S3 fallback, so it must turn the feature off.
    with pytest.raises(ValueError, match="domain"):
        cloudfront.parse_settings(json.dumps({**CONFIG, "domain": domain}), "10", lambda sid: PEM)


@pytest.mark.parametrize("domain", ["dtest.cloudfront.net", "d111111abcdef8.cloudfront.net"])
def test_parse_settings_accepts_a_cloudfront_host(domain):
    assert cloudfront.parse_settings(json.dumps({**CONFIG, "domain": domain}), "10", lambda sid: PEM).domain == domain


@pytest.mark.parametrize("key,expected", [
    ("Kreitmaier_2026_mQTL_eQTM_Osteoarthritis/methqtl_hg/methqtl_hg_ALLchr.txt.gz",
     "Kreitmaier_2026_mQTL_eQTM_Osteoarthritis/methqtl_hg/methqtl_hg_ALLchr.txt.gz"),
    ("Genetics of Osteoarthritis GWAS meta-analysis - autosomal summary statistics/HandOA/"
     "GO2.ALLANCESTRIES.HAND.AUTOSOMAL.12SEPT2023.txt.gz",
     "Genetics%20of%20Osteoarthritis%20GWAS%20meta-analysis%20-%20autosomal%20summary%20statistics/HandOA/"
     "GO2.ALLANCESTRIES.HAND.AUTOSOMAL.12SEPT2023.txt.gz"),
    ("a/b+c (v2).txt", "a/b%2Bc%20%28v2%29.txt"),
    ("a/café.txt", "a/caf%C3%A9.txt"),
    ("a/x~y_z-1.txt", "a/x~y_z-1.txt"),
])
def test_encode_key(key, expected):
    assert cloudfront.encode_key(key) == expected


def test_signed_url_is_valid_canned_signature():
    url = cloudfront.signed_url(make_settings(), "a/b+c (v2).txt", datetime(2030, 1, 1, tzinfo=timezone.utc))
    assert url.startswith("https://dtest.cloudfront.net/a/b%2Bc%20%28v2%29.txt?Expires=1893456000&")
    assert_valid_canned_signature(url)


@pytest.mark.parametrize("key", ["a/café.txt", "a/100%.txt", "a/what?.txt", "a/x#y.txt",
                                 "a b/é%?#+&=;,@.txt"])
def test_signed_url_round_trips_awkward_keys(key):
    url = cloudfront.signed_url(make_settings(), key, datetime(2030, 1, 1, tzinfo=timezone.utc))
    assert unquote(urlsplit(url).path[1:]) == key  # "?" and "#" in a key must not start a query or fragment
    assert "#" not in url and url.count("?") == 1
    assert_valid_canned_signature(url)


def test_signed_url_parses_the_private_key_once_per_pem(monkeypatch):
    parsed = []
    real = cloudfront.serialization.load_pem_private_key
    monkeypatch.setattr(cloudfront.serialization, "load_pem_private_key",
                        lambda *args, **kwargs: parsed.append(1) or real(*args, **kwargs))
    expires = datetime(2030, 1, 1, tzinfo=timezone.utc)

    for _ in range(3):
        assert_valid_canned_signature(cloudfront.signed_url(make_settings(), "a/b.txt", expires))

    assert len(parsed) == 1


def test_a_bad_pem_still_raises_from_signed_url_on_every_call():
    expires = datetime(2030, 1, 1, tzinfo=timezone.utc)
    for _ in range(2):  # a failure is not cached as if it were a key
        with pytest.raises(ValueError):
            cloudfront.signed_url(make_settings(pem=b"not a key"), "a/b.txt", expires)


def test_boto_calls_give_up_fast_so_an_outage_cannot_stall_the_event_loop():
    config = cloudfront._BOTO_CONFIG
    assert (config.connect_timeout, config.read_timeout) == (1, 1)
    assert config.retries == {"total_max_attempts": 1}


@mock_aws
def test_fetch_settings_reads_ssm_and_secret():
    seed_aws()
    assert cloudfront.fetch_settings("prd") == make_settings(10)


@mock_aws
def test_fetch_settings_reads_a_binary_secret():
    seed_aws(binary_secret=True)

    settings = cloudfront.fetch_settings("prd")

    assert settings.private_key_pem == PEM  # bytes, as for a SecretString
    assert_valid_canned_signature(cloudfront.signed_url(settings, "a/b.txt", datetime(2030, 1, 1, tzinfo=timezone.utc)))


@mock_aws
@pytest.mark.parametrize("seeded", [(), ("config",), ("percent",)])
def test_fetch_settings_raises_lookup_error_for_a_missing_parameter(seeded):
    ssm = boto3.client("ssm", region_name="us-east-1")
    config_name, percent_name = cloudfront.parameter_names("prd")
    if "config" in seeded:
        ssm.put_parameter(Name=config_name, Value=json.dumps(CONFIG), Type="String")
    if "percent" in seeded:
        ssm.put_parameter(Name=percent_name, Value="10", Type="String")

    with pytest.raises(LookupError, match="missing SSM parameters") as raised:
        cloudfront.fetch_settings("prd")

    named = set(str(raised.value).split(": ", 1)[1].split(", "))
    assert named == {name for part, name in (("config", config_name), ("percent", percent_name))
                     if part not in seeded}


@mock_aws
def test_load_settings_caches_for_60_seconds(monkeypatch):
    monkeypatch.setenv("DATA_REGISTRY_DB_NAME", "dataregistry")
    ssm = seed_aws("10")
    clock = [1000.0]
    assert cloudfront.load_settings(lambda: clock[0]).percent == 10
    ssm.put_parameter(Name="/data-registry/prd/cloudfront-downloads-percent", Value="50", Type="String",
                      Overwrite=True)
    clock[0] = 1059.0
    assert cloudfront.load_settings(lambda: clock[0]).percent == 10
    clock[0] = 1061.0
    assert cloudfront.load_settings(lambda: clock[0]).percent == 50


def test_load_settings_failure_returns_none_and_is_cached(monkeypatch):
    calls = []

    def boom(env):
        calls.append(env)
        raise RuntimeError("ssm down")

    monkeypatch.setattr(cloudfront, "fetch_settings", boom)
    clock = [0.0]
    assert cloudfront.load_settings(lambda: clock[0]) is None
    clock[0] = 30.0
    assert cloudfront.load_settings(lambda: clock[0]) is None
    assert len(calls) == 1
    clock[0] = 61.0
    assert cloudfront.load_settings(lambda: clock[0]) is None
    assert len(calls) == 2


def test_state_line_reaches_stderr_with_no_logging_configured():
    """The app configures no logging; the state line must still reach stderr (and so the journal)."""
    code = (
        "from dataregistry.api import cloudfront\n"
        "def boom(env):\n"
        "    raise RuntimeError('ssm down')\n"
        "cloudfront.fetch_settings = boom\n"
        "cloudfront.load_settings()\n"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=60,
                            cwd=Path(__file__).resolve().parents[1])
    assert result.returncode == 0, result.stderr
    assert "cloudfront downloads: off (RuntimeError: ssm down)" in result.stderr


def test_load_settings_logs_only_when_the_result_changes(monkeypatch, caplog):
    monkeypatch.setenv("DATA_REGISTRY_DB_NAME", "dataregistry")
    outcome = [RuntimeError("ssm down")]

    def fetch(env):
        if isinstance(outcome[0], Exception):
            raise outcome[0]
        return outcome[0]

    monkeypatch.setattr(cloudfront, "fetch_settings", fetch)
    clock = [0.0]
    with caplog.at_level("WARNING", logger=cloudfront.logger.name):
        for t in (0.0, 61.0, 122.0):  # three refreshes, same failure
            clock[0] = t
            cloudfront.load_settings(lambda: clock[0])
        outcome[0] = make_settings(10)
        clock[0] = 183.0
        cloudfront.load_settings(lambda: clock[0])
        clock[0] = 244.0
        cloudfront.load_settings(lambda: clock[0])
    assert [(r.levelname, r.getMessage()) for r in caplog.records] == [
        ("WARNING", "cloudfront downloads: off (RuntimeError: ssm down)"),
        ("WARNING", "cloudfront downloads: env=prd percent=10"),
    ]
    assert "BEGIN" not in caplog.text and "BEGIN" not in repr(make_settings(10))  # the key stays out of logs


OA_KEY = ("Genetics of Osteoarthritis GWAS meta-analysis - autosomal summary statistics/HandOA/"
          "GO2.ALLANCESTRIES.HAND.AUTOSOMAL.12SEPT2023.txt.gz")


@pytest.fixture
def s3_stub(monkeypatch):
    monkeypatch.setattr(cloudfront.s3, "get_signed_url", lambda b, k: f"https://s3.example/{b}/{k}")


def use_settings(monkeypatch, settings):
    monkeypatch.setattr(cloudfront, "load_settings", lambda: settings)


@pytest.mark.parametrize("percent,rand,expect_cf", [
    (0, 0.0, False),
    (100, 0.9999, True),
    (10, 0.0999, True),
    (10, 0.1, False),
])
def test_download_url_percent_bounds(monkeypatch, s3_stub, percent, rand, expect_cf):
    use_settings(monkeypatch, make_settings(percent))
    url = cloudfront.download_url("dig-data-registry", "x.gz", rand=lambda: rand)
    if expect_cf:
        assert url.startswith("https://dtest.cloudfront.net/x.gz?")
        assert_valid_canned_signature(url)
    else:
        assert url == "https://s3.example/dig-data-registry/x.gz"


def test_download_url_other_bucket_stays_s3(monkeypatch, s3_stub):
    use_settings(monkeypatch, make_settings(100))
    assert cloudfront.download_url("dig-analysis-data", "x.gz", rand=lambda: 0.0) == \
        "https://s3.example/dig-analysis-data/x.gz"


def test_download_url_when_settings_missing_uses_s3(monkeypatch, s3_stub):
    use_settings(monkeypatch, None)
    assert cloudfront.download_url("dig-data-registry", "x.gz", rand=lambda: 0.0) == \
        "https://s3.example/dig-data-registry/x.gz"


def test_download_url_falls_back_when_signing_fails(monkeypatch, s3_stub, caplog):
    use_settings(monkeypatch, make_settings(100, pem=b"not a key"))
    with caplog.at_level("ERROR", logger=cloudfront.logger.name):
        url = cloudfront.download_url("dig-data-registry", "x.gz", rand=lambda: 0.0)
    assert url == "https://s3.example/dig-data-registry/x.gz"
    assert "cloudfront signing failed; using S3 link" in caplog.text


PRIVATE_PREFIXES = ("hermes", "sgc", "hcm", "peg", "qc", "mskkp", "calr", "kpn-cms-assets")


@pytest.mark.parametrize("prefix", PRIVATE_PREFIXES)
def test_download_url_keeps_keys_under_a_denied_prefix_on_s3(monkeypatch, s3_stub, prefix):
    # The bucket policy denies CloudFront reads there, so a CloudFront link would 403 with no fallback.
    use_settings(monkeypatch, make_settings(100))
    assert cloudfront.download_url("dig-data-registry", f"{prefix}/x.gz", rand=lambda: 0.0) == \
        f"https://s3.example/dig-data-registry/{prefix}/x.gz"


@pytest.mark.parametrize("key", ["hcm2/x", "HCM/x", "x/hcm/y", "hcm", "hcm.gz", "hermes-x/y"])
def test_download_url_only_a_whole_first_segment_with_a_slash_after_it_is_guarded(monkeypatch, s3_stub, key):
    use_settings(monkeypatch, make_settings(100))
    url = cloudfront.download_url("dig-data-registry", key, rand=lambda: 0.0)
    assert url.startswith(f"https://dtest.cloudfront.net/{key}?")
    assert_valid_canned_signature(url)


def test_the_sign_cli_does_not_apply_the_guard(monkeypatch, capsys):
    # Operators use it to prove the bucket policy's Deny works, so it must sign a denied key.
    monkeypatch.setattr(cloudfront, "fetch_settings", lambda env: make_settings(0))
    cloudfront.main(["sign", "hermes/x", "--env", "prd"])
    out = capsys.readouterr().out.strip()
    assert out.startswith("https://dtest.cloudfront.net/hermes/x?Expires=")
    assert_valid_canned_signature(out)


def test_download_url_expiry_is_two_hours(monkeypatch, s3_stub):
    use_settings(monkeypatch, make_settings(100))
    url = cloudfront.download_url("dig-data-registry", "x.gz", rand=lambda: 0.0)
    expires = int(parse_qs(urlsplit(url).query)["Expires"][0])
    assert abs(expires - (time.time() + 7200)) <= 5


def stub_file_lookup(monkeypatch, key):
    monkeypatch.setattr("dataregistry.api.query.shortened_file_id_lookup", lambda *args: "abc")
    monkeypatch.setattr("dataregistry.api.query.get_phenotype_file", lambda *args: f"s3://dig-data-registry/{key}")


@pytest.mark.parametrize("key", [OA_KEY, "a/b+c (v2)/x~y_z-1 \u00e9.txt.gz"])
def test_stream_file_redirects_to_cloudfront(api_client, monkeypatch, key):
    stub_file_lookup(monkeypatch, key)
    use_settings(monkeypatch, make_settings(100))
    issued = []
    real_download_url = cloudfront.download_url
    monkeypatch.setattr(cloudfront, "download_url", lambda *args: issued.append(real_download_url(*args)) or issued[-1])

    r = api_client.get("/api/d/abc", follow_redirects=False)

    assert r.status_code == 307
    location = r.headers["location"]
    assert location.startswith("https://dtest.cloudfront.net/")
    assert unquote(urlsplit(location).path) == "/" + key  # the key round-trips through the redirect
    assert location == issued[0]  # and the redirect did not touch the encoding or the signature
    assert_valid_canned_signature(location)
    if key == OA_KEY:
        assert location.startswith("https://dtest.cloudfront.net/Genetics%20of%20Osteoarthritis%20GWAS")


@mock_aws
def test_stream_file_falls_back_when_settings_missing(api_client, monkeypatch):
    stub_file_lookup(monkeypatch, OA_KEY)
    use_settings(monkeypatch, None)
    r = api_client.get("/api/d/abc", follow_redirects=False)
    assert r.status_code == 307
    location = r.headers["location"]
    assert urlsplit(location).netloc.startswith("dig-data-registry.s3")
    assert "response-content-disposition" in location


def test_sign_cli_prints_url(monkeypatch, capsys):
    monkeypatch.setattr(cloudfront, "fetch_settings", lambda env: make_settings(0))
    cloudfront.main(["sign", "a/b.txt", "--env", "prd", "--expires-in", "-60"])
    out = capsys.readouterr().out.strip()
    assert out.startswith("https://dtest.cloudfront.net/a/b.txt?Expires=")
    assert int(parse_qs(urlsplit(out).query)["Expires"][0]) < time.time()
    assert_valid_canned_signature(out)
