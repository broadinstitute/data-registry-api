"""Run the LIVE file-name function through CloudFront's own test API with sample responses.

    deploy/cloudfront/.venv/bin/python -m deploy.cloudfront.check_filename_function

Each case is a viewer-response event; the check compares the content-disposition header the
function returns with the one the S3 presigned links carry. Read-only: test_function runs the
function in CloudFront and changes nothing.
"""
import argparse
import json
import sys
from typing import Any, Optional

import boto3
from botocore.exceptions import ClientError

from deploy.cloudfront import cdn_config as cfg

# (request uri, response status, expected content-disposition or None when the header must be absent)
CASES: list[tuple[str, int, Optional[str]]] = [
    ("/Kreitmaier_2026_mQTL_eQTM_Osteoarthritis/methqtl_hg/methqtl_hg_ALLchr.txt.gz", 200,
     'attachment; filename="methqtl_hg_ALLchr.txt.gz"'),
    ("/Genetics%20of%20Osteoarthritis%20GWAS%20meta-analysis%20-%20autosomal%20summary%20statistics/HandOA/"
     "GO2.ALLANCESTRIES.HAND.AUTOSOMAL.12SEPT2023.txt.gz", 206,
     'attachment; filename="GO2.ALLANCESTRIES.HAND.AUTOSOMAL.12SEPT2023.txt.gz"'),
    ("/x/b%2Bc%20%28v2%29.txt", 200, 'attachment; filename="b+c (v2).txt"'),
    ("/x/caf%C3%A9.txt", 200, "attachment; filename=\"caf%C3%A9.txt\"; filename*=UTF-8''caf%C3%A9.txt"),
    ("/x/bad%E0.txt", 200, 'attachment; filename="bad%E0.txt"'),
    ("/x/a.txt", 403, None),
]

_REASONS = {200: "OK", 206: "Partial Content", 403: "Forbidden"}


def viewer_response_event(uri: str, status: int) -> dict:
    return {
        "version": "1.0",
        "context": {"distributionDomainName": "d111111abcdef8.cloudfront.net", "distributionId": "EDFDVBD6EXAMPLE",
                    "eventType": "viewer-response", "requestId": "check-filename-function"},
        "viewer": {"ip": "198.51.100.11"},
        "request": {"method": "GET", "uri": uri, "querystring": {}, "headers": {}, "cookies": {}},
        "response": {"statusCode": status, "statusDescription": _REASONS[status],
                     "headers": {"content-type": {"value": "application/gzip"}}, "cookies": {}},
    }


def content_disposition(test_result: dict) -> Optional[str]:
    """The content-disposition value in a test_function result, or None when the function left it out."""
    if test_result.get("FunctionErrorMessage"):
        raise RuntimeError(f"function error: {test_result['FunctionErrorMessage']}")
    output = json.loads(test_result["FunctionOutput"])
    response = output.get("response", output)  # the output may or may not be wrapped in {"response": ...}
    return response.get("headers", {}).get("content-disposition", {}).get("value")


def check(cf: Any) -> int:
    etag = cf.describe_function(Name=cfg.NAME, Stage="LIVE")["ETag"]
    passed = 0
    for uri, status, expected in CASES:
        result = cf.test_function(Name=cfg.NAME, IfMatch=etag, Stage="LIVE",
                                  EventObject=json.dumps(viewer_response_event(uri, status)).encode())
        try:
            actual = content_disposition(result["TestResult"])
        except (RuntimeError, ValueError) as e:
            actual = f"<{e}>"
        ok = actual == expected
        passed += ok
        print(f"{'PASS' if ok else 'FAIL'}  {status} {uri}")
        if not ok:
            print(f"      expected: {expected!r}\n      actual:   {actual!r}")
    print(f"{passed}/{len(CASES)} PASS")
    return 0 if passed == len(CASES) else 1


def main(argv: Optional[list[str]] = None) -> None:
    argparse.ArgumentParser(prog="python -m deploy.cloudfront.check_filename_function",
                            description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
                            ).parse_args(argv)
    try:
        sys.exit(check(boto3.client("cloudfront", region_name=cfg.REGION)))
    except ClientError as e:
        sys.exit(f"error: AWS refused a call: {e.response['Error']['Code']}: {e.response['Error']['Message']} "
                 f"({e.operation_name})")


if __name__ == "__main__":
    main()
