"""Find, rewrite, and S3-mirror kp4cd.org asset URLs embedded in CMS payloads.

Payloads reference assets in three syntactic forms (absolute, protocol-relative,
CSS url()) and, inside JSON strings, with escaped slashes (https:\\/\\/...).
Matching is on the bare host — a scheme-anchored pattern misses a third of them.
"""
import re
import urllib.parse

import requests

from dataregistry.api.kpn_cms_query import upsert_asset

BROWSER_UA = ('Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
              '(KHTML, like Gecko) Chrome/126.0 Safari/537.36')
ASSET_PREFIX = 'kpn-cms-assets/'

# Any kp4cd.org URL (bare-host match, tolerating JSON-escaped slashes).
# The captured path is a sequence of (/segment) groups; segments stop at
# whitespace, quotes, parens, angle brackets, backslashes, and commas.
_ASSET_RE = re.compile(r'(?:https?:)?(?:\\?/){2}kp4cd\.org((?:\\?/[^\s"\'()<>\\,]+)+)')
_FILES_PREFIX = '/sites/default/files/'


def _unescape(path: str) -> str:
    return path.replace('\\/', '/')


def find_asset_urls(text_blob: str) -> set:
    urls = set()
    for m in _ASSET_RE.finditer(text_blob):
        path = _unescape(m.group(1))
        if path.startswith(_FILES_PREFIX):
            urls.add('https://kp4cd.org' + path)
    return urls


def asset_path(url: str) -> str:
    return urllib.parse.unquote(url.split(_FILES_PREFIX, 1)[1])


def rewrite_asset_urls(text_blob: str) -> str:
    def _sub(m):
        raw = m.group(1)
        path = _unescape(raw)
        if not path.startswith(_FILES_PREFIX):
            return m.group(0)          # non-asset kp4cd link (e.g. /contact): untouched
        new = '/api/kpn/files/' + path[len(_FILES_PREFIX):]
        return new.replace('/', '\\/') if '\\/' in raw else new
    return _ASSET_RE.sub(_sub, text_blob)


def mirror_assets(urls, engine, s3_client, bucket) -> dict:
    mirrored, absent = 0, []
    for url in sorted(urls):
        key = ASSET_PREFIX + asset_path(url)
        resp = requests.get(url, headers={'User-Agent': BROWSER_UA}, timeout=60)
        if resp.status_code == 200:
            ctype = resp.headers.get('Content-Type', 'application/octet-stream')
            s3_client.put_object(Bucket=bucket, Key=key, Body=resp.content, ContentType=ctype)
            upsert_asset(engine, url, key, ctype, len(resp.content), 'mirrored')
            mirrored += 1
        else:
            upsert_asset(engine, url, None, None, None, 'absent')
            absent.append(url)
    return {'mirrored': mirrored, 'absent': absent}
