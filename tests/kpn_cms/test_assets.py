import boto3
import responses
from moto import mock_aws
from sqlalchemy import text

from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api import kpn_cms_assets as a

engine = DataRegistryReadWriteDB().get_engine()

# One blob exercising all three syntactic forms + a JSON-escaped payload form
# with a multi-segment path (the shape real news thumbnails use).
BLOB = (
    'x <img src="https://kp4cd.org/sites/default/files/news_thumbnails/aug%206.png"> '
    'y <img src="//kp4cd.org/sites/default/files/inline-images/data_icon4.png"> '
    'z url(//kp4cd.org/sites/default/files/vueportal/t2d_bg.png) '
    'w src=\\"https:\\/\\/kp4cd.org\\/sites\\/default\\/files\\/news_thumbnails\\/small.svg\\"'
)


def test_find_asset_urls_all_forms():
    urls = a.find_asset_urls(BLOB)
    assert urls == {
        'https://kp4cd.org/sites/default/files/news_thumbnails/aug%206.png',
        'https://kp4cd.org/sites/default/files/inline-images/data_icon4.png',
        'https://kp4cd.org/sites/default/files/vueportal/t2d_bg.png',
        'https://kp4cd.org/sites/default/files/news_thumbnails/small.svg',
    }


def test_asset_path_decodes_percent():
    assert a.asset_path('https://kp4cd.org/sites/default/files/news_thumbnails/aug%206.png') \
        == 'news_thumbnails/aug 6.png'


def test_asset_path_strips_query_and_fragment():
    """Drupal image-style URLs with ?itok=TOKEN must strip query for clean S3 key."""
    assert a.asset_path('https://kp4cd.org/sites/default/files/styles/thumbnail/public/2023-05/photo.jpg?itok=AbC123xyz') \
        == 'styles/thumbnail/public/2023-05/photo.jpg'
    assert a.asset_path('https://kp4cd.org/sites/default/files/image.png#section') \
        == 'image.png'


def test_rewrite_covers_every_form_and_leaves_other_urls():
    out = a.rewrite_asset_urls(BLOB + ' keep https://kp4cd.org/contact')
    assert 'kp4cd.org/sites/default/files' not in out
    assert '/api/kpn/files/news_thumbnails/aug%206.png' in out
    assert 'url(/api/kpn/files/vueportal/t2d_bg.png)' in out
    assert '\\/api\\/kpn\\/files\\/news_thumbnails\\/small.svg' in out  # escaped form stays escaped
    assert 'https://kp4cd.org/contact' in out  # non-asset links untouched


def test_rewrite_strips_query_and_fragment():
    """Rewritten URLs must have clean paths without query/fragment."""
    out = a.rewrite_asset_urls('<img src="https://kp4cd.org/sites/default/files/styles/thumbnail/public/photo.jpg?itok=AbC123xyz">')
    assert '/api/kpn/files/styles/thumbnail/public/photo.jpg' in out
    assert '?itok=' not in out


@mock_aws
@responses.activate
def test_mirror_assets_uploads_and_records_absent():
    with engine.connect() as con:
        con.execute(text("TRUNCATE TABLE cms_asset")); con.commit()
    s3 = boto3.client('s3', region_name='us-east-1')
    s3.create_bucket(Bucket='test-bucket')
    ok = 'https://kp4cd.org/sites/default/files/images/news.svg'
    gone = 'https://kp4cd.org/sites/default/files/vueportal/missing.png'
    responses.get(ok, body=b'<svg/>', content_type='image/svg+xml')
    responses.get(gone, status=404)
    report = a.mirror_assets([ok, gone], engine, s3, 'test-bucket')
    assert report == {'mirrored': 1, 'absent': [gone], 'errors': []}
    body = s3.get_object(Bucket='test-bucket', Key='kpn-cms-assets/images/news.svg')['Body'].read()
    assert body == b'<svg/>'
    with engine.connect() as con:
        rows = con.execute(text("SELECT remote_url, status FROM cms_asset ORDER BY remote_url")).fetchall()
    assert [(r[0], r[1]) for r in rows] == [(ok, 'mirrored'), (gone, 'absent')]


@mock_aws
@responses.activate
def test_mirror_assets_handles_errors():
    """Network/S3 errors are caught and recorded as status='error'."""
    with engine.connect() as con:
        con.execute(text("TRUNCATE TABLE cms_asset")); con.commit()
    s3 = boto3.client('s3', region_name='us-east-1')
    s3.create_bucket(Bucket='test-bucket')
    ok = 'https://kp4cd.org/sites/default/files/images/good.svg'
    error_url = 'https://kp4cd.org/sites/default/files/images/timeout.svg'
    responses.get(ok, body=b'<svg/>', content_type='image/svg+xml')
    responses.get(error_url, body=ConnectionError('timeout'))
    report = a.mirror_assets([ok, error_url], engine, s3, 'test-bucket')
    assert report == {'mirrored': 1, 'absent': [], 'errors': [error_url]}
    with engine.connect() as con:
        rows = con.execute(text("SELECT remote_url, status FROM cms_asset ORDER BY remote_url")).fetchall()
    result = [(r[0], r[1]) for r in rows]
    assert result == [(ok, 'mirrored'), (error_url, 'error')]
