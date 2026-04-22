"""
tests/test_endpoints_liftover.py — integration tests for the four new liftover endpoints.

Endpoints tested:
  GET  /api/hermes/liftover/{file_id}
  GET  /api/hermes/liftover/{file_id}/unmapped-url
  GET  /api/hermes/portal-config
  PUT  /api/hermes/portal-config
"""
import uuid

import boto3
from moto import mock_aws
from sqlalchemy import text
from starlette.status import (
    HTTP_200_OK,
    HTTP_401_UNAUTHORIZED,
    HTTP_404_NOT_FOUND,
)

from dataregistry.api import query
from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api.jwt_utils import get_encoded_jwt_data
from dataregistry.api.model import GenomeBuild, HermesFileStatus, User

AUTHORIZATION = "Authorization"

# An admin token (user_name must match the seeded testuser row)
admin_token = f"Bearer {get_encoded_jwt_data(User(user_name='testuser@broadinstitute.org', roles=['admin'], id=1))}"
# A "view-only" token that has VIEW_ALL_ROLES membership
analyst_token = f"Bearer {get_encoded_jwt_data(User(user_name='analyst@example.org', roles=['analyst'], id=99))}"
# A non-owner, non-privileged token
other_token = f"Bearer {get_encoded_jwt_data(User(user_name='stranger@example.org', roles=[], id=42))}"


def _engine():
    return DataRegistryReadWriteDB().get_engine()


def _create_file_and_liftover_job(engine, uploader="testuser@broadinstitute.org"):
    """Insert a file_uploads row and a matching liftover_jobs row. Return (file_id, job_id)."""
    file_id = query.save_file_upload_info(
        engine,
        dataset="liftover-endpoint-test-ds",
        metadata={"column_map": {"chromosome": "CHR", "position": "BP"}},
        s3_path="hermes/liftover-endpoint-test-ds/file.csv",
        filename="file.csv",
        file_size=500,
        uploader=uploader,
        qc_script_options={"fd": 0.2},
        genome_build="grch38",
        initial_qc_status="SUBMITTED TO LIFTOVER",
    )
    job_id = str(uuid.uuid4())
    query.create_liftover_job(
        engine,
        job_id,
        file_id,
        GenomeBuild.grch38,
        GenomeBuild.hg19,
        f"s3://dig-data-registry/hermes/liftover-endpoint-test-ds/file.csv",
        f"s3://dig-data-registry/hermes/liftover/{file_id}/unmapped.tsv",
        uploader,
    )
    return file_id, job_id


# ===========================================================================
# GET /api/hermes/liftover/{file_id}
# ===========================================================================

class TestGetLiftoverJob:
    def test_owner_can_see_own_job(self, api_client):
        engine = _engine()
        # The seeded user is testuser@broadinstitute.org; admin_token uses that name.
        file_id, _ = _create_file_and_liftover_job(engine, uploader="testuser@broadinstitute.org")
        res = api_client.get(f"api/hermes/liftover/{file_id}", headers={AUTHORIZATION: admin_token})
        assert res.status_code == HTTP_200_OK
        data = res.json()
        assert data["source_genome_build"] == "grch38"
        assert data["target_genome_build"] == "hg19"

    def test_admin_can_see_any_job(self, api_client):
        engine = _engine()
        file_id, _ = _create_file_and_liftover_job(engine, uploader="someone@example.org")
        res = api_client.get(f"api/hermes/liftover/{file_id}", headers={AUTHORIZATION: admin_token})
        assert res.status_code == HTTP_200_OK

    def test_analyst_with_view_all_role_can_see_job(self, api_client):
        engine = _engine()
        file_id, _ = _create_file_and_liftover_job(engine, uploader="someone@example.org")
        res = api_client.get(f"api/hermes/liftover/{file_id}", headers={AUTHORIZATION: analyst_token})
        assert res.status_code == HTTP_200_OK

    def test_non_owner_non_admin_gets_401(self, api_client):
        engine = _engine()
        file_id, _ = _create_file_and_liftover_job(engine, uploader="not-stranger@example.org")
        res = api_client.get(f"api/hermes/liftover/{file_id}", headers={AUTHORIZATION: other_token})
        assert res.status_code == HTTP_401_UNAUTHORIZED

    def test_unknown_file_id_returns_404(self, api_client):
        fake_id = str(uuid.uuid4())
        res = api_client.get(f"api/hermes/liftover/{fake_id}", headers={AUTHORIZATION: admin_token})
        # 404 (no liftover job) or 401 (owner check fails for unknown file)
        assert res.status_code in (HTTP_404_NOT_FOUND, HTTP_401_UNAUTHORIZED)


# ===========================================================================
# GET /api/hermes/liftover/{file_id}/unmapped-url
# ===========================================================================

class TestGetLiftoverUnmappedUrl:
    @mock_aws
    def test_owner_gets_presigned_url(self, api_client):
        # Create the moto S3 bucket so the presigned URL generation doesn't fail.
        boto3.resource("s3", region_name="us-east-1").create_bucket(Bucket="dig-data-registry")

        engine = _engine()
        file_id, _ = _create_file_and_liftover_job(engine, uploader="testuser@broadinstitute.org")
        res = api_client.get(
            f"api/hermes/liftover/{file_id}/unmapped-url",
            headers={AUTHORIZATION: admin_token},
        )
        assert res.status_code == HTTP_200_OK
        url = res.json()
        assert isinstance(url, str) and url.startswith("http")

    def test_non_owner_non_admin_gets_401(self, api_client):
        engine = _engine()
        file_id, _ = _create_file_and_liftover_job(engine, uploader="not-stranger@example.org")
        res = api_client.get(
            f"api/hermes/liftover/{file_id}/unmapped-url",
            headers={AUTHORIZATION: other_token},
        )
        assert res.status_code == HTTP_401_UNAUTHORIZED


# ===========================================================================
# GET /api/hermes/portal-config
# ===========================================================================

class TestGetPortalConfig:
    def test_returns_seeded_hg19(self, api_client):
        """Default seed from conftest is hg19; returns target_genome_build."""
        res = api_client.get("api/hermes/portal-config", headers={AUTHORIZATION: admin_token})
        assert res.status_code == HTTP_200_OK
        assert res.json()["target_genome_build"] == "hg19"


# ===========================================================================
# PUT /api/hermes/portal-config
# ===========================================================================

class TestPutPortalConfig:
    def test_admin_can_update(self, api_client):
        res = api_client.put(
            "api/hermes/portal-config",
            headers={AUTHORIZATION: admin_token},
            json={"target_genome_build": "grch38"},
        )
        assert res.status_code == HTTP_200_OK
        assert res.json()["target_genome_build"] == "grch38"

        # Confirm GET reflects new value
        get_res = api_client.get("api/hermes/portal-config", headers={AUTHORIZATION: admin_token})
        assert get_res.status_code == HTTP_200_OK
        assert get_res.json()["target_genome_build"] == "grch38"

    def test_non_admin_gets_401(self, api_client):
        res = api_client.put(
            "api/hermes/portal-config",
            headers={AUTHORIZATION: other_token},
            json={"target_genome_build": "grch38"},
        )
        assert res.status_code == HTTP_401_UNAUTHORIZED

    def test_analyst_gets_401(self, api_client):
        """analyst has VIEW_ALL_ROLES but is not SUPER_USER (admin)."""
        res = api_client.put(
            "api/hermes/portal-config",
            headers={AUTHORIZATION: analyst_token},
            json={"target_genome_build": "grch38"},
        )
        assert res.status_code == HTTP_401_UNAUTHORIZED
