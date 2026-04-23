"""
tests/test_liftover.py — unit + integration tests for the GWAS liftover feature.

Covers:
  - should_liftover (pure function)
  - query helpers: set/get portal target build, create/fetch/update liftover_jobs,
    update_file_upload_after_liftover
  - _build_callback closure: SUCCESS, SUCCESS-no-summary, FAILURE, SUCCESS-with-QC-raise
"""
import json
import uuid
from unittest.mock import MagicMock

import pytest

from dataregistry.api import query
from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api.liftover import _build_callback, should_liftover
from dataregistry.api.model import GenomeBuild, HermesFileStatus, LiftoverJobStatus


# ---------------------------------------------------------------------------
# Shared engine fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def engine():
    return DataRegistryReadWriteDB().get_engine()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_file_id(engine, genome_build="grch38", initial_qc_status="SUBMITTED TO LIFTOVER"):
    """Create a minimal file_uploads row and return its ID (no dashes).

    genome_build is written into metadata.referenceGenome — the schema no
    longer has a dedicated column.  The SELECT-side normalizer in
    query.GENOME_BUILD_NORMALIZER_SQL maps it back to a GenomeBuild enum
    value for tests that assert on row.genome_build.
    """
    metadata = {
        "column_map": {"chromosome": "CHR", "position": "BP"},
        "referenceGenome": genome_build,
    }
    fid = query.save_file_upload_info(
        engine,
        dataset="test-dataset",
        metadata=metadata,
        s3_path="hermes/test-dataset/test.csv",
        filename="test.csv",
        file_size=1234,
        uploader="tester@example.org",
        qc_script_options={"fd": 0.2},
        initial_qc_status=initial_qc_status,
    )
    return fid


def _fetch_file_row(engine, file_id):
    """Fetch raw file_uploads row for assertions."""
    from sqlalchemy import text
    from dataregistry.api.query import GENOME_BUILD_NORMALIZER_SQL
    with engine.connect() as conn:
        row = conn.execute(
            text(f"SELECT qc_status, {GENOME_BUILD_NORMALIZER_SQL} AS genome_build "
                 f"FROM file_uploads WHERE id = :id"),
            {"id": str(file_id).replace("-", "")},
        ).first()
    return row


# ===========================================================================
# 1. Pure-function unit tests for should_liftover
# ===========================================================================

class TestShouldLiftover:
    def test_na_source_returns_false(self):
        assert should_liftover(GenomeBuild.na, GenomeBuild.hg19) is False

    def test_na_target_but_na_source_returns_false(self):
        # source=na always short-circuits regardless of target
        assert should_liftover(GenomeBuild.na, GenomeBuild.na) is False

    def test_same_build_returns_false(self):
        assert should_liftover(GenomeBuild.hg19, GenomeBuild.hg19) is False

    def test_same_grch38_returns_false(self):
        assert should_liftover(GenomeBuild.grch38, GenomeBuild.grch38) is False

    def test_grch38_to_hg19_returns_true(self):
        assert should_liftover(GenomeBuild.grch38, GenomeBuild.hg19) is True

    def test_hg19_to_grch38_returns_true(self):
        assert should_liftover(GenomeBuild.hg19, GenomeBuild.grch38) is True


# ===========================================================================
# 2. Query helper tests (require DB)
# ===========================================================================

class TestPortalTargetBuild:
    def test_set_then_get_round_trip(self, engine):
        query.set_portal_target_build(engine, "hermes", GenomeBuild.hg19, "test")
        result = query.get_portal_target_build(engine, "hermes")
        assert result == GenomeBuild.hg19

    def test_set_overwrites_previous(self, engine):
        query.set_portal_target_build(engine, "hermes", GenomeBuild.hg19, "test")
        query.set_portal_target_build(engine, "hermes", GenomeBuild.grch38, "test2")
        result = query.get_portal_target_build(engine, "hermes")
        assert result == GenomeBuild.grch38

    def test_unknown_portal_returns_none(self, engine):
        result = query.get_portal_target_build(engine, "nonexistent-portal-xyz")
        assert result is None


class TestLiftoverJobQueries:
    def test_create_then_fetch_round_trip(self, engine):
        file_id = _make_file_id(engine)
        job_id = str(uuid.uuid4())
        query.create_liftover_job(
            engine,
            job_id,
            file_id,
            GenomeBuild.grch38,
            GenomeBuild.hg19,
            "s3://dig-data-registry/hermes/test-dataset/test.csv",
            "s3://dig-data-registry/hermes/liftover/unmapped.tsv",
            "tester@example.org",
        )
        fetched = query.fetch_liftover_job_by_file_id(engine, file_id)
        assert fetched is not None
        assert fetched.status == LiftoverJobStatus.SUBMITTED.value
        assert fetched.source_genome_build == GenomeBuild.grch38
        assert fetched.target_genome_build == GenomeBuild.hg19

    def test_update_liftover_job_complete(self, engine):
        file_id = _make_file_id(engine)
        job_id = str(uuid.uuid4())
        query.create_liftover_job(
            engine,
            job_id,
            file_id,
            GenomeBuild.grch38,
            GenomeBuild.hg19,
            "s3://dig-data-registry/hermes/test-dataset/test.csv",
            "s3://dig-data-registry/hermes/liftover/unmapped.tsv",
            "tester@example.org",
        )
        summary_data = {"total": 1000, "lifted": 990, "unmapped": 10}
        query.update_liftover_job(
            engine,
            job_id,
            status=LiftoverJobStatus.COMPLETE.value,
            log="job completed OK",
            summary=summary_data,
        )
        fetched = query.fetch_liftover_job_by_file_id(engine, file_id)
        assert fetched.status == LiftoverJobStatus.COMPLETE.value
        assert fetched.summary == summary_data
        assert fetched.completed_at is not None

    def test_update_liftover_job_failed(self, engine):
        file_id = _make_file_id(engine)
        job_id = str(uuid.uuid4())
        query.create_liftover_job(
            engine,
            job_id,
            file_id,
            GenomeBuild.grch38,
            GenomeBuild.hg19,
            "s3://dig-data-registry/hermes/test-dataset/test.csv",
            "s3://dig-data-registry/hermes/liftover/unmapped.tsv",
            "tester@example.org",
        )
        query.update_liftover_job(
            engine,
            job_id,
            status=LiftoverJobStatus.FAILED.value,
            log="something went wrong",
            summary=None,
        )
        fetched = query.fetch_liftover_job_by_file_id(engine, file_id)
        assert fetched.status == LiftoverJobStatus.FAILED.value
        assert fetched.summary is None
        assert fetched.completed_at is not None


class TestUpdateFileUploadAfterLiftover:
    def test_with_genome_build_flips_build(self, engine):
        file_id = _make_file_id(engine, genome_build="grch38")
        query.update_file_upload_after_liftover(
            engine,
            file_id,
            qc_status=HermesFileStatus.SUBMITTED_TO_QC,
            genome_build=GenomeBuild.hg19,
        )
        row = _fetch_file_row(engine, file_id)
        assert row.qc_status == HermesFileStatus.SUBMITTED_TO_QC.value
        assert row.genome_build == GenomeBuild.hg19.value

    def test_without_genome_build_leaves_build_unchanged(self, engine):
        file_id = _make_file_id(engine, genome_build="grch38")
        query.update_file_upload_after_liftover(
            engine,
            file_id,
            qc_status=HermesFileStatus.LIFTOVER_FAILED,
            genome_build=None,
        )
        row = _fetch_file_row(engine, file_id)
        assert row.qc_status == HermesFileStatus.LIFTOVER_FAILED.value
        assert row.genome_build == "grch38"


# ===========================================================================
# 3. Integration tests for _build_callback
# ===========================================================================

COLUMN_MAPPING = {"chromosome": "CHR", "position": "BP", "pValue": "P"}
QC_SCRIPT_OPTIONS = {"fd": 0.2}


class TestBuildCallback:
    def _setup(self, engine):
        """Create file_uploads + liftover_jobs rows, return (file_id, job_id)."""
        file_id = _make_file_id(
            engine,
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
            "s3://dig-data-registry/hermes/test-dataset/test.csv",
            "s3://dig-data-registry/hermes/liftover/unmapped.tsv",
            "tester@example.org",
        )
        return file_id, job_id

    def test_success_path(self, engine, monkeypatch):
        """SUCCEEDED + summary line → liftover COMPLETE, genome_build flipped, QC submitted."""
        file_id, job_id = self._setup(engine)

        mock_submit = MagicMock(return_value=None)
        monkeypatch.setattr("dataregistry.api.liftover.batch.submit_and_await_job", mock_submit)

        summary_payload = {"total": 1000, "lifted": 995, "unmapped": 5}
        log_str = f"some log\nLIFTOVER_SUMMARY_JSON: {json.dumps(summary_payload)}\ndone"

        callback = _build_callback(
            file_id=file_id,
            dataset="test-dataset",
            file_name="test.csv",
            column_mapping=COLUMN_MAPPING,
            qc_script_options=QC_SCRIPT_OPTIONS,
            target_build=GenomeBuild.hg19,
        )
        callback(engine, log_str, job_id, "SUCCEEDED")

        # liftover_jobs row
        lj = query.fetch_liftover_job_by_file_id(engine, file_id)
        assert lj.status == LiftoverJobStatus.COMPLETE.value
        assert lj.summary == summary_payload
        assert lj.completed_at is not None

        # file_uploads row
        row = _fetch_file_row(engine, file_id)
        assert row.qc_status == HermesFileStatus.SUBMITTED_TO_QC.value
        assert row.genome_build == GenomeBuild.hg19.value

        # QC submission was called once
        mock_submit.assert_called_once()
        call_kwargs = mock_submit.call_args
        job_config_arg = call_kwargs[0][1]  # second positional arg is job_config
        assert job_config_arg["jobName"] == "hermes-qc-job"
        assert job_config_arg["jobQueue"] == "hermes-qc-job-queue"

    def test_success_path_no_summary_line(self, engine, monkeypatch):
        """SUCCEEDED but log has no LIFTOVER_SUMMARY_JSON line → summary stays None."""
        file_id, job_id = self._setup(engine)

        mock_submit = MagicMock(return_value=None)
        monkeypatch.setattr("dataregistry.api.liftover.batch.submit_and_await_job", mock_submit)

        callback = _build_callback(
            file_id=file_id,
            dataset="test-dataset",
            file_name="test.csv",
            column_mapping=COLUMN_MAPPING,
            qc_script_options=QC_SCRIPT_OPTIONS,
            target_build=GenomeBuild.hg19,
        )
        callback(engine, "log with no summary", job_id, "SUCCEEDED")

        lj = query.fetch_liftover_job_by_file_id(engine, file_id)
        assert lj.status == LiftoverJobStatus.COMPLETE.value
        assert lj.summary is None

        # QC was still called
        mock_submit.assert_called_once()

    def test_failure_path(self, engine, monkeypatch):
        """FAILED → liftover FAILED, genome_build NOT flipped, qc_status=LIFTOVER FAILED, QC not called."""
        file_id, job_id = self._setup(engine)

        mock_submit = MagicMock()
        monkeypatch.setattr("dataregistry.api.liftover.batch.submit_and_await_job", mock_submit)

        callback = _build_callback(
            file_id=file_id,
            dataset="test-dataset",
            file_name="test.csv",
            column_mapping=COLUMN_MAPPING,
            qc_script_options=QC_SCRIPT_OPTIONS,
            target_build=GenomeBuild.hg19,
        )
        callback(engine, "liftover failed log", job_id, "FAILED")

        # liftover_jobs row
        lj = query.fetch_liftover_job_by_file_id(engine, file_id)
        assert lj.status == LiftoverJobStatus.FAILED.value
        assert lj.completed_at is not None

        # file_uploads row — genome_build must NOT flip
        row = _fetch_file_row(engine, file_id)
        assert row.genome_build == "grch38"
        assert row.qc_status == HermesFileStatus.LIFTOVER_FAILED.value

        mock_submit.assert_not_called()

    def test_success_qc_submit_raises(self, engine, monkeypatch):
        """SUCCEEDED but nested QC submit raises → qc_status=FAILED TO SUBMIT TO QC, exception re-raised."""
        file_id, job_id = self._setup(engine)

        mock_submit = MagicMock(side_effect=RuntimeError("batch unavailable"))
        monkeypatch.setattr("dataregistry.api.liftover.batch.submit_and_await_job", mock_submit)

        summary_payload = {"total": 100, "lifted": 90, "unmapped": 10}
        log_str = f"LIFTOVER_SUMMARY_JSON: {json.dumps(summary_payload)}"

        callback = _build_callback(
            file_id=file_id,
            dataset="test-dataset",
            file_name="test.csv",
            column_mapping=COLUMN_MAPPING,
            qc_script_options=QC_SCRIPT_OPTIONS,
            target_build=GenomeBuild.hg19,
        )

        with pytest.raises(RuntimeError, match="batch unavailable"):
            callback(engine, log_str, job_id, "SUCCEEDED")

        # qc_status must be set to FAILED TO SUBMIT TO QC
        row = _fetch_file_row(engine, file_id)
        assert row.qc_status == HermesFileStatus.SUBMISSION_TO_QC_FAILED.value
