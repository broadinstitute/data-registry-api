import uuid

import pytest
from fastapi.testclient import TestClient

from dataregistry.api import query
from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api.model import SGCCohort, SGCGWASFile
from sqlalchemy import text


def _make_gwas_file(cohort_id: str) -> SGCGWASFile:
    return SGCGWASFile(
        cohort_id=cohort_id,
        dataset="test_dataset",
        phenotype="T2D",
        ancestry="EUR",
        file_name="gwas.tsv",
        file_size=1024,
        s3_path="s3://bucket/gwas.tsv",
        uploaded_by="testuser",
        column_mapping={"chr": "chromosome", "pos": "position"},
    )


def _setup_cohort_and_file(engine):
    """Create an sgc_cohorts row and an sgc_gwas_files row; return (cohort_id, file_id)."""
    cohort = SGCCohort(
        name="QC Plot Test Cohort",
        uploaded_by="testuser",
        total_sample_size=500,
        number_of_males=250,
        number_of_females=250,
    )
    cohort_id = query.upsert_sgc_cohort(engine, cohort)
    file_id = query.insert_sgc_gwas_file(engine, _make_gwas_file(cohort_id))
    return cohort_id, file_id


def _truncate_plot_tables(engine):
    with engine.connect() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 0"))
        conn.execute(text("TRUNCATE TABLE sgc_gwas_plot_results"))
        conn.execute(text("TRUNCATE TABLE sgc_gwas_files"))
        conn.execute(text("TRUNCATE TABLE sgc_gwas_cohorts"))
        conn.commit()
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 1"))


def test_insert_pending_creates_row(api_client: TestClient):
    """insert_sgc_plot_result_pending creates one PENDING row joined to the file."""
    engine = DataRegistryReadWriteDB().get_engine()
    _truncate_plot_tables(engine)

    cohort_id, file_id = _setup_cohort_and_file(engine)

    plot_id = query.insert_sgc_plot_result_pending(engine, file_id)

    assert plot_id is not None
    assert len(plot_id) == 32  # UUID without dashes

    results = query.get_sgc_plot_results(engine)
    assert len(results) == 1

    row = results[0]
    assert row["status"] == "PENDING"
    assert row["id"] == plot_id
    assert row["file_id"] == file_id
    assert row["dataset"] == "test_dataset"
    assert row["phenotype"] == "T2D"
    assert row["ancestry"] == "EUR"
    assert row["lambda_gc"] is None


def test_insert_pending_is_idempotent(api_client: TestClient):
    """Calling insert_sgc_plot_result_pending twice for the same file_id yields only one row."""
    engine = DataRegistryReadWriteDB().get_engine()
    _truncate_plot_tables(engine)

    _, file_id = _setup_cohort_and_file(engine)

    query.insert_sgc_plot_result_pending(engine, file_id)
    query.insert_sgc_plot_result_pending(engine, file_id)

    results = query.get_sgc_plot_results(engine)
    assert len(results) == 1
    assert results[0]["status"] == "PENDING"


def test_update_plot_result_succeeded(api_client: TestClient):
    """After insert_pending, updating to SUCCEEDED persists all provided fields."""
    engine = DataRegistryReadWriteDB().get_engine()
    _truncate_plot_tables(engine)

    _, file_id = _setup_cohort_and_file(engine)
    query.insert_sgc_plot_result_pending(engine, file_id)

    query.update_sgc_plot_result(
        engine,
        file_id,
        status="SUCCEEDED",
        lambda_gc=1.04,
        n_variants=10000,
        n_sig_5e8=5,
        n_sig_1e5=50,
        manhattan_s3_key="s3://bucket/manhattan.png",
        qq_s3_key="s3://bucket/qq.png",
    )

    results = query.get_sgc_plot_results(engine)
    assert len(results) == 1

    row = results[0]
    assert row["status"] == "SUCCEEDED"
    assert abs(row["lambda_gc"] - 1.04) < 1e-9
    assert row["n_variants"] == 10000
    assert row["n_sig_5e8"] == 5
    assert row["n_sig_1e5"] == 50
    assert row["manhattan_s3_key"] == "s3://bucket/manhattan.png"
    assert row["qq_s3_key"] == "s3://bucket/qq.png"


def test_update_plot_result_partial_update(api_client: TestClient):
    """COALESCE logic: re-updating with only status preserves existing numeric fields."""
    engine = DataRegistryReadWriteDB().get_engine()
    _truncate_plot_tables(engine)

    _, file_id = _setup_cohort_and_file(engine)
    query.insert_sgc_plot_result_pending(engine, file_id)

    # First update: set SUCCEEDED with data
    query.update_sgc_plot_result(
        engine,
        file_id,
        status="SUCCEEDED",
        lambda_gc=1.04,
        n_variants=10000,
        manhattan_s3_key="s3://bucket/manhattan.png",
        qq_s3_key="s3://bucket/qq.png",
    )

    # Second update: only change status back to RUNNING — numeric fields must survive
    query.update_sgc_plot_result(engine, file_id, status="RUNNING")

    results = query.get_sgc_plot_results(engine)
    assert len(results) == 1

    row = results[0]
    assert row["status"] == "RUNNING"
    assert abs(row["lambda_gc"] - 1.04) < 1e-9
    assert row["n_variants"] == 10000
    assert row["manhattan_s3_key"] == "s3://bucket/manhattan.png"
    assert row["qq_s3_key"] == "s3://bucket/qq.png"


def test_update_plot_result_unknown_file_raises(api_client: TestClient):
    """update_sgc_plot_result raises ValueError when no row exists for the given file_id."""
    engine = DataRegistryReadWriteDB().get_engine()

    unknown_file_id = uuid.uuid4().hex  # random hex; guaranteed not in DB

    with pytest.raises(ValueError):
        query.update_sgc_plot_result(engine, unknown_file_id, status="RUNNING")
