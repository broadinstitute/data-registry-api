import json

import pytest

from dataregistry.api import hcm_query
from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api.hcm_model import HCMGWASFile


def _make_file(engine, genome_build="GRCh37", **overrides) -> str:
    """Insert a hcm_gwas_files row, return the file id (dashless hex)."""
    kwargs = dict(
        cohort_name="LiftTestCohort", sarc="ALL", ancestry="EUR", sex="ALL",
        genome_build=genome_build, software="REGENIE", analyst="tester",
        file_name="f.tsv", file_size=10, s3_path="hcm/gwas/x/f.tsv",
        uploaded_by="tester",
        column_mapping={"chromosome": "CHR", "position": "POS",
                        "effect_allele": "A1", "non_effect_allele": "A2"},
    )
    kwargs.update(overrides)
    return hcm_query.insert_hcm_gwas_file(engine, HCMGWASFile(**kwargs))


def test_to_worker_column_mapping():
    from hcm_liftover.submit_liftover_batch import to_worker_column_mapping
    cm = {"chromosome": "CHR", "position": "POS",
          "effect_allele": "A1", "non_effect_allele": "A2"}
    assert to_worker_column_mapping(cm) == {
        "chromosome": "CHR", "position": "POS", "ref": "A2", "alt": "A1"}


def test_to_worker_column_mapping_missing_key_raises():
    from hcm_liftover.submit_liftover_batch import to_worker_column_mapping
    with pytest.raises(ValueError):
        to_worker_column_mapping({"chromosome": "CHR"})   # missing pos/alleles


def test_ucsc_source_build():
    from hcm_liftover.submit_liftover_batch import ucsc_source_build
    assert ucsc_source_build("GRCh37") == "hg19"
    with pytest.raises(ValueError):
        ucsc_source_build("GRCh38")   # already target -- should never be lifted
    with pytest.raises(ValueError):
        ucsc_source_build(None)


def test_plan_liftover_wave_caps_and_reports_remaining():
    from hcm_liftover.submit_liftover_batch import plan_liftover_wave
    wave, remaining = plan_liftover_wave(list(range(120)), 50)
    assert wave == list(range(50))
    assert remaining == 70


def test_select_liftable_finds_grch37_without_job(api_client):
    from hcm_liftover.submit_liftover_batch import select_liftable
    engine = DataRegistryReadWriteDB().get_engine()
    file_id = _make_file(engine, genome_build="GRCh37")

    liftable, unrecognized = select_liftable(engine, None)

    assert [f["file_id"] for f in liftable] == [file_id]
    assert unrecognized == []


def test_select_liftable_excludes_file_with_succeeded_job(api_client):
    from hcm_liftover.submit_liftover_batch import select_liftable
    engine = DataRegistryReadWriteDB().get_engine()
    file_id = _make_file(engine, genome_build="GRCh37")
    lid = hcm_query.insert_hcm_liftover_pending(
        engine, file_id, "hg19", "hg38",
        "s3://b/archive/f.tsv", "s3://b/unmapped.tsv", "tester")
    hcm_query.update_hcm_liftover_job(engine, lid, status="SUCCEEDED", completed=True)

    liftable, unrecognized = select_liftable(engine, None)

    assert file_id not in [f["file_id"] for f in liftable]


def test_select_liftable_excludes_grch38_file(api_client):
    from hcm_liftover.submit_liftover_batch import select_liftable
    engine = DataRegistryReadWriteDB().get_engine()
    file_id = _make_file(engine, genome_build="GRCh38")

    liftable, unrecognized = select_liftable(engine, None)

    assert file_id not in [f["file_id"] for f in liftable]
    assert file_id not in [f["file_id"] for f in unrecognized]


def test_make_liftover_callback_flips_file_build_on_success(api_client):
    from hcm_liftover.submit_liftover_batch import make_liftover_callback
    engine = DataRegistryReadWriteDB().get_engine()
    file_id = _make_file(engine, genome_build="GRCh37")
    lid = hcm_query.insert_hcm_liftover_pending(
        engine, file_id, "hg19", "hg38",
        "s3://b/archive/f.tsv", "s3://b/unmapped.tsv", "tester")

    complete_log = "LIFTOVER_SUMMARY_JSON: " + json.dumps({"total_lifted": 5})
    cb = make_liftover_callback(file_id)
    cb(engine, complete_log, lid, "SUCCEEDED")

    job = hcm_query.get_hcm_liftover_job_for_file(engine, file_id)
    assert job["status"] == "SUCCEEDED"
    assert job["summary"] == {"total_lifted": 5}

    from sqlalchemy import text
    with engine.connect() as c:
        build = c.execute(text(
            "SELECT genome_build FROM hcm_gwas_files WHERE id = :id"),
            {"id": file_id}).scalar()
    assert build == "GRCh38"


def test_make_liftover_callback_does_not_flip_build_on_failure(api_client):
    from hcm_liftover.submit_liftover_batch import make_liftover_callback
    engine = DataRegistryReadWriteDB().get_engine()
    file_id = _make_file(engine, genome_build="GRCh37")
    lid = hcm_query.insert_hcm_liftover_pending(
        engine, file_id, "hg19", "hg38",
        "s3://b/archive/f.tsv", "s3://b/unmapped.tsv", "tester")

    cb = make_liftover_callback(file_id)
    cb(engine, "boom", lid, "FAILED")

    from sqlalchemy import text
    with engine.connect() as c:
        build = c.execute(text(
            "SELECT genome_build FROM hcm_gwas_files WHERE id = :id"),
            {"id": file_id}).scalar()
    assert build == "GRCh37"   # unchanged

    job = hcm_query.get_hcm_liftover_job_for_file(engine, file_id)
    assert job["status"] == "FAILED"
