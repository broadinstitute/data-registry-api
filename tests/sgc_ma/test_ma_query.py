import pytest
from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api import query
from dataregistry.api.query import MA_LIST_SQL


@pytest.fixture
def engine():
    return DataRegistryReadWriteDB().get_engine()


def test_ma_list_sql_selects_all():
    assert "FROM sgc_gwas_ma_results" in MA_LIST_SQL


def test_ma_list_sql_selects_totals():
    assert "total_cases" in MA_LIST_SQL
    assert "total_controls" in MA_LIST_SQL


def test_sgc_ma_result_model_has_totals():
    from dataregistry.api.model import SGCMAResult
    m = SGCMAResult(id="x", phenotype="p", ancestry="a", status="SUCCEEDED",
                    total_cases=150, total_controls=300)
    assert m.total_cases == 150 and m.total_controls == 300


def test_insert_sgc_ma_run_creates_distinct_rows(engine):
    r1 = query.insert_sgc_ma_run(engine, "PH", "EUR", run_type="manual",
                                 dataset_file_ids=["a", "b"], maf_min=0.005, info_min=0.3,
                                 submitted_by="rev", label="first")
    r2 = query.insert_sgc_ma_run(engine, "PH", "EUR", run_type="manual",
                                 dataset_file_ids=["a", "c"], maf_min=0.01, info_min=0.3,
                                 submitted_by="rev", label="second")
    assert r1 != r2                       # no upsert -> two rows for same pheno/anc
    got = query.get_sgc_ma_run(engine, r1)
    assert got["run_type"] == "manual" and got["label"] == "first"
    assert got["dataset_file_ids"] == ["a", "b"]     # JSON round-trips to a list
    assert got["maf_min"] == 0.005 and got["info_min"] == 0.3
    assert got["sex"] == "All"            # default when omitted


def test_insert_sgc_ma_run_sex_round_trips(engine):
    # nine-bucket CHECK: sex != 'All' requires ancestry = 'Combined'
    run_id = query.insert_sgc_ma_run(engine, "PH", "Combined", sex="Male")
    assert query.get_sgc_ma_run(engine, run_id)["sex"] == "Male"


def test_update_sgc_ma_result_targets_run_id(engine):
    r1 = query.insert_sgc_ma_run(engine, "PH2", "EAS")
    r2 = query.insert_sgc_ma_run(engine, "PH2", "EAS")
    query.update_sgc_ma_result(engine, r1, status="SUCCEEDED", n_meta_variants=42)
    assert query.get_sgc_ma_run(engine, r1)["status"] == "SUCCEEDED"
    assert query.get_sgc_ma_run(engine, r2)["status"] == "PENDING"   # sibling untouched


def test_get_sgc_ma_run_missing_returns_none(engine):
    assert query.get_sgc_ma_run(engine, "0" * 32) is None
