from unittest.mock import MagicMock
import json, pytest
from dataregistry.api import query

def _engine_capturing():
    conn = MagicMock(); conn.execute.return_value.rowcount = 1
    eng = MagicMock(); eng.connect.return_value.__enter__.return_value = conn
    return eng, conn

def test_update_writes_only_non_none_and_json_dumps_n_dropped():
    eng, conn = _engine_capturing()
    query.update_sgc_munge_result(eng, "f"*32, status="SUCCEEDED", n_in=10, n_dropped={"maf": 2}, error_message=None)
    sql = str(conn.execute.call_args.args[0]); params = conn.execute.call_args.args[1]
    assert "n_in = :n_in" in sql and "n_dropped = :n_dropped" in sql and "error_message" not in sql
    assert params["n_dropped"] == json.dumps({"maf": 2}) and params["status"] == "SUCCEEDED"

def test_update_accepts_engine_field():
    eng, conn = _engine_capturing()
    query.update_sgc_munge_result(eng, "f"*32, status="SUCCEEDED", engine="x")
    sql = str(conn.execute.call_args.args[0]); params = conn.execute.call_args.args[1]
    assert "engine = :engine" in sql and params["engine"] == "x"

def test_update_unknown_field_rejected():
    eng, _ = _engine_capturing()
    with pytest.raises(ValueError):
        query.update_sgc_munge_result(eng, "f"*32, status="RUNNING", bogus=1)

def test_update_zero_rows_raises():
    eng, conn = _engine_capturing(); conn.execute.return_value.rowcount = 0
    with pytest.raises(ValueError, match="No sgc_gwas_munge_results row"):
        query.update_sgc_munge_result(eng, "f"*32, status="RUNNING")

def test_insert_pending_is_upsert_reset():
    eng, conn = _engine_capturing()
    query.insert_sgc_munge_result_pending(eng, "f"*32)
    sql = str(conn.execute.call_args.args[0])
    assert "ON DUPLICATE KEY UPDATE" in sql and "standardised_s3_key=NULL" in sql.replace(" ", "") and "status='PENDING'" in sql.replace(" ", "")

def test_format_row_decodes_json_and_ids():
    d = query._format_sgc_munge_row({"id": b"a"*32, "file_id": b"b"*32, "n_dropped": '{"maf": 1}'})
    assert d["id"] == "a"*32 and d["n_dropped"] == {"maf": 1}

def test_update_clear_error_sets_error_message_null():
    eng, conn = _engine_capturing()
    query.update_sgc_munge_result(eng, "f"*32, status="RUNNING", batch_job_id="j", clear_error=True)
    sql = str(conn.execute.call_args.args[0]); params = conn.execute.call_args.args[1]
    assert "error_message = NULL" in sql and "error_message" not in params
    assert "batch_job_id = :batch_job_id" in sql

def test_update_without_clear_error_leaves_error_message():
    eng, conn = _engine_capturing()
    query.update_sgc_munge_result(eng, "f"*32, status="RUNNING", batch_job_id="j")
    assert "error_message" not in str(conn.execute.call_args.args[0])

def test_update_clear_error_with_error_message_rejected():
    eng, _ = _engine_capturing()
    with pytest.raises(ValueError, match="clear_error"):
        query.update_sgc_munge_result(eng, "f"*32, status="FAILED", error_message="x", clear_error=True)
