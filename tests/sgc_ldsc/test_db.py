from unittest.mock import MagicMock
from dataregistry.api import query


def _capture():
    eng = MagicMock()
    conn = eng.connect.return_value.__enter__.return_value
    conn.execute.return_value.rowcount = 1
    return eng, conn


def test_update_sgc_ldsc_pending_only_touches_ldsc_columns():
    eng, conn = _capture()
    query.update_sgc_ldsc_pending(eng, "abc123")
    sql = conn.execute.call_args[0][0].text
    assert "ldsc_status" in sql and "ldsc_intercept" in sql
    assert "lambda_gc" not in sql and "manhattan_s3_key" not in sql
    assert " status " not in sql  # never the QC status column


def test_update_sgc_ldsc_result_writes_metrics():
    eng, conn = _capture()
    query.update_sgc_ldsc_result(eng, "abc123", ldsc_status="SUCCEEDED",
                                 ldsc_intercept=1.03, ldsc_h2=0.21, ldsc_ratio=0.1,
                                 ldsc_effective_n=12345.0, ldsc_n_snps=900000)
    params = conn.execute.call_args[0][1]
    assert params["ldsc_status"] == "SUCCEEDED"
    assert params["ldsc_intercept"] == 1.03
    assert params["ldsc_h2"] == 0.21
    assert params["ldsc_ratio"] == 0.1
    assert params["ldsc_effective_n"] == 12345.0
    assert params["ldsc_n_snps"] == 900000
