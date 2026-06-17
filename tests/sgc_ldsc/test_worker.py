from unittest.mock import patch

import pytest

import sgc_ldsc.ldsc_worker as w


@patch("sgc_ldsc.ldsc_worker.boto3")
@patch("sgc_ldsc.ldsc_worker._ensure_reference")
@patch("sgc_ldsc.ldsc_worker.query.update_sgc_ldsc_result")
@patch("sgc_ldsc.ldsc_worker._compute_for_file")
@patch("sgc_ldsc.ldsc_worker._get_engine")
def test_run_one_writes_succeeded_with_metrics(_eng, compute, upd, _ref, _boto3):
    compute.return_value = {"intercept": 1.04, "h2": 0.2, "ratio": 0.12,
                            "effective_n": 9999.0, "n_snps": 800000}
    w.run_one(s3_path="s3path", column_mapping={"col_chromosome": "CHR"},
              bucket="b", file_id="fid", ancestry="EUR", genome_build="GRCh38",
              ref_bucket="ref", batch_job_id="J1")
    statuses = [c.kwargs["ldsc_status"] for c in upd.call_args_list]
    assert statuses[0] == "RUNNING" and statuses[-1] == "SUCCEEDED"
    assert upd.call_args_list[-1].kwargs["ldsc_intercept"] == 1.04


@patch("sgc_ldsc.ldsc_worker.boto3")
@patch("sgc_ldsc.ldsc_worker._ensure_reference")
@patch("sgc_ldsc.ldsc_worker.query.update_sgc_ldsc_result")
@patch("sgc_ldsc.ldsc_worker._compute_for_file", side_effect=ValueError("too few SNPs"))
@patch("sgc_ldsc.ldsc_worker._get_engine")
def test_run_one_writes_failed_on_error(_eng, _c, upd, _ref, _boto3):
    with pytest.raises(ValueError):
        w.run_one(s3_path="s", column_mapping={}, bucket="b", file_id="fid",
                  ancestry="EUR", genome_build="GRCh38", ref_bucket="ref")
    assert upd.call_args_list[-1].kwargs["ldsc_status"] == "FAILED"
    assert "too few SNPs" in upd.call_args_list[-1].kwargs["ldsc_error"]
