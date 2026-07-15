from unittest.mock import patch, MagicMock
import sgc_ldsc.submit_ldsc_batch as s


def test_list_files_query_excludes_combined_mid_and_requires_qc_row():
    sql = s.LIST_SQL
    assert "ldsc" not in sql.lower() or "ldsc_status" in sql  # selects only candidates
    assert "NOT IN ('Combined','MID')" in sql
    assert "JOIN sgc_gwas_plot_results" in sql           # must already be QC'd
    assert "sgc_gwas_cohorts" in sql                     # genome_build join


@patch("sgc_ldsc.submit_ldsc_batch.boto3")
@patch("sgc_ldsc.submit_ldsc_batch.query.update_sgc_ldsc_result")
@patch("sgc_ldsc.submit_ldsc_batch.query.update_sgc_ldsc_pending")
def test_submit_stamps_pending_then_jobid(pending, result, boto3):
    boto3.client.return_value.submit_job.return_value = {"jobId": "J9"}
    s._submit_one(MagicMock(), boto3.client.return_value,
                  {"file_id": "fid", "s3_path": "p", "column_mapping": "{}",
                   "ancestry": "EUR", "genome_build": "GRCh38", "phenotype": "X"},
                  bucket="b", ref_bucket="r", db_name="dataregistry")
    pending.assert_called_once()
    assert result.call_args.kwargs["ldsc_batch_job_id"] == "J9"


def test_normalize_build_maps_free_text_to_canonical():
    assert s._normalize_build("GRCh38") == "GRCh38"
    assert s._normalize_build("GRCh37") == "GRCh37"
    assert s._normalize_build("GRCh38 / hg38") == "GRCh38"
    assert s._normalize_build("GRCh37 liftover to GRCh38") == "GRCh38"
    # unknown passes through so the build skip-check in main() still rejects it
    assert s._normalize_build("hg19") == "hg19"


def test_list_sql_is_resumable_and_skips_succeeded():
    sql = s.LIST_SQL
    assert "ldsc_status" in sql
    norm = sql.replace(" ", "")
    assert "ldsc_statusISNULL" in norm          # re-picks never-run rows
    assert "'PENDING','FAILED'" in norm         # re-picks unfinished work
    assert "SUCCEEDED" not in sql               # SUCCEEDED is excluded, never selected
