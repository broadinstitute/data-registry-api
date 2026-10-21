import contextlib
import os
import shutil
import signal
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from sgc_munge import munge
from tests.sgc_munge.fixtures import MAP, requires_bcftools

pytestmark = requires_bcftools

FID = "f" * 32
PREFIX = "sgc/munge/" + FID


def _setup(monkeypatch, fake_munge):
    calls = []
    monkeypatch.setattr(munge, "_get_engine", lambda: "ENG")
    monkeypatch.setattr(munge.munge_db, "update_sgc_munge_result",
                        lambda eng, fid, **kw: calls.append(kw))
    s3 = MagicMock()
    monkeypatch.setattr(munge.boto3, "client", lambda *a, **k: s3)
    monkeypatch.setattr(munge, "download_refs", lambda s3, p, d: ("fa", "db", "ref-v1"))
    monkeypatch.setattr(munge, "Reference", lambda fa, db: contextlib.nullcontext("REF"))
    monkeypatch.setattr(munge, "munge_file", fake_munge)
    return calls, s3


def _run_one():
    munge.run_one(s3_path="sgc/gwas/x.gz", column_mapping=MAP, bucket="b", file_id=FID,
                  output_prefix=PREFIX, ref_prefix="s3://r/p/", cases=1, controls=2,
                  maf_min=0.005, info_min=0.3, batch_job_id="job1")


def test_run_one_success_updates_and_uploads(monkeypatch):
    def fake_munge(in_path, cm, ref, out_dir, **kw):
        assert in_path.endswith("x.gz")
        os.makedirs(out_dir, exist_ok=True)  # the real munge_file creates it
        Path(out_dir, "standardised.tsv.gz").write_bytes(b"x")
        Path(out_dir, "munge.json").write_text("{}")
        return {"n_in": 3, "n_out": 2, "n_with_rsid": 1, "n_swapped": 0, "n_dropped": {"maf": 1},
                "has_eaf": True, "has_info": False, "engine": munge.ENGINE,
                "ref_version": kw["ref_version"]}
    calls, s3 = _setup(monkeypatch, fake_munge)
    _run_one()
    assert calls[0] == {"status": "RUNNING", "batch_job_id": "job1", "clear_error": True}
    final = calls[-1]
    assert final["status"] == "SUCCEEDED"
    assert final["standardised_s3_key"] == PREFIX + "/standardised.tsv.gz"
    assert final["munge_json_s3_key"] == PREFIX + "/munge.json"
    assert final["ref_mismatch_s3_key"] is None and final["ref_version"] == "ref-v1"
    assert final["engine"] == munge.ENGINE
    assert final["n_dropped"] == {"maf": 1} and final["maf_min"] == 0.005
    uploaded = {c.args[2] for c in s3.upload_file.call_args_list}
    assert uploaded == {PREFIX + "/standardised.tsv.gz", PREFIX + "/munge.json"}
    types = {c.args[2]: c.kwargs["ExtraArgs"]["ContentType"] for c in s3.upload_file.call_args_list}
    assert types[PREFIX + "/standardised.tsv.gz"] == "application/gzip"
    assert types[PREFIX + "/munge.json"] == "application/json"


def test_run_one_uploads_mismatch_sidecar(monkeypatch):
    def fake_munge(in_path, cm, ref, out_dir, **kw):
        os.makedirs(out_dir, exist_ok=True)
        for n in ("standardised.tsv.gz", "munge.json", "ref_mismatch.tsv.gz"):
            Path(out_dir, n).write_bytes(b"x")
        return {"n_in": 1, "n_out": 1, "n_with_rsid": 0, "n_swapped": 0, "n_dropped": {},
                "has_eaf": False, "has_info": False, "engine": munge.ENGINE, "ref_version": "v"}
    calls, s3 = _setup(monkeypatch, fake_munge)
    _run_one()
    assert calls[-1]["ref_mismatch_s3_key"] == PREFIX + "/ref_mismatch.tsv.gz"
    assert len(s3.upload_file.call_args_list) == 3


def test_run_one_failure_marks_failed_and_reraises(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("boom")
    calls, _ = _setup(monkeypatch, boom)
    with pytest.raises(RuntimeError, match="boom"):
        _run_one()
    assert calls[0]["status"] == "RUNNING"
    assert calls[-1]["status"] == "FAILED"
    assert calls[-1]["error_message"].startswith("RuntimeError: boom")


def test_run_one_bad_mapping_exits_2(monkeypatch):
    def bad(*a, **k):
        raise ValueError("column_mapping missing required keys: ['col_se']")
    calls, _ = _setup(monkeypatch, bad)
    with pytest.raises(SystemExit) as ei:
        _run_one()
    assert ei.value.code == 2
    assert calls[-1]["status"] == "FAILED"
    assert "col_se" in calls[-1]["error_message"]


def test_run_one_sigterm_marks_failed_before_exit(monkeypatch):
    def killed(*a, **k):
        signal.raise_signal(signal.SIGTERM)   # what Batch sends on timeout / Spot reclaim
        raise AssertionError("not reached")
    before = signal.getsignal(signal.SIGTERM)
    calls, _ = _setup(monkeypatch, killed)
    with pytest.raises(RuntimeError, match="terminated by SIGTERM"):
        _run_one()
    assert calls[-1]["status"] == "FAILED"
    assert calls[-1]["error_message"].startswith(
        "RuntimeError: terminated by SIGTERM (Batch timeout or Spot reclaim)")
    assert signal.getsignal(signal.SIGTERM) == before   # handler restored


def test_run_one_wrong_build_exits_2(monkeypatch, tmp_path):
    # real munge_file + Reference: the wrong-build guard takes the bad-mapping path
    import random
    from tests.sgc_munge.fixtures import BASE_AT, _gwas_row, build_reference, write_gwas
    rng = random.Random(5)
    rows = [_gwas_row("1", 1 + i % len(BASE_AT["1"]), *rng.sample("ACGT", 2))
            for i in range(1200)]
    gwas = write_gwas(tmp_path, rows)
    fa, db = build_reference(tmp_path)
    calls = []
    monkeypatch.setattr(munge, "_get_engine", lambda: "ENG")
    monkeypatch.setattr(munge.munge_db, "update_sgc_munge_result",
                        lambda eng, fid, **kw: calls.append(kw))
    s3 = MagicMock()
    s3.download_file.side_effect = lambda bucket, key, local: shutil.copy(gwas, local)
    monkeypatch.setattr(munge.boto3, "client", lambda *a, **k: s3)
    monkeypatch.setattr(munge, "download_refs", lambda s3, p, d: (fa, db, "ref-v1"))
    with pytest.raises(SystemExit) as ei:
        _run_one()
    assert ei.value.code == 2
    assert calls[-1]["status"] == "FAILED"
    assert "suggests the file is not GRCh38" in calls[-1]["error_message"]
    s3.upload_file.assert_not_called()


def test_parse_s3_prefix():
    assert munge.parse_s3_prefix("s3://dig-data-registry/sgc/ref/grch38-dbsnp155/") == (
        "dig-data-registry", "sgc/ref/grch38-dbsnp155/")


@pytest.mark.parametrize("prefix", ["s3://r/p", "s3://r/p/", "s3://r/p//"])
def test_parse_s3_prefix_normalises_trailing_slash(prefix):
    assert munge.parse_s3_prefix(prefix) == ("r", "p/")


def test_parse_s3_prefix_bucket_only():
    assert munge.parse_s3_prefix("s3://r") == munge.parse_s3_prefix("s3://r/") == ("r", "")


def test_download_refs_same_keys_with_or_without_trailing_slash(tmp_path):
    def keys(prefix):
        s3 = MagicMock()
        s3.download_file.side_effect = lambda bucket, key, local: Path(local).write_text(
            '{"ref_version":"v9"}' if local.endswith("MANIFEST.json") else "")
        munge.download_refs(s3, prefix, str(tmp_path))
        return [c.args[:2] for c in s3.download_file.call_args_list]
    assert keys("s3://r/p") == keys("s3://r/p/") == [("r", "p/" + f) for f in munge.REF_FILES]


def test_download_refs_returns_paths_and_version(tmp_path):
    s3 = MagicMock()

    def dl(bucket, key, local):
        Path(local).write_text('{"ref_version":"v9"}' if local.endswith("MANIFEST.json") else "")
    s3.download_file.side_effect = dl
    fa, db, ver = munge.download_refs(s3, "s3://r/p/", str(tmp_path))
    assert fa.endswith("GRCh38.primary.fa.gz") and db.endswith("dbsnp155.grch38.slim.vcf.gz")
    assert ver == "v9"
    assert {c.args[1] for c in s3.download_file.call_args_list} == {"p/" + f for f in munge.REF_FILES}


def test_main_cases_na_becomes_none(monkeypatch):
    seen = {}
    monkeypatch.setattr(munge, "run_one", lambda **kw: seen.update(kw))
    res = CliRunner().invoke(munge.main, [
        "--s3-path", "a", "--column-mapping", "{}", "--bucket", "b", "--file-id", "f",
        "--output-prefix", "o", "--ref-prefix", "s3://r/p/", "--cases", "NA", "--controls", "12"])
    assert res.exit_code == 0, res.output
    assert seen["cases"] is None and seen["controls"] == 12 and seen["maf_min"] == 0.005
