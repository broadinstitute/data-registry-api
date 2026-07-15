import json
import pandas as pd
from sgc_ma.run_ma import meta_analyze

def _norm(rows):
    return pd.DataFrame(rows)

def test_meta_analyze_end_to_end(tmp_path):
    cohorts = [{"file_id": "a", "dataset": "A", "cases": 1, "controls": 1},
               {"file_id": "b", "dataset": "B", "cases": 1, "controls": 1},
               {"file_id": "c", "dataset": "C", "cases": 1, "controls": 1}]
    # same variant in all 3 cohorts, consistent direction; one singleton
    base = dict(se=0.1, pvalue=0.01, eaf=0.3, n=1000)
    frames = {
        "a": _norm([dict(chromosome="1", position=100, ea="G", oa="A", beta=0.2, **base)]),
        "b": _norm([dict(chromosome="1", position=100, ea="A", oa="G", beta=-0.2, **base)]),
        "c": _norm([dict(chromosome="1", position=100, ea="G", oa="A", beta=0.2, **base),
                    dict(chromosome="2", position=50, ea="C", oa="A", beta=0.9, **base)]),
    }
    # chunks_fn yields one chunk (a one-element list) per cohort
    summary = meta_analyze(cohorts, lambda co: [frames[co["file_id"]]], str(tmp_path))
    meta = pd.read_csv(tmp_path / "meta.tsv.gz", sep="\t")
    assert len(meta) == 1                     # only the shared variant (>=2 cohorts)
    assert meta.iloc[0]["n_cohorts"] == 3
    assert abs(meta.iloc[0]["beta"] - 0.2) < 1e-9   # all agree after harmonization
    assert (tmp_path / "manhattan.png").exists() and (tmp_path / "qq.png").exists()
    assert summary["n_cohorts"] == 3 and summary["n_meta_variants"] == 1
    assert json.loads((tmp_path / "summary.json").read_text())["per_cohort"][0]["dataset"] == "A"

def test_meta_analyze_empty_when_no_shared_variants(tmp_path):
    cohorts = [{"file_id": "a", "dataset": "A", "cases": 1, "controls": 1},
               {"file_id": "b", "dataset": "B", "cases": 1, "controls": 1}]
    base = dict(se=0.1, pvalue=0.01, eaf=0.3, n=1000)
    frames = {  # different variant in each cohort -> nothing shared by >=2
        "a": pd.DataFrame([dict(chromosome="1", position=100, ea="G", oa="A", beta=0.2, **base)]),
        "b": pd.DataFrame([dict(chromosome="2", position=200, ea="C", oa="A", beta=0.2, **base)]),
    }
    summary = meta_analyze(cohorts, lambda co: [frames[co["file_id"]]], str(tmp_path))
    meta = pd.read_csv(tmp_path / "meta.tsv.gz", sep="\t")
    assert len(meta) == 0
    assert summary["n_meta_variants"] == 0
    assert not (tmp_path / "manhattan.png").exists()
    assert json.loads((tmp_path / "summary.json").read_text())["n_meta_variants"] == 0

def test_meta_analyze_skips_unreadable_cohort(tmp_path):
    cohorts = [{"file_id": "a", "dataset": "A", "cases": 1, "controls": 1},
               {"file_id": "bad", "dataset": "BAD", "cases": 1, "controls": 1},
               {"file_id": "c", "dataset": "C", "cases": 1, "controls": 1}]
    base = dict(se=0.1, pvalue=0.01, eaf=0.3, n=1000)
    good = pd.DataFrame([dict(chromosome="1", position=100, ea="G", oa="A", beta=0.2, **base)])
    def chunks_fn(co):
        if co["file_id"] == "bad":
            raise ValueError("boom")
        return [good.copy()]
    summary = meta_analyze(cohorts, chunks_fn, str(tmp_path))
    assert summary["n_cohorts"] == 3 and summary["n_cohorts_used"] == 2
    assert any(c.get("skipped") for c in summary["per_cohort"])
    meta = pd.read_csv(tmp_path / "meta.tsv.gz", sep="\t")
    assert len(meta) == 1 and meta.iloc[0]["n_cohorts"] == 2

def test_meta_analyze_aborts_on_infra_error(tmp_path):
    import pytest
    cohorts = [{"file_id": "a", "dataset": "A", "cases": 1, "controls": 1},
               {"file_id": "b", "dataset": "B", "cases": 1, "controls": 1}]
    def chunks_fn(co):
        raise RuntimeError("s3 down")   # infra error, not ValueError
    with pytest.raises(RuntimeError):
        meta_analyze(cohorts, chunks_fn, str(tmp_path))

def test_main_records_batch_job_id_and_content_types(tmp_path, monkeypatch):
    """main() should (a) stamp the row with its AWS_BATCH_JOB_ID on the RUNNING
    update, and (b) upload every artifact with an explicit ContentType. Fully
    mocked: no DB, no S3, no network."""
    from click.testing import CliRunner
    import sgc_ma.run_ma as rm
    import dataregistry.api.query as q
    import dataregistry.api.db as db

    updates = []
    monkeypatch.setattr(q, "insert_sgc_ma_pending", lambda *a, **k: "id")
    monkeypatch.setattr(q, "update_sgc_ma_result", lambda *a, **k: updates.append(k))

    class _DummyDB:
        def get_engine(self):
            return object()
    monkeypatch.setattr(db, "DataRegistryReadWriteDB", _DummyDB)
    monkeypatch.setattr(rm.sel, "select_cohorts", lambda *a, **k: [])

    def fake_meta(cohorts, chunks_fn, outdir, label=""):
        import os
        os.makedirs(outdir, exist_ok=True)
        for n in ["meta.tsv.gz", "manhattan.png", "qq.png", "summary.json", "summary.tsv", "top_loci.tsv"]:
            with open(os.path.join(outdir, n), "wb") as fh:
                fh.write(b"x")
        return {"meta_lambda_gc": 1.0, "n_meta_variants": 1, "n_genome_wide_sig": 0,
                "n_cohorts": 0, "n_cohorts_used": 0}
    monkeypatch.setattr(rm, "meta_analyze", fake_meta)

    uploads = []
    class FakeS3:
        def upload_file(self, Filename, Bucket, Key, ExtraArgs=None):
            uploads.append((Key, ExtraArgs))
        def download_file(self, *a, **k):
            pass
    monkeypatch.setattr(rm.boto3, "client", lambda *a, **k: FakeS3())
    monkeypatch.setenv("AWS_BATCH_JOB_ID", "job-xyz")

    res = CliRunner().invoke(rm.main, ["--phenotype", "PH", "--ancestry", "EUR",
                                       "--bucket", "b", "--local-out", str(tmp_path / "out")])
    assert res.exit_code == 0, res.output

    # (a) the RUNNING update carried this container's Batch job id
    running = [u for u in updates if u.get("status") == "RUNNING"]
    assert running and running[0].get("batch_job_id") == "job-xyz"

    # (b) every artifact uploaded with the right ContentType
    ct = {k: (ea or {}).get("ContentType") for k, ea in uploads}
    assert ct["sgc/ma/PH/EUR/manhattan.png"] == "image/png"
    assert ct["sgc/ma/PH/EUR/qq.png"] == "image/png"
    assert ct["sgc/ma/PH/EUR/summary.json"] == "application/json"
    assert ct["sgc/ma/PH/EUR/summary.tsv"] == "text/tab-separated-values"
    assert ct["sgc/ma/PH/EUR/top_loci.tsv"] == "text/tab-separated-values"
    assert ct["sgc/ma/PH/EUR/meta.tsv.gz"] == "application/gzip"


def test_main_local_run_leaves_batch_job_id_none(tmp_path, monkeypatch):
    """With no AWS_BATCH_JOB_ID (local run), the RUNNING update passes None so
    COALESCE leaves the column untouched."""
    from click.testing import CliRunner
    import sgc_ma.run_ma as rm
    import dataregistry.api.query as q
    import dataregistry.api.db as db

    updates = []
    monkeypatch.setattr(q, "insert_sgc_ma_pending", lambda *a, **k: "id")
    monkeypatch.setattr(q, "update_sgc_ma_result", lambda *a, **k: updates.append(k))

    class _DummyDB:
        def get_engine(self):
            return object()
    monkeypatch.setattr(db, "DataRegistryReadWriteDB", _DummyDB)
    monkeypatch.setattr(rm.sel, "select_cohorts", lambda *a, **k: [])
    monkeypatch.setattr(rm, "meta_analyze",
                        lambda *a, **k: {"meta_lambda_gc": None, "n_meta_variants": 0,
                                         "n_genome_wide_sig": 0, "n_cohorts": 0, "n_cohorts_used": 0})

    class FakeS3:
        def upload_file(self, *a, **k):
            pass
        def download_file(self, *a, **k):
            pass
    monkeypatch.setattr(rm.boto3, "client", lambda *a, **k: FakeS3())
    monkeypatch.delenv("AWS_BATCH_JOB_ID", raising=False)

    res = CliRunner().invoke(rm.main, ["--phenotype", "PH", "--ancestry", "EUR",
                                       "--bucket", "b", "--local-out", str(tmp_path / "out")])
    assert res.exit_code == 0, res.output
    running = [u for u in updates if u.get("status") == "RUNNING"]
    assert running and running[0].get("batch_job_id") is None


def test_read_meta_for_plot_keeps_chromosome_string(tmp_path):
    import gzip
    from sgc_ma.run_ma import _read_meta_for_plot
    p = tmp_path / "meta.tsv.gz"
    with gzip.open(p, "wt") as fh:
        fh.write("chromosome\tposition\tpvalue\n")
        fh.write("1\t100\t0.01\n2\t200\t0.02\n")   # numeric chromosomes only (the failing case)
    df = _read_meta_for_plot(str(p))
    assert df["chromosome"].map(type).eq(str).all()          # stayed string, not int64
    valid = [str(i) for i in range(1, 23)] + ["X", "Y"]
    assert df["chromosome"].isin(valid).all()                # render_manhattan would keep these
