import json
from unittest.mock import MagicMock

from click.testing import CliRunner

from sgc_munge import submit_munge_batch
from sgc_munge.submit_munge_batch import _LIST_SQL, _list_files


class _Row:
    def __init__(self, d):
        self._mapping = d


class _Conn:
    def __init__(self, rows):
        self.rows = rows
        self.params = None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.params = params
        return [_Row(r) for r in self.rows]


class _Engine:
    def __init__(self, rows):
        self.conn = _Conn(rows)

    def connect(self):
        return self.conn


def _rows():
    return [
        {"file_id": "a" * 32, "genome_build": "GRCh37"},
        {"file_id": "b" * 32, "genome_build": "GRCh38"},
        {"file_id": "c" * 32, "genome_build": "liftover to GRCh38"},
    ]


def test_list_sql_requires_qc_success_and_joins_munge():
    assert "JOIN sgc_gwas_plot_results p ON p.file_id = f.id AND p.status = 'SUCCEEDED'" in _LIST_SQL
    assert "LEFT JOIN sgc_gwas_munge_results mu ON mu.file_id = f.id" in _LIST_SQL
    assert "mu.status IN ('PENDING','FAILED')" in _LIST_SQL and ":phenotype" in _LIST_SQL


def test_list_files_filters_build_ids_and_limit():
    kw = dict(force=False, limit=None, phenotype=None, file_ids=())
    eng = _Engine(_rows())
    out = _list_files(eng, **kw)
    assert [r["file_id"] for r in out] == ["b" * 32, "c" * 32]
    assert eng.conn.params == {"force": 0, "phenotype": None}

    # explicit id of a GRCh37 file: build filter still applies
    assert _list_files(_Engine(_rows()), **{**kw, "file_ids": ("a" * 32,)}) == []

    # explicit id forces re-submission and keeps only that id
    eng = _Engine(_rows())
    out = _list_files(eng, **{**kw, "file_ids": ("c" * 32,), "phenotype": "PSOR"})
    assert [r["file_id"] for r in out] == ["c" * 32]
    assert eng.conn.params == {"force": 1, "phenotype": "PSOR"}

    assert len(_list_files(_Engine(_rows()), **{**kw, "limit": 1})) == 1


def test_submit_file_order_and_params(monkeypatch):
    calls = []
    mock_query = MagicMock()
    monkeypatch.setattr(submit_munge_batch, "query", mock_query)
    mock_query.insert_sgc_munge_result_pending.side_effect = lambda *a: calls.append("insert")
    batch = MagicMock()
    batch.submit_job.side_effect = lambda **kw: (calls.append("submit"), {"jobId": "j1"})[1]
    row = {"file_id": "f" * 32, "s3_path": "sgc/gwas/x.gz", "column_mapping": '{"col_chromosome":"CHR"}',
           "cases": 10, "controls": None, "phenotype": "PSOR"}
    jid = submit_munge_batch.submit_file(engine="E", batch=batch, row=row, bucket="b",
                                         db_name="dataregistry_qa", ref_prefix="s3://r/p/")
    assert jid == "j1" and calls == ["insert", "submit"]
    kw = batch.submit_job.call_args.kwargs
    p = kw["parameters"]
    assert kw["jobName"] == "sgc-munge-" + "f" * 16
    assert p["output-prefix"] == "sgc/munge/" + "f" * 32 and p["cases"] == "10" and p["controls"] == "NA"
    assert p["ref-prefix"] == "s3://r/p/" and p["bucket"] == "b" and p["file-id"] == "f" * 32
    assert json.loads(p["column-mapping"]) == {"col_chromosome": "CHR"}
    assert kw["containerOverrides"]["environment"] == [
        {"name": "DATA_REGISTRY_DB_NAME", "value": "dataregistry_qa"}]
    mock_query.update_sgc_munge_result.assert_called_once_with("E", "f" * 32, status="PENDING", batch_job_id="j1")


def test_main_dry_run_submits_nothing(monkeypatch):
    monkeypatch.setenv("DATA_REGISTRY_DB_NAME", "orig")  # main() overwrites it; monkeypatch restores
    rows = [{"file_id": "a" * 32, "s3_path": "p1", "phenotype": "PSOR"},
            {"file_id": "b" * 32, "s3_path": "p2", "phenotype": None}]
    monkeypatch.setattr(submit_munge_batch, "_list_files", lambda *a, **k: rows)
    mock_query = MagicMock()
    monkeypatch.setattr(submit_munge_batch, "query", mock_query)
    batch = MagicMock()
    monkeypatch.setattr(submit_munge_batch.boto3, "client", lambda *a, **k: batch)
    monkeypatch.setattr("dataregistry.api.db.DataRegistryReadWriteDB",
                        lambda: MagicMock())
    res = CliRunner().invoke(submit_munge_batch.main,
                             ["--bucket", "b", "--db-name", "dataregistry_qa", "--dry-run"])
    assert res.exit_code == 0, res.output
    assert "Found 2" in res.output and "Submitted 0 job(s)" in res.output
    batch.submit_job.assert_not_called()
    mock_query.insert_sgc_munge_result_pending.assert_not_called()


def _explain_rows():
    return [
        {"file_id": "a" * 32, "phenotype": "PSOR", "genome_build": "GRCh37", "plot_status": "SUCCEEDED"},
        {"file_id": "d" * 32, "phenotype": "PSOR", "genome_build": "GRCh38", "plot_status": "FAILED"},
        {"file_id": "e" * 32, "phenotype": "PSOR", "genome_build": "GRCh38", "plot_status": None},
        {"file_id": "f" * 32, "phenotype": "ECZ", "genome_build": "GRCh38", "plot_status": "SUCCEEDED"},
        {"file_id": "9" * 32, "phenotype": "PSOR", "genome_build": "GRCh38", "plot_status": "SUCCEEDED"},
    ]


def test_dropped_file_ids_gives_a_reason_per_missing_id():
    from sgc_munge.submit_munge_batch import _dropped_file_ids
    eng = _Engine(_explain_rows())
    listed = [{"file_id": "c" * 32}]
    wanted = ("c" * 32, "a" * 32, "d" * 32, "e" * 32, "f" * 32, "9" * 32, "0" * 32, "a" * 32)
    reasons = _dropped_file_ids(eng, wanted, listed, phenotype="PSOR")
    assert reasons == {
        "a" * 32: "genome build 'GRCh37' is not GRCh38",
        "d" * 32: "QC plots not SUCCEEDED (status=FAILED)",
        "e" * 32: "QC plots not SUCCEEDED (status=none)",
        "f" * 32: "phenotype 'ECZ' does not match --phenotype 'PSOR'",
        "9" * 32: "cut by --limit",
        "0" * 32: "unknown file id",
    }
    assert list(reasons) == ["a" * 32, "d" * 32, "e" * 32, "f" * 32, "9" * 32, "0" * 32]
    assert eng.conn.params == {"file_ids": ["a" * 32, "d" * 32, "e" * 32, "f" * 32, "9" * 32,
                                            "0" * 32]}


def test_dropped_file_ids_skips_query_when_nothing_dropped():
    from sgc_munge.submit_munge_batch import _dropped_file_ids
    eng = _Engine(_explain_rows())
    assert _dropped_file_ids(eng, ("c" * 32,), [{"file_id": "c" * 32}], phenotype=None) == {}
    assert eng.conn.params is None


def test_main_reports_dropped_file_ids_before_found(monkeypatch):
    monkeypatch.setenv("DATA_REGISTRY_DB_NAME", "orig")
    rows = [{"file_id": "c" * 32, "s3_path": "p3", "phenotype": "PSOR"}]
    monkeypatch.setattr(submit_munge_batch, "_list_files", lambda *a, **k: rows)
    monkeypatch.setattr(submit_munge_batch.boto3, "client", lambda *a, **k: MagicMock())
    db = MagicMock()
    db.get_engine.return_value = _Engine(_explain_rows())
    monkeypatch.setattr("dataregistry.api.db.DataRegistryReadWriteDB", lambda: db)
    res = CliRunner().invoke(submit_munge_batch.main, [
        "--bucket", "b", "--db-name", "dataregistry_qa", "--dry-run",
        "--file-id", "c" * 32, "--file-id", "a" * 32, "--file-id", "0" * 32])
    assert res.exit_code == 0, res.output
    lines = res.output.splitlines()
    assert lines[:3] == [
        f"  dropped --file-id {'a' * 32}: genome build 'GRCh37' is not GRCh38",
        f"  dropped --file-id {'0' * 32}: unknown file id",
        "Found 1 file(s) to munge (bucket=b, db=dataregistry_qa, force=False, limit=None)"]
