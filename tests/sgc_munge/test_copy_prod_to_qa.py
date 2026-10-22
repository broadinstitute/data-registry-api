import importlib.util
from pathlib import Path

_PATH = Path(__file__).resolve().parents[2] / "scripts" / "sgc" / "copy_prod_phenotype_to_qa.py"


def _load():
    spec = importlib.util.spec_from_file_location("copy_prod_phenotype_to_qa", _PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_plan_copy_same_key_both_sides():
    m = _load()
    rows = {"files": [{"s3_path": "sgc/gwas/a/x.gz"}, {"s3_path": "sgc/gwas/b/y.gz"}]}
    assert m.plan_copy(rows, "dig-data-registry", "dig-data-registry-qa") == [
        ("dig-data-registry", "sgc/gwas/a/x.gz", "dig-data-registry-qa"),
        ("dig-data-registry", "sgc/gwas/b/y.gz", "dig-data-registry-qa"),
    ]


def test_select_rows_sql_filters_phenotype_and_cohort_names():
    m = _load()
    assert "f.phenotype = :phenotype" in m._FILES_SQL
    assert "sc.name IN :cohorts" in m._FILES_SQL


class _Conn:
    def __init__(self, log):
        self.log = log

    def execute(self, stmt, params=None):
        self.log.append((str(stmt), params))
        return type("R", (), {"rowcount": 1})()


class _Engine:
    def __init__(self):
        self.log = []

    def begin(self):
        log = self.log

        class _Ctx:
            def __enter__(self_):
                return _Conn(log)

            def __exit__(self_, *a):
                return False

        return _Ctx()


def test_insert_rows_order_and_ignore():
    m = _load()
    eng = _Engine()
    rows = {
        "cohorts": [{"id": "c1", "name": "X"}],
        "gwas_cohorts": [{"cohort_id": "c1", "metadata": "{}"}],
        "files": [{"id": "f1", "cohort_id": "c1"}, {"id": "f2", "cohort_id": "c1"}],
    }
    counts = m.insert_rows(eng, rows)
    stmts = [s for s, _ in eng.log]
    assert len(stmts) == 4
    assert all(s.startswith("INSERT IGNORE INTO") for s in stmts)
    assert "sgc_cohorts" in stmts[0]
    assert "sgc_gwas_cohorts" in stmts[1]
    assert "sgc_gwas_files" in stmts[2] and "sgc_gwas_files" in stmts[3]
    assert eng.log[2][1] == {"id": "f1", "cohort_id": "c1"}
    assert counts == {"sgc_cohorts": 1, "sgc_gwas_cohorts": 1, "sgc_gwas_files": 2}


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return iter(self._rows)


class _ReadConn:
    def __init__(self, files, log):
        self.files, self.log = files, log

    def execute(self, stmt, params=None):
        sql = str(stmt)
        self.log.append((sql, params))
        if "FROM sgc_gwas_files" in sql:
            return _Result(self.files)
        if "FROM sgc_cohorts" in sql:
            return _Result([{"id": i, "name": f"n{i}"} for i in params["ids"]])
        if "FROM sgc_gwas_cohorts" in sql:
            return _Result([{"cohort_id": i, "metadata": "{}"} for i in params["ids"]])
        raise AssertionError(sql)


class _ReadEngine:
    def __init__(self, files):
        self.files, self.log = files, []

    def connect(self):
        eng = self

        class _Ctx:
            def __enter__(self_):
                return _ReadConn(eng.files, eng.log)

            def __exit__(self_, *a):
                return False

        return _Ctx()


def test_select_rows_keeps_only_grch38_effective():
    m = _load()
    builds = ["GRCh37", "liftover to GRCh38", None, "hg38"]
    cohort_ids = ["c1", "c1", "c2", "c3"]
    files = [{"id": f"f{i+1}", "cohort_id": cohort_ids[i], "genome_build_raw": builds[i],
              "cohort_name": "X"} for i in range(4)]
    eng = _ReadEngine(files)
    out = m.select_rows(eng, "PSOR", ())
    assert [f["id"] for f in out["files"]] == ["f2", "f4"]
    for f in out["files"]:
        assert "genome_build_raw" not in f and "cohort_name" not in f
    cohort_calls = [p for s, p in eng.log if "FROM sgc_cohorts" in s or "FROM sgc_gwas_cohorts" in s]
    assert len(cohort_calls) == 2
    assert all(p == {"ids": ["c1", "c3"]} for p in cohort_calls)
    assert [c["id"] for c in out["cohorts"]] == ["c1", "c3"]
    assert [c["cohort_id"] for c in out["gwas_cohorts"]] == ["c1", "c3"]


def test_main_rejects_same_connection_urls(monkeypatch):
    from click.testing import CliRunner
    m = _load()
    called = []
    monkeypatch.setattr(m.sqlalchemy, "create_engine", lambda *a, **k: called.append(a))
    res = CliRunner().invoke(m.main, ["--phenotype", "PSOR", "--prod-db-connection", "mysql://x/y",
                                      "--qa-db-connection", "mysql://x/y", "--dry-run"])
    assert res.exit_code == 2
    assert "prod and QA connection URLs must differ" in res.output
    assert called == []
