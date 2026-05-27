import gzip
import io
import uuid
from unittest import mock

import boto3
import numpy as np
import pytest
from moto import mock_aws


@pytest.fixture
def s3_setup():
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="dig-data-registry")
        rows = ["CHROM\tGENPOS\tP"]
        rng = np.random.default_rng(0)
        for _ in range(2000):
            chrom = int(rng.integers(1, 23))
            pos = int(rng.integers(1, 250_000_000))
            p = float(rng.uniform(1e-9, 1.0))
            rows.append(f"{chrom}\t{pos}\t{p}")
        body = "\n".join(rows).encode()
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as g:
            g.write(body)
        s3.put_object(Bucket="dig-data-registry",
                      Key="sgc/uploads/test/sample.tsv.gz",
                      Body=buf.getvalue())
        yield s3


def test_worker_writes_outputs_and_updates_db(s3_setup):
    from sgc_qc_plots import qc_plots

    file_id = uuid.uuid4().hex
    col_map = {"col_chromosome": "CHROM", "col_position": "GENPOS", "col_pvalue": "P"}

    with mock.patch.object(qc_plots, "_get_engine") as get_eng, \
         mock.patch.object(qc_plots, "_update_db") as upd:
        get_eng.return_value = "fake-engine"
        qc_plots.run_one(
            s3_path="sgc/uploads/test/sample.tsv.gz",
            column_mapping=col_map,
            bucket="dig-data-registry",
            file_id=file_id,
            output_prefix=f"sgc/qc/plots/{file_id}",
        )

        # _update_db is called at least twice: status=RUNNING then status=SUCCEEDED
        assert upd.call_count >= 2
        final = upd.call_args
        kw = final.kwargs
        assert kw["status"] == "SUCCEEDED"
        assert kw["n_variants"] > 0
        assert kw["lambda_gc"] is not None
        assert kw["manhattan_s3_key"].endswith("manhattan.png")
        assert kw["qq_s3_key"].endswith("qq.png")
        assert kw["file_id"] == file_id

    keys = [o["Key"] for o in s3_setup.list_objects_v2(
        Bucket="dig-data-registry", Prefix=f"sgc/qc/plots/{file_id}")["Contents"]]
    assert any(k.endswith("manhattan.png") for k in keys)
    assert any(k.endswith("qq.png") for k in keys)
    assert any(k.endswith("qc.json") for k in keys)


def test_worker_marks_failed_on_missing_columns(s3_setup):
    from sgc_qc_plots import qc_plots
    file_id = uuid.uuid4().hex

    with mock.patch.object(qc_plots, "_get_engine") as get_eng, \
         mock.patch.object(qc_plots, "_update_db") as upd:
        get_eng.return_value = "fake-engine"
        with pytest.raises(SystemExit):
            qc_plots.run_one(
                s3_path="sgc/uploads/test/sample.tsv.gz",
                column_mapping={"col_chromosome": "CHROM"},  # missing pos and pvalue
                bucket="dig-data-registry",
                file_id=file_id,
                output_prefix=f"sgc/qc/plots/{file_id}",
            )
        # final update was FAILED
        final = upd.call_args
        assert final.kwargs["status"] == "FAILED"
        assert "col_position" in final.kwargs["error_message"]
        assert "col_pvalue" in final.kwargs["error_message"]
