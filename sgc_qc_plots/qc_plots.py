"""Batch worker entry point. Downloads a GWAS file, computes QC stats, renders
Manhattan + QQ plots, uploads artifacts, updates the DB. Mirrors the CLI
signature of sgc_batch/validate_gwas.py.
"""
import json
import os
import tempfile
import traceback
from typing import Optional

import boto3
import click
import pandas as pd
from sqlalchemy import text

from sgc_qc_plots.computations import (
    lambda_gc, normalize_chromosome, filter_valid_pvalues, count_significant,
)
from sgc_qc_plots.plots import render_manhattan, render_qq


def _get_engine():
    """Build SQLAlchemy engine using the same Secrets Manager secret the API uses.

    Imported lazily so unit tests that mock this function don't need the API
    package on the import path.
    """
    from dataregistry.api.db import DataRegistryReadWriteDB
    return DataRegistryReadWriteDB().get_engine()


def _update_db(engine, *, file_id: str, status: str, **fields) -> None:
    """Update the plot-result row for file_id. Only non-None fields are written."""
    set_clauses = ["status = :status"]
    params = {"status": status, "file_id": file_id}
    for k, v in fields.items():
        if v is None:
            continue
        set_clauses.append(f"{k} = :{k}")
        params[k] = v
    sql = f"UPDATE sgc_gwas_plot_results SET {', '.join(set_clauses)} WHERE file_id = :file_id"
    with engine.connect() as conn:
        conn.execute(text(sql), params)
        conn.commit()


def _download_to_tmpdir(s3_client, bucket: str, key: str, tmpdir: str) -> str:
    local = os.path.join(tmpdir, os.path.basename(key))
    s3_client.download_file(bucket, key, local)
    return local


def _read_three_cols(local_path: str, chrom_hdr: str, pos_hdr: str, p_hdr: str) -> pd.DataFrame:
    """Read only the three needed columns. pandas auto-detects .gz."""
    return pd.read_csv(
        local_path, sep="\t", usecols=[chrom_hdr, pos_hdr, p_hdr],
        dtype={chrom_hdr: str},
        low_memory=False,
    ).rename(columns={chrom_hdr: "chromosome", pos_hdr: "position", p_hdr: "pvalue"})


def _upload(s3_client, bucket: str, key: str, local_path: str) -> None:
    s3_client.upload_file(local_path, bucket, key)


def run_one(*, s3_path: str, column_mapping: dict, bucket: str,
            file_id: str, output_prefix: str,
            batch_job_id: Optional[str] = None) -> None:
    engine = _get_engine()
    chrom_hdr = column_mapping.get("col_chromosome")
    pos_hdr = column_mapping.get("col_position")
    p_hdr = column_mapping.get("col_pvalue")
    missing = [k for k, v in [("col_chromosome", chrom_hdr),
                              ("col_position", pos_hdr),
                              ("col_pvalue", p_hdr)] if not v]
    if missing:
        _update_db(engine, file_id=file_id, status="FAILED",
                   error_message=f"column_mapping missing required keys: {missing}")
        raise SystemExit(2)
    assert chrom_hdr and pos_hdr and p_hdr  # narrowed by the guard above

    _update_db(engine, file_id=file_id, status="RUNNING", batch_job_id=batch_job_id)
    s3 = boto3.client("s3", region_name="us-east-1")

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            local = _download_to_tmpdir(s3, bucket, s3_path, tmpdir)
            df = _read_three_cols(local, chrom_hdr, pos_hdr, p_hdr)
            df["chromosome"] = normalize_chromosome(df["chromosome"])
            df = df.dropna(subset=["chromosome"])
            df = filter_valid_pvalues(df, "pvalue")
            n_variants = len(df)
            if n_variants == 0:
                raise ValueError("no rows with valid chromosome and 0 < p <= 1")

            lam = lambda_gc(df["pvalue"])
            n_sig_5e8 = count_significant(df["pvalue"], 5e-8)
            n_sig_1e5 = count_significant(df["pvalue"], 1e-5)

            man_local = os.path.join(tmpdir, "manhattan.png")
            qq_local = os.path.join(tmpdir, "qq.png")
            json_local = os.path.join(tmpdir, "qc.json")
            render_manhattan(df, man_local, title=f"file_id={file_id[:8]}")
            render_qq(df["pvalue"], qq_local, title=f"file_id={file_id[:8]}", lambda_gc=lam)
            with open(json_local, "w") as fh:
                json.dump({
                    "file_id": file_id, "lambda_gc": lam,
                    "n_variants": n_variants,
                    "n_sig_5e8": n_sig_5e8, "n_sig_1e5": n_sig_1e5,
                    "column_mapping": column_mapping,
                }, fh, indent=2)

            man_key = f"{output_prefix}/manhattan.png"
            qq_key = f"{output_prefix}/qq.png"
            json_key = f"{output_prefix}/qc.json"
            _upload(s3, bucket, man_key, man_local)
            _upload(s3, bucket, qq_key, qq_local)
            _upload(s3, bucket, json_key, json_local)

        _update_db(engine, file_id=file_id, status="SUCCEEDED",
                   lambda_gc=lam, n_variants=n_variants,
                   n_sig_5e8=n_sig_5e8, n_sig_1e5=n_sig_1e5,
                   manhattan_s3_key=man_key, qq_s3_key=qq_key)
    except SystemExit:
        raise
    except Exception as e:
        _update_db(engine, file_id=file_id, status="FAILED", batch_job_id=batch_job_id,
                   error_message=f"{type(e).__name__}: {e}\n{traceback.format_exc()[:1500]}")
        raise


@click.command()
@click.option("--s3-path", required=True)
@click.option("--column-mapping", required=True, help="JSON string")
@click.option("--bucket", required=True)
@click.option("--file-id", required=True)
@click.option("--output-prefix", required=True)
@click.option("--batch-job-id", required=False)
def main(s3_path, column_mapping, bucket, file_id, output_prefix, batch_job_id):
    col_map = json.loads(column_mapping)
    run_one(s3_path=s3_path, column_mapping=col_map, bucket=bucket,
            file_id=file_id, output_prefix=output_prefix, batch_job_id=batch_job_id)


if __name__ == "__main__":
    main()
