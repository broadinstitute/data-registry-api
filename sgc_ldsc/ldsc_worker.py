"""AWS Batch entry point for univariate LDSC. Mirrors sgc_qc_plots/qc_plots.py.
Writes ONLY ldsc_* columns on the shared sgc_gwas_plot_results row.
"""
import csv
import gzip
import glob
import json
import os
import shutil
import tempfile
import traceback
import zipfile
from typing import Optional

import boto3
import click

from dataregistry.api import query
from sgc_ldsc import reference, munge
from sgc_ldsc.compute import run_univariate


def _get_engine():
    from dataregistry.api.db import DataRegistryReadWriteDB
    return DataRegistryReadWriteDB().get_engine()


def _download(s3, bucket: str, key: str, dst: str) -> None:
    if not os.path.exists(dst):
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        s3.download_file(bucket, key, dst)


def _flatten_weights(weights_dir: str) -> None:
    """weights_{anc}.zip may extract into a nested folder; move *.l2.ldscore.gz up
    to weights_dir so reference.load_input_weights finds them. No-op if already flat."""
    if glob.glob(os.path.join(weights_dir, "weights.1.l2.ldscore.gz")):
        return
    for f in glob.glob(os.path.join(weights_dir, "**", "*.l2.ldscore.gz"), recursive=True):
        shutil.move(f, os.path.join(weights_dir, os.path.basename(f)))


def _ensure_reference(ref_bucket: str, ancestry: str, genome_build: str, cache_dir: str) -> None:
    """Fetch inputs/weights/snpmap for this ancestry+build from s3://{ref_bucket}/bin/
    into cache_dir, in the on-disk layout reference.py expects. Idempotent."""
    s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1"))

    inputs_dst = os.path.join(cache_dir, "inputs", f"sldsc_inputs.{ancestry}.zip")
    _download(s3, ref_bucket, f"bin/sldsc_inputs/sldsc_inputs.{ancestry}.zip", inputs_dst)

    weights_dir = os.path.join(cache_dir, "weights", ancestry)
    if not os.path.exists(os.path.join(weights_dir, "weights.1.l2.ldscore.gz")):
        wz = os.path.join(cache_dir, f"weights_{ancestry}.zip")
        _download(s3, ref_bucket, f"bin/weights/weights_{ancestry}.zip", wz)
        os.makedirs(weights_dir, exist_ok=True)
        with zipfile.ZipFile(wz) as z:
            z.extractall(weights_dir)
        _flatten_weights(weights_dir)

    for build_type in ("standard", "flipped"):
        name = f"sumstats.{build_type}.{genome_build}.{ancestry}.snpmap"
        _download(s3, ref_bucket, f"bin/snpmap/{name}", os.path.join(cache_dir, "snpmap", name))


def _read_rows(local_path: str, sep: str = "\t"):
    """Yield each data row as a dict keyed by header (handles .gz transparently)."""
    opener = gzip.open if local_path.endswith(".gz") else open
    with opener(local_path, "rt") as f:
        reader = csv.DictReader(f, delimiter=sep)
        for row in reader:
            yield row


def _compute_for_file(local_path, column_mapping, ancestry, genome_build, cache_dir) -> dict:
    cm = munge.build_col_map(column_mapping)
    snpmap = reference.load_snpmap(cache_dir, ancestry, genome_build, "standard")
    snpmap_flipped = reference.load_snpmap(cache_dir, ancestry, genome_build, "flipped")
    records = munge.munge_records(_read_rows(local_path), cm, snpmap, snpmap_flipped)
    data = munge.n90_filter(records)
    if not data:
        raise ValueError("no variants mapped to the reference panel")
    baseline_ld = reference.load_baseline_ld_col0(cache_dir, ancestry)
    input_weights = reference.load_input_weights(cache_dir, ancestry)
    m_snps = reference.load_baseline_m(cache_dir, ancestry)
    ld_rs = list(reference.ld_rs_order(cache_dir, ancestry))
    return run_univariate(data=data, ld_rs=ld_rs, baseline_ld=baseline_ld,
                          input_weights=input_weights, m_snps=m_snps)


def run_one(*, s3_path, column_mapping, bucket, file_id, ancestry, genome_build,
            ref_bucket, batch_job_id: Optional[str] = None) -> None:
    engine = _get_engine()
    query.update_sgc_ldsc_result(engine, file_id, ldsc_status="RUNNING",
                                 ldsc_batch_job_id=batch_job_id)
    s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1"))
    try:
        with tempfile.TemporaryDirectory() as tmp:
            local = os.path.join(tmp, os.path.basename(s3_path))
            s3.download_file(bucket, s3_path, local)
            cache_dir = os.getenv("LDSC_REF_CACHE", "/tmp/ldsc_ref")
            _ensure_reference(ref_bucket, ancestry, genome_build, cache_dir)
            res = _compute_for_file(local, column_mapping, ancestry, genome_build, cache_dir)
        query.update_sgc_ldsc_result(
            engine, file_id, ldsc_status="SUCCEEDED",
            ldsc_intercept=res["intercept"], ldsc_h2=res["h2"], ldsc_ratio=res["ratio"],
            ldsc_effective_n=res["effective_n"], ldsc_n_snps=res["n_snps"])
    except Exception as e:
        # Best-effort failure write: if this DB write itself raises (engine gone,
        # network), it must not mask the original error — always re-raise that.
        try:
            query.update_sgc_ldsc_result(
                engine, file_id, ldsc_status="FAILED",
                ldsc_error=f"{type(e).__name__}: {e}\n{traceback.format_exc()[:1500]}")
        except Exception:
            pass
        raise


@click.command()
@click.option("--s3-path", required=True)
@click.option("--column-mapping", required=True, help="JSON string")
@click.option("--bucket", required=True)
@click.option("--file-id", required=True)
@click.option("--ancestry", required=True)
@click.option("--genome-build", required=True)
@click.option("--ref-bucket", required=True)
@click.option("--batch-job-id", required=False)
def main(s3_path, column_mapping, bucket, file_id, ancestry, genome_build, ref_bucket, batch_job_id):
    run_one(s3_path=s3_path, column_mapping=json.loads(column_mapping), bucket=bucket,
            file_id=file_id, ancestry=ancestry, genome_build=genome_build,
            ref_bucket=ref_bucket, batch_job_id=batch_job_id)


if __name__ == "__main__":
    main()
