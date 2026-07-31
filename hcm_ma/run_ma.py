"""Bottom-line meta-analysis worker for the HCM tenant. Reuses the shared
sgc_ma compute core; translates HCM's canonical column_mapping to the reader's
col_* keys, then meta-analyzes the selected GRCh38 cohorts."""
import json
import os
import tempfile

import boto3
import click

from dataregistry.api import hcm_query as query_mod
from dataregistry.api.db import DataRegistryReadWriteDB
from hcm_ma import select as sel
from sgc_ma.reader import read_cohort_chunks
from sgc_ma.run_ma import meta_analyze, _CONTENT_TYPES


@click.command()
@click.option("--run-id", required=True)
@click.option("--bucket", required=True)
@click.option("--local-out", default="ma_out", help="local working dir")
def main(run_id, bucket, local_out):
    engine = DataRegistryReadWriteDB().get_engine()
    run = query_mod.get_hcm_ma_run(engine, run_id)
    if not run:
        raise SystemExit(f"no HCM MA run {run_id}")
    file_ids = run.get("dataset_file_ids") or []
    maf_min = 0.005 if run.get("maf_min") is None else run["maf_min"]
    info_min = 0.3 if run.get("info_min") is None else run["info_min"]
    label = run.get("label") or "HCM meta-analysis"
    prefix = f"hcm/ma/{run_id}"

    query_mod.update_hcm_ma_result(engine, run_id, status="RUNNING",
                                   batch_job_id=os.environ.get("AWS_BATCH_JOB_ID"))
    try:
        cohorts = sel.select_files_by_ids(engine, file_ids)
        for co in cohorts:
            co["column_mapping"] = sel.hcm_colmap_to_ma(co["column_mapping"])
        click.echo(f"run {run_id}: {len(cohorts)} cohorts")
        s3 = boto3.client("s3", region_name="us-east-1")

        def chunks_fn(co):
            with tempfile.TemporaryDirectory() as td:
                local = os.path.join(td, os.path.basename(co["s3_path"]))
                s3.download_file(bucket, co["s3_path"], local)
                yield from read_cohort_chunks(local, co["column_mapping"],
                                              co.get("cases"), co.get("controls"))

        summary = meta_analyze(cohorts, chunks_fn, local_out, label=label,
                               maf_min=maf_min, info_min=info_min)
        for name in ["meta.tsv.gz", "manhattan.png", "qq.png", "summary.json",
                     "summary.tsv", "top_loci.tsv"]:
            p = os.path.join(local_out, name)
            if os.path.exists(p):
                ctype = _CONTENT_TYPES.get(os.path.splitext(name)[1])
                s3.upload_file(p, bucket, f"{prefix}/{name}",
                               ExtraArgs={"ContentType": ctype} if ctype else None)
        click.echo(json.dumps(summary, indent=2))
        query_mod.update_hcm_ma_result(
            engine, run_id, status="SUCCEEDED",
            meta_lambda_gc=summary["meta_lambda_gc"],
            n_meta_variants=summary["n_meta_variants"],
            n_genome_wide_sig=summary["n_genome_wide_sig"],
            n_cohorts=summary["n_cohorts"], n_cohorts_used=summary["n_cohorts_used"],
            total_cases=summary["total_cases"], total_controls=summary["total_controls"],
            manhattan_s3_key=f"{prefix}/manhattan.png", qq_s3_key=f"{prefix}/qq.png",
            meta_s3_key=f"{prefix}/meta.tsv.gz",
            summary_json_s3_key=f"{prefix}/summary.json",
            summary_tsv_s3_key=f"{prefix}/summary.tsv",
            top_loci_s3_key=f"{prefix}/top_loci.tsv")
    except Exception as e:
        query_mod.update_hcm_ma_result(engine, run_id, status="FAILED", error_message=str(e))
        raise


if __name__ == "__main__":
    main()
