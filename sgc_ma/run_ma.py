"""Bottom-line meta-analysis worker: stream cohorts, k-way merge, combine, plot.

No code path here loads a whole cohort into memory: cohorts are read in
chunks (chunks_fn), extracted+sorted to disk (extract_sorted), combined via a
bounded-memory k-way merge (merge_and_combine), and the merge output is
written to meta.tsv.gz incrementally. The only in-memory frames are single
chunks, individual merged rows, the small set of genome-wide-significant
rows (for clumping), and the final meta.tsv.gz read back for plotting
(bounded to the output size, not the inputs).
"""
import gzip
import json
import os
import tempfile

import boto3
import click
import pandas as pd

from sgc_ma import select as sel
from sgc_ma.reader import read_cohort_chunks
from sgc_ma.stream import extract_sorted, merge_and_combine
from sgc_qc_plots.plots import render_manhattan, render_qq
from sgc_qc_plots.computations import lambda_gc

META_COLUMNS = ["chromosome", "position", "ref", "alt", "beta", "se", "pvalue",
                "n", "n_cohorts", "dir_concordance", "i2"]


def _read_meta_for_plot(meta_path):
    """Read back the meta output for plotting. dtype={"chromosome": str} is
    required: without it pandas infers int64 when no X/Y are present, and
    render_manhattan's chromosome.isin(_CHROMS) (a string set) then drops every
    row, producing a silently-empty Manhattan plot."""
    return pd.read_csv(meta_path, sep="\t",
                       usecols=["chromosome", "position", "pvalue"],
                       dtype={"chromosome": str})


def _clump(sig, window=500_000):
    """Greedy distance-based clumping: keep the most significant variant, drop
    others within +/- window bp on the same chromosome, repeat."""
    sig = sig.sort_values("pvalue").reset_index(drop=True)
    leads, taken = [], []
    for _, r in sig.iterrows():
        if any(r["chromosome"] == c and abs(int(r["position"]) - p) <= window for c, p in taken):
            continue
        leads.append(r)
        taken.append((r["chromosome"], int(r["position"])))
    return pd.DataFrame(leads, columns=sig.columns)


def meta_analyze(cohorts: list[dict], chunks_fn, outdir: str, label: str = "meta-analysis") -> dict:
    os.makedirs(outdir, exist_ok=True)
    per_cohort = []
    n_cohorts_used = 0
    n_meta = 0
    sig_rows = []
    meta_path = os.path.join(outdir, "meta.tsv.gz")

    with tempfile.TemporaryDirectory() as tmp:
        sorted_paths = []
        for co in sorted(cohorts, key=lambda c: c["dataset"]):
            sorted_path = os.path.join(tmp, f"{co['file_id']}.tsv")
            try:
                stats = extract_sorted(chunks_fn(co), sorted_path)
            except ValueError as e:
                per_cohort.append({"dataset": co["dataset"], "file_id": co.get("file_id"),
                                   "skipped": True, "reason": str(e)})
                continue
            n_cohorts_used += 1
            sorted_paths.append(sorted_path)
            per_cohort.append({"dataset": co["dataset"], "file_id": co["file_id"],
                               "cases": co.get("cases"), "controls": co.get("controls"),
                               "n_variants_in": stats["n_in"], "n_variants_used": stats["n_kept"],
                               "sum_n": stats["sum_n"]})

        # Stream the k-way merge straight to meta.tsv.gz; never buffer the
        # full merge output. sorted_paths must stay alive (inside `tmp`)
        # for the duration of this loop.
        with gzip.open(meta_path, "wt") as fh:
            fh.write("\t".join(META_COLUMNS) + "\n")
            for row in merge_and_combine(sorted_paths):
                fh.write("\t".join(str(row[c]) for c in META_COLUMNS) + "\n")
                n_meta += 1
                if row["pvalue"] < 5e-8:
                    sig_rows.append(row)

    if n_meta:
        # Read back only the columns needed for plotting; bounded to the
        # (small) meta output, not the (large) per-cohort inputs.
        plot_df = _read_meta_for_plot(meta_path)
        lam = float(lambda_gc(plot_df["pvalue"]))
        render_manhattan(plot_df, os.path.join(outdir, "manhattan.png"), title=label)
        render_qq(plot_df["pvalue"], os.path.join(outdir, "qq.png"), lambda_gc=lam)
        top = _clump(pd.DataFrame(sig_rows, columns=META_COLUMNS))
        top.to_csv(os.path.join(outdir, "top_loci.tsv"), sep="\t", index=False)
    else:
        lam = None

    summary = {"n_cohorts": len(cohorts), "n_cohorts_used": n_cohorts_used,
               "n_meta_variants": n_meta,
               "meta_lambda_gc": lam, "n_genome_wide_sig": len(sig_rows),
               "per_cohort": per_cohort,
               "caveats": [
                   "SE not rescaled across cohorts (assumes comparable log-OR betas)",
                   "palindromic A/T and C/G SNPs dropped",
                   "GRCh37-only cohorts excluded (GRCh38-effective subset only)",
                   "meta.tsv.gz rows are in lexicographic key order, not genomic order (re-sort if needed)",
                   "indels whose allele strings differ across cohorts are not merged (kept as singletons, then dropped as <2 cohorts)",
               ]}
    with open(os.path.join(outdir, "summary.json"), "w") as fh:
        json.dump(summary, fh, indent=2)
    pd.DataFrame(per_cohort).to_csv(os.path.join(outdir, "summary.tsv"), sep="\t", index=False)
    return summary


@click.command()
@click.option("--phenotype", required=True)
@click.option("--ancestry", required=True)
@click.option("--bucket", required=True)
@click.option("--out-prefix", default=None, help="S3 prefix; default sgc/ma/{pheno}/{anc}")
@click.option("--local-out", default="ma_out", help="local working dir")
def main(phenotype, ancestry, bucket, out_prefix, local_out):
    from dataregistry.api import query
    from dataregistry.api.db import DataRegistryReadWriteDB
    engine = DataRegistryReadWriteDB().get_engine()
    prefix = out_prefix or f"sgc/ma/{phenotype}/{ancestry}"

    query.insert_sgc_ma_pending(engine, phenotype, ancestry)
    query.update_sgc_ma_result(engine, phenotype, ancestry, status="RUNNING")
    try:
        cohorts = sel.select_cohorts(engine, phenotype, ancestry)
        click.echo(f"selected {len(cohorts)} cohorts for {phenotype}/{ancestry}")
        s3 = boto3.client("s3", region_name="us-east-1")

        def chunks_fn(co):
            # A generator function: the download + chunked read only happen once
            # this is iterated, and the TemporaryDirectory stays alive for the
            # duration of the iteration (cleaned up right after it's exhausted).
            with tempfile.TemporaryDirectory() as td:
                local = os.path.join(td, os.path.basename(co["s3_path"]))
                s3.download_file(bucket, co["s3_path"], local)
                yield from read_cohort_chunks(local, co["column_mapping"], co.get("cases"), co.get("controls"))

        summary = meta_analyze(cohorts, chunks_fn, local_out, label=f"{phenotype} {ancestry} meta-analysis")
        for name in ["meta.tsv.gz", "manhattan.png", "qq.png", "summary.json", "summary.tsv", "top_loci.tsv"]:
            p = os.path.join(local_out, name)
            if os.path.exists(p):
                s3.upload_file(p, bucket, f"{prefix}/{name}")
        click.echo(json.dumps(summary, indent=2))

        query.update_sgc_ma_result(
            engine, phenotype, ancestry, status="SUCCEEDED",
            meta_lambda_gc=summary["meta_lambda_gc"],
            n_meta_variants=summary["n_meta_variants"],
            n_genome_wide_sig=summary["n_genome_wide_sig"],
            n_cohorts=summary["n_cohorts"],
            n_cohorts_used=summary["n_cohorts_used"],
            manhattan_s3_key=f"{prefix}/manhattan.png",
            qq_s3_key=f"{prefix}/qq.png",
            meta_s3_key=f"{prefix}/meta.tsv.gz",
            summary_json_s3_key=f"{prefix}/summary.json",
            summary_tsv_s3_key=f"{prefix}/summary.tsv",
            top_loci_s3_key=f"{prefix}/top_loci.tsv",
        )
    except Exception as e:
        query.update_sgc_ma_result(engine, phenotype, ancestry, status="FAILED",
                                   error_message=str(e))
        raise


if __name__ == "__main__":
    main()
