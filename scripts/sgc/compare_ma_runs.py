"""Compare an OLD custom-Python meta-analysis (meta.tsv.gz) with a NEW METAL result (metal.tbl.gz).

Usage: PYTHONPATH=. python scripts/sgc/compare_ma_runs.py --old meta.tsv.gz --new metal.tbl.gz [--json OUT]
       [--sample N] [--include-singletons]

Streams both files. The old file is held in a dict keyed by (chrom, pos) (~2-3 GB at 17M rows); the new
file is streamed. Overlap counts, sign agreement and the 2x2 table are exact; --sample N only subsamples
the (beta, -log10 p) pairs used for the Pearson r / median |dbeta| (reservoir sampling).
"""
import argparse
import csv
import gzip
import json
import math
import random
import statistics
from array import array

import click
import numpy as np
import pandas as pd

from sgc_metal.summarize import CLUMP_WINDOW, LOG10_GWS, clump
from sgc_metal.tbl import iter_tbl
from sgc_qc_plots.computations import lambda_gc

P_FLOOR = 1e-300
_COMP = str.maketrans("ACGT", "TGCA")
LOCI_COLS = ["chromosome", "position", "log10p"]


def _open(path):
    return gzip.open(path, "rt") if str(path).endswith(".gz") else open(path, "rt")


def _chrom(c):
    c = str(c)
    return c[3:] if c.lower().startswith("chr") else c


def comp(s):
    return s.translate(_COMP)


def log10_clamped(p):
    return math.log10(max(float(p), P_FLOOR))


def load_old(path):
    """-> (dict (chrom,pos) -> [(ref, alt, beta, se, p)], lambda p array, sig rows, n)"""
    by_pos, pvals, sig, n = {}, array("d"), [], 0
    with _open(path) as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            n += 1
            p = max(float(r["pvalue"]), P_FLOOR)
            chrom, pos = _chrom(r["chromosome"]), int(r["position"])
            by_pos.setdefault((chrom, pos), []).append(
                (r["ref"].upper(), r["alt"].upper(), float(r["beta"]), float(r["se"]), p))
            pvals.append(p)
            if math.log10(p) < LOG10_GWS:
                sig.append((chrom, pos, math.log10(p)))
    return by_pos, pvals, sig, n


def match_alleles(old, new_ref, new_alt):
    """-> (old_row, alt_target) or None. alt_target is the allele (new strand) equal to old alt."""
    ref, alt = old[0], old[1]
    want = {new_ref, new_alt}
    if {ref, alt} == want:
        return alt
    if {comp(ref), comp(alt)} == want:
        return comp(alt)
    return None


def pearson(x, y):
    if len(x) < 2:
        return None
    x, y = np.asarray(x, float), np.asarray(y, float)
    if x.std() == 0 or y.std() == 0:
        return None
    return float(np.corrcoef(x, y)[0, 1])


def lead_loci(sig):
    df = pd.DataFrame(sig, columns=LOCI_COLS)
    return clump(df, CLUMP_WINDOW) if len(df) else df


def nearest(chrom, pos, leads):
    best = None
    for r in leads.itertuples():
        if r.chromosome == chrom:
            d = abs(int(r.position) - pos)
            if best is None or d < best[0]:
                best = (d, r)
    return best


def compare(old_path, new_path, sample=None, include_singletons=False, seed=0):
    rng = random.Random(seed)
    old, old_p, old_sig, n_old = load_old(old_path)
    n_new = n_new_2 = n_new_1 = 0
    new_p, new_sig = array("d"), []
    overlap_pos, matched_pos, new_only_pos = set(), set(), set()
    mism = {}
    n_match = n_sign = 0
    table = {"both": 0, "old_only": 0, "new_only": 0, "neither": 0}
    pairs = []  # (old_beta, new_beta, old_nlp, new_nlp), reservoir if sample
    seen = 0
    for r in iter_tbl(new_path):
        n_new += 1
        if r.n_cohorts < 2:
            n_new_1 += 1
            if not include_singletons:
                continue
        else:
            n_new_2 += 1
        chrom = _chrom(r.chromosome)
        key = (chrom, r.position)
        new_p.append(r.pvalue)
        if r.n_cohorts >= 2 and r.log10p < LOG10_GWS:
            new_sig.append((chrom, r.position, r.log10p))
        rows = old.get(key)
        if rows is None:
            new_only_pos.add(key)
            continue
        overlap_pos.add(key)
        ref, alt = r.ref.upper(), r.alt.upper()
        hit = None
        for o in rows:
            t = match_alleles(o, ref, alt)
            if t is not None:
                hit = (o, t)
                break
        if hit is None:
            mism.setdefault(key, (rows[0][:2], (ref, alt)))
            continue
        matched_pos.add(key)
        o, target = hit
        new_beta = r.effect if r.allele1.upper() == target else -r.effect
        n_match += 1
        if (o[2] > 0) == (new_beta > 0):
            n_sign += 1
        o_sig, n_sig = o[4] < 5e-8, r.log10p < LOG10_GWS
        table["both" if o_sig and n_sig else "old_only" if o_sig else "new_only" if n_sig else "neither"] += 1
        pair = (o[2], new_beta, -math.log10(o[4]), -r.log10p)
        seen += 1
        if not sample or len(pairs) < sample:
            pairs.append(pair)
        else:
            j = rng.randrange(seen)
            if j < sample:
                pairs[j] = pair
    # a position matched by one new row is not a mismatch even if another new row at it mismatched
    mism = {k: v for k, v in mism.items() if k not in matched_pos}
    old_keys = set(old)
    ob, nb, ol, nl = (list(c) for c in zip(*pairs)) if pairs else ([], [], [], [])
    old_leads, new_leads = lead_loci(old_sig), lead_loci(new_sig)

    def within(a, b):
        return sum(1 for r in a.itertuples() if (m := nearest(r.chromosome, int(r.position), b)) and m[0] <= CLUMP_WINDOW)

    top = []
    for r in old_leads.head(10).itertuples():
        m = nearest(r.chromosome, int(r.position), new_leads)
        top.append({"chromosome": r.chromosome, "position": int(r.position), "old_log10p": float(r.log10p),
                    "new_chromosome": m[1].chromosome if m else None,
                    "new_position": int(m[1].position) if m else None,
                    "new_log10p": float(m[1].log10p) if m else None, "distance": m[0] if m else None})
    lam = lambda _, p: float(lambda_gc(pd.Series(np.frombuffer(p, dtype=float)))) if len(p) else None
    return {
        "counts": {"old_variants": n_old, "new_rows_total": n_new, "new_rows_2plus": n_new_2,
                   "new_singletons": n_new_1, "singletons_included": include_singletons},
        "overlap": {"both": len(overlap_pos), "old_only": len(old_keys - overlap_pos),
                    "new_only": len(new_only_pos), "allele_match": len(matched_pos),
                    "allele_mismatch": len(mism),
                    "mismatch_examples": [{"chromosome": k[0], "position": k[1], "old": list(v[0]),
                                           "new": list(v[1])} for k, v in list(mism.items())[:5]]},
        "agreement": {"n_variants": n_match, "n_pairs_for_correlation": len(pairs),
                      "beta_pearson_r": pearson(ob, nb),
                      "sign_agreement_pct": 100.0 * n_sign / n_match if n_match else None,
                      "median_abs_dbeta": statistics.median(abs(a - b) for a, b in zip(ob, nb)) if pairs else None,
                      "neglog10p_pearson_r": pearson(ol, nl), "gws_table": table},
        "top_loci": {"n_old_leads": len(old_leads), "n_new_leads": len(new_leads),
                     "old_leads_with_new_within_500kb": within(old_leads, new_leads),
                     "new_leads_with_old_within_500kb": within(new_leads, old_leads), "top_old_leads": top},
        "lambda_gc": {"old": lam(None, old_p), "new": lam(None, new_p)},
    }


def _f(x, fmt="{:.4g}"):
    return "NA" if x is None else fmt.format(x)


def render(res):
    c, o, a, t, g = res["counts"], res["overlap"], res["agreement"], res["top_loci"], res["agreement"]["gws_table"]
    L = ["## MA comparison: old (Python) vs new (METAL)", "", "### Row counts",
         f"- old variants: {c['old_variants']}", f"- new rows total: {c['new_rows_total']}",
         f"- new rows with >=2 cohorts: {c['new_rows_2plus']}", f"- new singletons: {c['new_singletons']}"
         + (" (included)" if c["singletons_included"] else " (excluded)"), "",
         "### Overlap on chrom:pos", f"- both: {o['both']}", f"- old only: {o['old_only']}",
         f"- new only: {o['new_only']}", f"- allele-set match: {o['allele_match']}",
         f"- allele mismatch: {o['allele_mismatch']}"]
    for e in o["mismatch_examples"]:
        L.append(f"  - {e['chromosome']}:{e['position']} old {'/'.join(e['old'])} vs new {'/'.join(e['new'])}")
    L += ["", f"### Allele-matched variants (n={a['n_variants']}, correlation pairs={a['n_pairs_for_correlation']})",
          f"- Pearson r beta (new oriented to old alt): {_f(a['beta_pearson_r'], '{:.5f}')}",
          f"- sign agreement: {_f(a['sign_agreement_pct'], '{:.3f}')}%",
          f"- median |dbeta|: {_f(a['median_abs_dbeta'])}",
          f"- Pearson r -log10 p: {_f(a['neglog10p_pearson_r'], '{:.5f}')}", "",
          "| p<5e-8 | new sig | new not sig |", "|---|---|---|",
          f"| old sig | {g['both']} | {g['old_only']} |", f"| old not sig | {g['new_only']} | {g['neither']} |", "",
          "### Top loci (500 kb clumps)", f"- old leads: {t['n_old_leads']}; new leads: {t['n_new_leads']}",
          f"- old leads with a new lead within 500 kb: {t['old_leads_with_new_within_500kb']}",
          f"- new leads with an old lead within 500 kb: {t['new_leads_with_old_within_500kb']}", "",
          "| old lead | old log10p | nearest new lead | new log10p | dist (bp) |", "|---|---|---|---|---|"]
    for r in t["top_old_leads"]:
        nl = "none" if r["new_position"] is None else f"{r['new_chromosome']}:{r['new_position']}"
        L.append(f"| {r['chromosome']}:{r['position']} | {r['old_log10p']:.2f} | {nl} | "
                 f"{_f(r['new_log10p'], '{:.2f}')} | {_f(r['distance'], '{}')} |")
    L += ["", f"### Lambda GC", f"- old: {_f(res['lambda_gc']['old'], '{:.4f}')}; "
          f"new: {_f(res['lambda_gc']['new'], '{:.4f}')}", "",
          "Note: per-cohort inputs are not compared (the two pipelines summarise them differently)."]
    return "\n".join(L)


@click.command()
@click.option("--old", "old_path", required=True, type=click.Path(exists=True))
@click.option("--new", "new_path", required=True, type=click.Path(exists=True))
@click.option("--json", "json_out", type=click.Path(), default=None)
@click.option("--sample", type=int, default=None, help="Subsample N matched variants for correlations only.")
@click.option("--include-singletons", is_flag=True)
def main(old_path, new_path, json_out, sample, include_singletons):
    res = compare(old_path, new_path, sample, include_singletons)
    click.echo(render(res))
    if json_out:
        with open(json_out, "w") as fh:
            json.dump(res, fh, indent=2)


if __name__ == "__main__":
    main()
