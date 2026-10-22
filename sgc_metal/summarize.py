"""Summaries, plots and top loci derived from a METAL .tbl (single streaming pass)."""
import math
import os

import pandas as pd

from sgc_metal.tbl import TOP_LOCI_COLUMNS, iter_tbl, row_to_locus
from sgc_qc_plots.computations import lambda_gc
from sgc_qc_plots.plots import render_manhattan, render_qq

GWS = 5e-8
LOG10_GWS = math.log10(GWS)
CLUMP_WINDOW = 500_000

# Where the four frequency columns of metal.tbl.gz come from (summary.json freq_source).
FREQ_PASS1, FREQ_PASS2, FREQ_NONE = "pass1", "pass2_eaf_cohorts", "none"

CAVEATS = [
    "METAL fixed-effect inverse-variance (SCHEME STDERR); SE not rescaled across cohorts",
    "palindromic A/T and C/G SNPs retained; alleles aligned to the GRCh38 reference for SNP_ID only",
    "variants present in only one cohort are kept in metal.tbl.gz but excluded from plots, lambda, and top loci",
    "GRCh37-only cohorts excluded (GRCh38-effective subset only)",
]
CAVEAT_FREQ_PASS1 = "Effect_AF averaged only over cohorts reporting EAF (AVERAGEFREQ ON; every used cohort reports EAF)"
CAVEAT_FREQ_PASS2 = ("allele frequencies (Freq1/FreqSE/MinFreq/MaxFreq) come from a second METAL pass over the "
                     "cohorts that report EAF; METAL would otherwise count a missing frequency as 0")
CAVEAT_FREQ_NONE = "allele frequencies (Freq1/FreqSE/MinFreq/MaxFreq) are NA: no cohort reports EAF"
FREQ_CAVEATS = {FREQ_PASS1: CAVEAT_FREQ_PASS1, FREQ_PASS2: CAVEAT_FREQ_PASS2, FREQ_NONE: CAVEAT_FREQ_NONE}


def filter_caveat(maf_min: float, info_min: float) -> str:
    return (f"variants filtered pre-meta per cohort: MAF>={maf_min:g} and INFO>={info_min:g} where the cohort "
            "provides those columns (see sgc/munge/{file_id}/munge.json)")


def allele_mismatch_caveat(total: int) -> str:
    return (f"{total} cohort rows were dropped by METAL because their allele strings did not match the marker's "
            "alleles (indels reported in a different form); see per_cohort.n_allele_mismatch")


def build_caveats(*, freq_source: str, maf_min: float, info_min: float, n_allele_mismatch: int = 0) -> list[str]:
    out = list(CAVEATS) + [filter_caveat(maf_min, info_min), FREQ_CAVEATS[freq_source]]
    if n_allele_mismatch:
        out.append(allele_mismatch_caveat(n_allele_mismatch))
    return out


def clump(sig: pd.DataFrame, window: int = CLUMP_WINDOW) -> pd.DataFrame:
    """Greedy distance-based clumping: keep the most significant variant (smallest log10p,
    which stays ordered below double range), drop others within +/- window bp on the same
    chromosome, repeat."""
    sig = sig.sort_values("log10p", kind="mergesort").reset_index(drop=True)
    leads, taken = [], []
    for _, r in sig.iterrows():
        if any(r["chromosome"] == c and abs(int(r["position"]) - p) <= window for c, p in taken):
            continue
        leads.append(r)
        taken.append((r["chromosome"], int(r["position"])))
    return pd.DataFrame(leads, columns=sig.columns)


def totals_from_per_cohort(per_cohort):
    """Sum cases/controls over cohorts actually used in the meta (skipped
    excluded). None-safe: returns None for a total when no used cohort carries
    that count."""
    used = [c for c in per_cohort if not c.get("skipped")]
    cases = [c["cases"] for c in used if c.get("cases") is not None]
    controls = [c["controls"] for c in used if c.get("controls") is not None]
    return (sum(cases) if cases else None, sum(controls) if controls else None)


def summarize_tbl(tbl_path: str, out_dir: str, label: str) -> dict:
    os.makedirs(out_dir, exist_ok=True)
    n_total = 0
    chroms, positions, pvalues = [], [], []
    sig = []
    for r in iter_tbl(tbl_path):
        n_total += 1
        if r.n_cohorts < 2:
            continue
        chroms.append(r.chromosome)
        positions.append(r.position)
        pvalues.append(r.pvalue)  # clamped to the smallest double: finite in plots and lambda
        if r.log10p < LOG10_GWS:  # exact below double range
            sig.append(row_to_locus(r))

    top = clump(pd.DataFrame(sig, columns=TOP_LOCI_COLUMNS))
    top[TOP_LOCI_COLUMNS].to_csv(os.path.join(out_dir, "top_loci.tsv"), sep="\t", index=False)

    lam = None
    if pvalues:
        df = pd.DataFrame({"chromosome": chroms, "position": positions, "pvalue": pvalues})
        lam = float(lambda_gc(df["pvalue"]))
        render_manhattan(df, os.path.join(out_dir, "manhattan.png"), title=f"{label} - METAL meta-analysis")
        render_qq(df["pvalue"], os.path.join(out_dir, "qq.png"), title=f"{label} - METAL meta-analysis",
                  lambda_gc=lam)
    return {"n_meta_variants": n_total, "n_meta_variants_2plus": len(pvalues), "meta_lambda_gc": lam,
            "n_genome_wide_sig": len(sig), "n_top_loci": len(top)}


def build_summary(*, cohorts: list[dict], used: list[dict], skipped: list[dict], ignored: list[dict],
                  tbl_stats: dict, engine_version: str, extra: dict | None = None,
                  caveats: list[str] | None = None) -> dict:
    """caveats defaults to the fixed CAVEATS; meta_analyze passes build_caveats(...)."""
    ident = ("dataset", "file_id", "cohort", "ancestry", "sex")
    per_cohort = []
    for co in sorted(used, key=lambda c: c.get("dataset") or ""):
        per_cohort.append({
            **{k: co.get(k) for k in ident}, "cases": co.get("cases"), "controls": co.get("controls"),
            "n_variants_in": co.get("munge_n_in"), "n_variants_used": co.get("munge_n_out"),
            "n_with_rsid": co.get("munge_n_with_rsid"), "munge_ref_version": co.get("munge_ref_version"),
            # from metal.log: rows METAL kept, and rows it dropped as "Bad alleles"
            "n_markers_processed": co.get("metal_n_markers_processed"),
            "n_allele_mismatch": co.get("metal_n_allele_mismatch"),
        })
    for co in skipped:
        per_cohort.append({**{k: co.get(k) for k in ident}, "cases": co.get("cases"),
                           "controls": co.get("controls"), "skipped": True, "reason": co.get("reason")})
    for co in ignored:
        per_cohort.append({"cohort": co.get("cohort"), "dataset": co.get("dataset"), "file_id": co.get("file_id"),
                           "ancestry": co.get("ancestry"), "sex": co.get("sex"), "skipped": True,
                           "reason": f"MA ignore-list: {co.get('reason')}"})
    total_cases, total_controls = totals_from_per_cohort(per_cohort)
    summary = {
        "n_cohorts": len(cohorts),
        "n_cohorts_used": len(used),
        "total_cases": total_cases,
        "total_controls": total_controls,
        "per_cohort": per_cohort,
        "engine": engine_version,
        "scheme": "STDERR",
        "n_meta_variants": tbl_stats["n_meta_variants"],
        "n_meta_variants_2plus": tbl_stats["n_meta_variants_2plus"],
        "meta_lambda_gc": tbl_stats["meta_lambda_gc"],
        "n_genome_wide_sig": tbl_stats["n_genome_wide_sig"],
        "n_top_loci": tbl_stats["n_top_loci"],
        "caveats": list(CAVEATS if caveats is None else caveats),
    }
    if extra:
        summary.update(extra)
    return summary
