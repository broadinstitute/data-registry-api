"""Manhattan and QQ plot rendering. Uses matplotlib Agg backend (no display)."""
from typing import Optional

import matplotlib
matplotlib.use("Agg")  # noqa: E402
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# Two-shade grey alternation requested by reviewers (no rainbow colors).
_COLORS = ["#666666", "#bbbbbb"]
_CHROMS = [str(i + 1) for i in range(22)] + ["X", "Y"]
_CHROM_LEN = {
    "1": 247249719, "2": 242951149, "3": 199501827, "4": 191273063,
    "5": 180857866, "6": 170899992, "7": 158821424, "8": 146274826,
    "9": 140273252, "10": 135374737, "11": 134452384, "12": 132349534,
    "13": 114142980, "14": 106368585, "15": 100338915, "16": 88827254,
    "17": 78774742, "18": 76117153, "19": 63811651, "20": 62435964,
    "21": 46944323, "22": 49691432, "X": 154913754, "Y": 57772954,
}

def _build_chrom_layout(chroms: list[str]):
    """Layout x-coordinates only for the chromosomes actually present in this file.

    Files commonly omit X/Y (e.g. male-only analyses, autosome-only pipelines).
    Reserving empty axis space for absent chromosomes wastes the visible range,
    so callers pass the chromosomes that are present rather than reusing a
    fixed full-genome layout.
    """
    frame: dict[str, dict] = {}
    xtick: dict[str, int] = {}
    pos = 0
    for i, c in enumerate(chroms):
        frame[c] = {"x": pos, "color": _COLORS[i % 2]}
        xtick[c] = pos + _CHROM_LEN[c] // 2
        pos += _CHROM_LEN[c]
    return frame, xtick, pos  # pos == xmax


def render_manhattan(df: pd.DataFrame, out_path: str, *, title: Optional[str] = None,
                     thin_above_p: float = 1e-3, thin_factor: int = 10) -> None:
    """Render a Manhattan plot. Thins points with p > thin_above_p by thin_factor
    to keep matplotlib responsive on large files.
    Expects a DataFrame with columns 'chromosome' (str), 'position' (int-like), 'pvalue' (float-like)."""
    work = df[df["chromosome"].isin(_CHROMS)].copy()
    work["pvalue"] = work["pvalue"].astype(float)
    work["y"] = -np.log10(work["pvalue"])

    keep_sig = work["pvalue"] <= thin_above_p
    thinned = work.loc[~keep_sig].iloc[::thin_factor]
    work = pd.concat([work.loc[keep_sig], thinned], ignore_index=True)

    present_chroms = [c for c in _CHROMS if c in set(work["chromosome"].unique())]
    chrom_frame, chrom_xtick, xmax = _build_chrom_layout(present_chroms)

    work["x_base"] = work["chromosome"].map(lambda c: chrom_frame[c]["x"])
    work["color"] = work["chromosome"].map(lambda c: chrom_frame[c]["color"])
    work["x"] = work["x_base"] + work["position"].astype(int)

    fig, ax = plt.subplots(figsize=(15, 6))
    ax.set_ylabel("-log10(p)")
    ax.set_xlabel("chromosome")
    ax.set_xticks([chrom_xtick[c] for c in present_chroms])
    ax.set_xticklabels(present_chroms)
    ax.hlines(5, 0, xmax, linestyle="dashed", color="gray")    # suggestive 1e-5
    ax.hlines(8, 0, xmax, linestyle="dashed", color="red")     # GWAS 5e-8
    ax.scatter(work["x"], work["y"], s=4, c=work["color"], rasterized=True)
    if title:
        ax.set_title(title)
    fig.tight_layout()
    fig.savefig(out_path, dpi=120)
    plt.close(fig)


def render_qq(pvalues: pd.Series, out_path: str, *,
              title: Optional[str] = None, lambda_gc: Optional[float] = None) -> None:
    """Render a QQ plot of -log10(p) vs uniform expected."""
    p = pvalues.astype(float).sort_values().reset_index(drop=True)
    n = len(p)
    if n == 0:
        raise ValueError("render_qq requires at least one p-value")
    expected = -np.log10(np.arange(1, n + 1) / (n + 1))
    observed = -np.log10(p)

    fig, ax = plt.subplots(figsize=(7, 7))
    ax.scatter(expected, observed, s=4, color="#08306b", rasterized=True)
    lim = max(expected.max(), observed.max())
    ax.plot([0, lim], [0, lim], color="red", linewidth=1)
    ax.set_xlabel("expected -log10(p)")
    ax.set_ylabel("observed -log10(p)")
    parts = [title] if title else []
    if lambda_gc is not None:
        parts.append(f"λ_GC = {lambda_gc:.3f}")
    if parts:
        ax.set_title(" | ".join(parts))
    fig.tight_layout()
    fig.savefig(out_path, dpi=120)
    plt.close(fig)
