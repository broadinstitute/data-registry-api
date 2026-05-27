from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from sgc_qc_plots.plots import render_manhattan, render_qq


def _toy_df(n=10_000, seed=0):
    rng = np.random.default_rng(seed)
    return pd.DataFrame({
        "chromosome": rng.choice([str(i) for i in range(1, 23)] + ["X"], size=n),
        "position": rng.integers(1, 250_000_000, size=n),
        "pvalue": rng.uniform(1e-12, 1.0, size=n),
    })


def test_render_manhattan_creates_png(tmp_path: Path):
    out = tmp_path / "manhattan.png"
    render_manhattan(_toy_df(), str(out), title="test ds | phenotype | EUR")
    assert out.exists() and out.stat().st_size > 5_000


def test_render_qq_creates_png(tmp_path: Path):
    out = tmp_path / "qq.png"
    render_qq(_toy_df()["pvalue"], str(out), title="test ds | phenotype | EUR", lambda_gc=1.03)
    assert out.exists() and out.stat().st_size > 5_000


def test_render_manhattan_handles_string_pvalues(tmp_path):
    # Simulates CSV-loaded data where pvalue arrives as object dtype
    df = _toy_df(n=500)
    df["pvalue"] = df["pvalue"].astype(str)
    out = tmp_path / "manhattan.png"
    render_manhattan(df, str(out))
    assert out.exists() and out.stat().st_size > 5_000


def test_render_qq_empty_raises(tmp_path):
    out = tmp_path / "qq.png"
    with pytest.raises(ValueError):
        render_qq(pd.Series([], dtype=float), str(out))
