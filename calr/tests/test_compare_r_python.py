#!/usr/bin/env python3
"""
Cross-validation test suite: compare Python CalR conversion with R CalR output.

Tests all three supported vendor formats (TSE, Oxymax/CLAMS, Sable).
For each format:
  1. Run Python conversion on the example file
  2. Shell out to Rscript to run the R equivalent
  3. Compare dimensions, column names, and numeric values
"""

import sys
import subprocess
import pytest
import pandas as pd
import numpy as np
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from calr.loaders import load_cal_file
from calr.tse_loader import load_tse_file, convert_tse
from calr.oxymax_loader import load_oxymax_file, convert_oxymax
from calr.sable_loader import load_sable_file, convert_sable

TEST_DATA_DIR = Path(__file__).parent.parent / 'test_data'
CALR_R_DIR = Path.home() / 'code-repos' / 'broad' / 'calr' / 'R'

# Numeric columns to compare with tolerance
KEY_NUMERIC_COLS = [
    'subject.mass', 'vo2', 'vco2', 'ee', 'ee.acc', 'rer',
    'feed', 'feed.acc', 'drink', 'drink.acc',
    'xytot', 'xyamb', 'wheel', 'wheel.acc',
    'pedmeter', 'allmeter', 'body.temp',
    'exp.minute', 'exp.hour', 'exp.day',
]

# Tolerance for numeric comparisons
TOLERANCE = 0.01
ACCUMULATED_TOLERANCE = 0.5  # ee.acc, feed.acc etc can drift


def _r_source_block():
    """Return the R code to source required CalR functions."""
    return f"""
suppressMessages(library(shiny))
suppressMessages(library(data.table))
suppressMessages(library(lubridate))
suppressMessages(library(stringr))
suppressMessages(library(stringi))
suppressMessages(library(tidyr))
suppressMessages(library(plyr))
if (requireNamespace("arrow", quietly = TRUE)) suppressMessages(library(arrow))

# Workaround stray 'br' (leftover browser()) in modSable.R
br <- NULL

calr_r_dir <- "{CALR_R_DIR}"
required_files <- c(
  "period.R", "asdate.R", "asdatetime.R", "utils.R", "maths.R",
  "loadCalFile.R", "loadTSEFile.R", "modTSE.R",
  "loadOxyFile.R", "modOxy.R",
  "loadSableFile.R", "modSable.R",
  "retrofitCalR.R"
)

for (r_file in required_files) {{
  file_path <- file.path(calr_r_dir, r_file)
  if (file.exists(file_path)) {{
    source(file_path)
  }}
}}
"""


def _run_r_script(r_code: str) -> bool:
    """Run an R script and return True on success."""
    try:
        result = subprocess.run(
            ["Rscript", "-"],
            input=r_code,
            text=True,
            capture_output=True,
            timeout=120,
        )
        if result.returncode != 0:
            print(f"R STDOUT: {result.stdout}")
            print(f"R STDERR: {result.stderr}")
            return False
        if result.stdout:
            print(f"R: {result.stdout.strip()}")
        return True
    except subprocess.TimeoutExpired:
        print("R script timed out")
        return False
    except FileNotFoundError:
        pytest.skip("Rscript not found")
        return False


def _sort_for_comparison(df: pd.DataFrame) -> pd.DataFrame:
    """Sort a CalR DataFrame by cage (numeric) + Date.Time for stable comparison.

    R and Python may order cages differently (R sorts alphabetically via ddply,
    Python sorts numerically). Sorting both by the same key aligns them.
    """
    df = df.copy()
    sort_cols = []
    if 'cage' in df.columns:
        df['_cage_int'] = pd.to_numeric(df['cage'], errors='coerce')
        sort_cols.append('_cage_int')
    if 'Date.Time' in df.columns:
        sort_cols.append('Date.Time')
    if sort_cols:
        df = df.sort_values(sort_cols).reset_index(drop=True)
    if '_cage_int' in df.columns:
        df = df.drop(columns=['_cage_int'])
    return df


def _compare_dataframes(r_df: pd.DataFrame, py_df: pd.DataFrame, label: str):
    """Compare R and Python DataFrames, asserting key properties match."""
    # Compare row counts
    assert r_df.shape[0] == py_df.shape[0], (
        f"[{label}] Row count mismatch: R={r_df.shape[0]}, Python={py_df.shape[0]}"
    )

    # Sort both for stable row-by-row comparison
    r_df = _sort_for_comparison(r_df)
    py_df = _sort_for_comparison(py_df)

    # Check common columns exist
    r_cols = set(r_df.columns)
    py_cols = set(py_df.columns)
    common_cols = r_cols & py_cols

    # Report but don't fail on extra columns in either direction
    r_only = r_cols - py_cols
    py_only = py_cols - r_cols
    if r_only:
        print(f"[{label}] Columns in R only: {r_only}")
    if py_only:
        print(f"[{label}] Columns in Python only: {py_only}")

    # Compare numeric columns
    mismatches = []
    for col in KEY_NUMERIC_COLS:
        if col not in common_cols:
            continue

        r_vals = pd.to_numeric(r_df[col], errors='coerce')
        py_vals = pd.to_numeric(py_df[col], errors='coerce')

        # Both NaN is OK
        both_nan = r_vals.isna() & py_vals.isna()
        non_nan = ~r_vals.isna() & ~py_vals.isna()

        if not non_nan.any():
            continue  # both all NaN

        tol = ACCUMULATED_TOLERANCE if '.acc' in col else TOLERANCE
        diffs = np.abs(r_vals[non_nan].values - py_vals[non_nan].values)
        max_diff = float(diffs.max())

        if max_diff > tol:
            mismatches.append((col, max_diff))
            print(f"[{label}] {col}: max_diff={max_diff:.6f} (tolerance={tol})")
            print(f"  R first 5:      {list(r_vals.head(5))}")
            print(f"  Python first 5:  {list(py_vals.head(5))}")

    # Compare subject.id
    if 'subject.id' in common_cols:
        r_ids = r_df['subject.id'].astype(str).values
        py_ids = py_df['subject.id'].astype(str).values
        id_match = (r_ids == py_ids).all()
        if not id_match:
            print(f"[{label}] subject.id mismatch:")
            print(f"  R:  {list(r_ids[:5])}")
            print(f"  Py: {list(py_ids[:5])}")

    # Compare cage
    if 'cage' in common_cols:
        r_cages = r_df['cage'].astype(str).values
        py_cages = py_df['cage'].astype(str).values
        cage_match = (r_cages == py_cages).all()
        if not cage_match:
            print(f"[{label}] cage mismatch:")
            print(f"  R:  {list(r_cages[:5])}")
            print(f"  Py: {list(py_cages[:5])}")

    assert not mismatches, (
        f"[{label}] Numeric mismatches (col, max_diff): {mismatches}"
    )


# ---------------------------------------------------------------------------
# TSE Tests
# ---------------------------------------------------------------------------

class TestTSEConversion:
    """Compare TSE conversion between Python and R."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        self.tse_file = TEST_DATA_DIR / 'calr_tse.csv'
        if not self.tse_file.exists():
            pytest.skip("TSE test file not found")
        self.r_output = tmp_path / 'r_output_tse.csv'
        self.py_output = tmp_path / 'py_output_tse.csv'

    def test_tse_python_runs(self):
        """Python TSE conversion produces output."""
        py_df = load_cal_file(self.tse_file)
        assert py_df is not None
        assert len(py_df) > 0

    def test_tse_matches_r(self):
        """Python TSE output matches R output."""
        # Run Python
        py_df = load_cal_file(self.tse_file)
        py_df.to_csv(self.py_output, index=False)

        # Run R
        r_code = _r_source_block() + f"""
raw_tse <- loadTSEFile(in.file="{self.tse_file}")
r_output <- modTSE(raw_tse)
write.csv(r_output, "{self.r_output}", row.names = FALSE)
cat("TSE R rows:", nrow(r_output), "\\n")
"""
        assert _run_r_script(r_code), "R TSE conversion failed"

        r_df = pd.read_csv(self.r_output)
        _compare_dataframes(r_df, py_df, "TSE")


# ---------------------------------------------------------------------------
# Oxymax/CLAMS Tests
# ---------------------------------------------------------------------------

class TestOxymaxConversion:
    """Compare Oxymax/CLAMS conversion between Python and R."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        self.oxy_file = TEST_DATA_DIR / 'clams_example.csv'
        if not self.oxy_file.exists():
            pytest.skip("CLAMS test file not found")
        self.r_output = tmp_path / 'r_output_oxy.csv'

    def test_oxymax_python_runs(self):
        """Python Oxymax conversion produces output."""
        raw = load_oxymax_file(self.oxy_file)
        py_df = convert_oxymax(raw)
        assert py_df is not None
        assert len(py_df) > 0

    def test_oxymax_matches_r(self):
        """Python Oxymax output matches R output."""
        # Run Python
        raw = load_oxymax_file(self.oxy_file)
        py_df = convert_oxymax(raw)

        # Run R
        r_code = _r_source_block() + f"""
raw_oxy <- loadOxyFile(input.file="{self.oxy_file}")
r_output <- modOxy(list(raw_oxy))
write.csv(r_output, "{self.r_output}", row.names = FALSE)
cat("Oxymax R rows:", nrow(r_output), "\\n")
"""
        assert _run_r_script(r_code), "R Oxymax conversion failed"

        r_df = pd.read_csv(self.r_output)
        _compare_dataframes(r_df, py_df, "Oxymax")


# ---------------------------------------------------------------------------
# Sable Tests
# ---------------------------------------------------------------------------

class TestSableConversion:
    """Compare Sable conversion between Python and R."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        self.sable_file = TEST_DATA_DIR / 'sable_example.csv'
        if not self.sable_file.exists():
            pytest.skip("Sable test file not found")
        self.r_output = tmp_path / 'r_output_sable.csv'

    def test_sable_python_runs(self):
        """Python Sable conversion produces output."""
        raw = load_sable_file(self.sable_file)
        py_df = convert_sable(raw)
        assert py_df is not None
        assert len(py_df) > 0

    def test_sable_matches_r(self):
        """Python Sable output matches R output."""
        # Run Python
        raw = load_sable_file(self.sable_file)
        py_df = convert_sable(raw)

        # Run R - override loadSableFile to use read.csv if arrow not available
        r_code = _r_source_block() + f"""
if (!requireNamespace("arrow", quietly = TRUE)) {{
  loadSableFile <- function(in.file, header=T, separater=",", quote='"') {{
    s <- read.csv(in.file, header = header, sep = separater, quote = quote)
    s <- s[, colSums(is.na(s)) < nrow(s)]
    s <- s[complete.cases(s), ]
    s
  }}
}}
raw_sable <- loadSableFile(in.file="{self.sable_file}")
r_output <- modSable(sableData=raw_sable)
write.csv(r_output, "{self.r_output}", row.names = FALSE)
cat("Sable R rows:", nrow(r_output), "\\n")
"""
        assert _run_r_script(r_code), "R Sable conversion failed"

        r_df = pd.read_csv(self.r_output)
        _compare_dataframes(r_df, py_df, "Sable")


# ---------------------------------------------------------------------------
# Format Detection Tests
# ---------------------------------------------------------------------------

class TestFormatDetection:
    """Test that format auto-detection works for all file types."""

    def test_detect_tse(self):
        f = TEST_DATA_DIR / 'calr_tse.csv'
        if f.exists():
            from calr.loaders import detect_format
            assert detect_format(f) == 'tse'

    def test_detect_oxymax(self):
        f = TEST_DATA_DIR / 'clams_example.csv'
        if f.exists():
            from calr.loaders import detect_format
            assert detect_format(f) == 'oxymax'

    def test_detect_sable(self):
        f = TEST_DATA_DIR / 'sable_example.csv'
        if f.exists():
            from calr.loaders import detect_format
            assert detect_format(f) == 'sable'


# ---------------------------------------------------------------------------
# Standalone runner
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
