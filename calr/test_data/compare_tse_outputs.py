#!/usr/bin/env python3
"""
Compare Python CalR TSE conversion output with R CalR output.

This script:
1. Runs Python TSE conversion
2. Shells out to R to run R TSE conversion
3. Compares the outputs
"""

import sys
import subprocess
import pandas as pd
import numpy as np
from pathlib import Path

# Add parent directory to path to import calr
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from calr.loaders import load_cal_file


def run_r_conversion(tse_file: str, output_file: str) -> bool:
    """Run R CalR conversion via shell."""
    r_script = f"""
# Load required libraries
suppressMessages(library(shiny))
suppressMessages(library(data.table))
suppressMessages(library(lubridate))

# Source R CalR functions
calr_r_dir <- "/home/dhite/code-repos/broad/calr/R"
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

# Load and convert TSE file
cat("Running R TSE conversion...\\n")
raw_tse <- loadTSEFile(in.file="{tse_file}")
r_output <- modTSE(raw_tse)

# Save output
write.csv(r_output, "{output_file}", row.names = FALSE)
cat("R conversion complete:", nrow(r_output), "rows\\n")
"""
    
    try:
        result = subprocess.run(
            ["Rscript", "-"],
            input=r_script,
            text=True,
            capture_output=True,
            check=True
        )
        print(result.stdout)
        if result.stderr:
            print("R warnings:", result.stderr, file=sys.stderr)
        return True
    except subprocess.CalledProcessError as e:
        print(f"R conversion failed: {e}", file=sys.stderr)
        print("STDOUT:", e.stdout, file=sys.stderr)
        print("STDERR:", e.stderr, file=sys.stderr)
        return False


def compare_dataframes(r_df: pd.DataFrame, py_df: pd.DataFrame) -> dict:
    """Compare R and Python DataFrames and return comparison results."""
    results = {
        'dimensions_match': r_df.shape == py_df.shape,
        'r_shape': r_df.shape,
        'py_shape': py_df.shape,
        'columns_match': set(r_df.columns) == set(py_df.columns),
        'r_only_cols': list(set(r_df.columns) - set(py_df.columns)),
        'py_only_cols': list(set(py_df.columns) - set(r_df.columns)),
        'common_cols': list(set(r_df.columns) & set(py_df.columns)),
        'value_comparisons': {}
    }
    
    # Compare values in common columns
    for col in sorted(results['common_cols']):
        r_vals = r_df[col]
        py_vals = py_df[col]
        
        comparison = {
            'dtype_r': str(r_vals.dtype),
            'dtype_py': str(py_vals.dtype),
            'sample_r': list(r_vals.head(5)),
            'sample_py': list(py_vals.head(5))
        }
        
        # For numeric columns, compute differences
        if pd.api.types.is_numeric_dtype(r_vals) and pd.api.types.is_numeric_dtype(py_vals):
            # Align dataframes if needed
            r_numeric = pd.to_numeric(r_vals, errors='coerce')
            py_numeric = pd.to_numeric(py_vals, errors='coerce')
            
            # Check for NaN equality
            both_nan = r_numeric.isna() & py_numeric.isna()
            
            # For non-NaN values, compute differences
            non_nan_mask = ~r_numeric.isna() & ~py_numeric.isna()
            if non_nan_mask.any():
                diffs = np.abs(r_numeric[non_nan_mask] - py_numeric[non_nan_mask])
                comparison['max_diff'] = float(diffs.max())
                comparison['mean_diff'] = float(diffs.mean())
                comparison['matches'] = (diffs < 0.001).all()
            else:
                comparison['matches'] = both_nan.all()
                comparison['max_diff'] = 0.0
                comparison['mean_diff'] = 0.0
        else:
            # For non-numeric, just check equality
            comparison['matches'] = (r_vals.astype(str) == py_vals.astype(str)).all()
        
        results['value_comparisons'][col] = comparison
    
    return results


def print_comparison_report(results: dict):
    """Print a detailed comparison report."""
    print("\n" + "="*70)
    print("TSE CONVERSION COMPARISON: Python vs R")
    print("="*70)
    
    # Dimensions
    print(f"\n📊 DIMENSIONS")
    print(f"  R:      {results['r_shape'][0]:,} rows × {results['r_shape'][1]} columns")
    print(f"  Python: {results['py_shape'][0]:,} rows × {results['py_shape'][1]} columns")
    if results['dimensions_match']:
        print("  ✓ Dimensions match")
    else:
        print("  ✗ Dimension mismatch!")
    
    # Columns
    print(f"\n📋 COLUMNS")
    print(f"  Common columns: {len(results['common_cols'])}")
    if results['r_only_cols']:
        print(f"  ⚠ In R only: {', '.join(results['r_only_cols'])}")
    if results['py_only_cols']:
        print(f"  ⚠ In Python only: {', '.join(results['py_only_cols'])}")
    if results['columns_match']:
        print("  ✓ All columns match")
    
    # Key columns to highlight
    key_cols = ['subject.id', 'subject.mass', 'cage', 'vo2', 'vco2', 'ee', 'rer', 
                'feed', 'feed.acc', 'drink', 'drink.acc', 'xytot', 'xyamb']
    
    print(f"\n🔍 VALUE COMPARISON (Key Columns)")
    mismatches = []
    
    for col in key_cols:
        if col not in results['value_comparisons']:
            continue
        
        comp = results['value_comparisons'][col]
        status = "✓" if comp['matches'] else "✗"
        
        print(f"\n  {col}:")
        print(f"    Status: {status}")
        
        if pd.api.types.is_numeric_dtype(comp['sample_r'][0].__class__):
            if 'max_diff' in comp:
                print(f"    Max diff: {comp['max_diff']:.6f}")
                if comp['max_diff'] > 0.001:
                    print(f"    Mean diff: {comp['mean_diff']:.6f}")
                    mismatches.append(col)
        
        # Show sample values for first few rows
        print(f"    R     : {comp['sample_r'][:3]}")
        print(f"    Python: {comp['sample_py'][:3]}")
    
    # Summary
    print(f"\n{'='*70}")
    print("SUMMARY")
    print("="*70)
    
    if (results['dimensions_match'] and results['columns_match'] and 
        not mismatches):
        print("✅ SUCCESS: Python TSE loader perfectly replicates R behavior!")
        print("   All dimensions, columns, and values match.")
        return True
    else:
        print("⚠️  ISSUES DETECTED:")
        if not results['dimensions_match']:
            print("   - Dimension mismatch")
        if not results['columns_match']:
            print("   - Column mismatch")
        if mismatches:
            print(f"   - Value mismatches in: {', '.join(mismatches)}")
        return False


def main():
    """Main comparison function."""
    tse_file = "calr_tse.csv"
    r_output_file = "r_output_tse.csv"
    py_output_file = "python_output_tse.csv"
    
    print("="*70)
    print("TSE CONVERSION COMPARISON")
    print("="*70)
    
    # Run Python conversion
    print(f"\n1️⃣  Running Python TSE conversion...")
    try:
        py_df = load_cal_file(tse_file)
        py_df.to_csv(py_output_file, index=False)
        print(f"   ✓ Python conversion complete: {py_df.shape[0]:,} rows × {py_df.shape[1]} columns")
    except Exception as e:
        print(f"   ✗ Python conversion failed: {e}")
        return 1
    
    # Run R conversion
    print(f"\n2️⃣  Running R TSE conversion...")
    if not run_r_conversion(tse_file, r_output_file):
        print("   ✗ R conversion failed")
        return 1
    
    # Load R output
    print(f"\n3️⃣  Loading R output...")
    try:
        r_df = pd.read_csv(r_output_file)
        print(f"   ✓ R output loaded: {r_df.shape[0]:,} rows × {r_df.shape[1]} columns")
    except Exception as e:
        print(f"   ✗ Failed to load R output: {e}")
        return 1
    
    # Compare
    print(f"\n4️⃣  Comparing outputs...")
    results = compare_dataframes(r_df, py_df)
    
    # Print report
    success = print_comparison_report(results)
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
