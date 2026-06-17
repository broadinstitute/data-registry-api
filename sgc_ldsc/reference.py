"""Load LDSC reference inputs from a local cache dir (the Batch worker downloads
them from s3://dig-ldsc-server/bin/). Ported from dig-ldsc-methods inputs.py +
weights.py, reduced to the univariate needs: baseline LD column 0, base SNP
count, regression-weight LD scores, snpmap, and the reference SNP order.
"""
import gzip
import zipfile
from typing import Dict

import numpy as np

# Per Phase 0: snpmap filenames use the GRCh38/GRCh37 strings verbatim (the same
# values SGC stores in genome_build). The map is identity; it exists only as the
# single guard point for an unexpected build string (KeyError = fail fast).
BUILD_TOKEN: Dict[str, str] = {"GRCh38": "GRCh38", "GRCh37": "GRCh37"}


def _input_zip(data_path: str, ancestry: str) -> str:
    return f"{data_path}/inputs/sldsc_inputs.{ancestry}.zip"


def load_baseline_ld_col0(data_path: str, ancestry: str) -> np.ndarray:
    """Genome-wide (base annotation) LD score = column 0 of baseline_ld, shape (M,1)."""
    with zipfile.ZipFile(_input_zip(data_path, ancestry)) as z:
        with z.open(f"baseline/baseline_ld.{ancestry}.npy") as f:
            return np.load(f)[:, 0:1]


def load_baseline_m(data_path: str, ancestry: str) -> np.ndarray:
    """Number of SNPs in the base annotation, shape (1,1)."""
    with zipfile.ZipFile(_input_zip(data_path, ancestry)) as z:
        with z.open(f"baseline/baseline_parameter_snps.{ancestry}.npy") as f:
            return np.load(f)[0:1]


def load_input_weights(data_path: str, ancestry: str) -> np.ndarray:
    """Regression-weight LD scores (column index 3 of the weights files), shape (M,1)."""
    out = []
    for chrom in range(1, 23):
        with gzip.open(f"{data_path}/weights/{ancestry}/weights.{chrom}.l2.ldscore.gz", "rt") as f:
            f.readline()  # header
            for line in f:
                parts = line.strip().split("\t")
                if len(parts) > 1:
                    out.append(float(parts[3]))
    return np.array([out]).T


def load_snpmap(data_path: str, ancestry: str, genome_build: str, build_type: str) -> Dict[str, str]:
    """var_id -> rs_id map. build_type is 'standard' or 'flipped'."""
    token = BUILD_TOKEN[genome_build]
    path = f"{data_path}/snpmap/sumstats.{build_type}.{token}.{ancestry}.snpmap"
    m = {}
    with open(path) as f:
        for row in f:
            var_id, rs_id = row.strip().split("\t")
            m[var_id] = rs_id
    return m


def ld_rs_order(data_path: str, ancestry: str):
    """Yield rs_ids in the weights' SNP order (defines the regression SNP order)."""
    for chrom in range(1, 23):
        with gzip.open(f"{data_path}/weights/{ancestry}/weights.{chrom}.l2.ldscore.gz", "rt") as f:
            f.readline()
            for line in f:
                parts = line.strip().split("\t")
                if len(parts) > 1:
                    yield parts[1]
