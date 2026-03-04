"""
CalR - Calorimetry Data Conversion Package

Converts raw calorimetry data from various manufacturers
(Oxymax/CLAMS, TSE, Sable) to standardized CalR format.

This is a Python port of the R CalR conversion logic.
"""

from .loaders import load_cal_file
from .converters import CalRFormat

__version__ = "0.1.0"
__all__ = ["load_cal_file", "CalRFormat"]
