"""Unit tests for the INFO-mapping-required guard on new GWAS uploads."""
import fastapi
import pytest

from dataregistry.api.sgc import _require_imputation_quality_mapping


def test_present_info_mapping_passes():
    _require_imputation_quality_mapping({"col_imputation_quality": "INFO"})  # no raise


def test_missing_key_raises_400():
    with pytest.raises(fastapi.HTTPException) as e:
        _require_imputation_quality_mapping({"col_chromosome": "CHR"})
    assert e.value.status_code == 400


def test_blank_value_raises_400():
    with pytest.raises(fastapi.HTTPException) as e:
        _require_imputation_quality_mapping({"col_imputation_quality": "   "})
    assert e.value.status_code == 400


def test_none_value_raises_400():
    with pytest.raises(fastapi.HTTPException):
        _require_imputation_quality_mapping({"col_imputation_quality": None})


def test_empty_mapping_raises_400():
    with pytest.raises(fastapi.HTTPException):
        _require_imputation_quality_mapping({})
