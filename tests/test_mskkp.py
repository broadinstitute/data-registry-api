from dataregistry.api.mskkp import suggest_column_map


def test_effect_allele_aliases():
    """effect_allele and ref map to effectAllele"""
    result = suggest_column_map(
        ["effect_allele", "ref"],
        ["effectAllele", "nonEffectAllele", "pValue"]
    )
    assert result.get("effect_allele") == "effectAllele", f"Expected effect_allele -> effectAllele, got {result}"
    assert result.get("ref") == "effectAllele", f"Expected ref -> effectAllele, got {result}"


def test_non_effect_allele_aliases():
    """non_effect_allele and alt map to nonEffectAllele"""
    result = suggest_column_map(
        ["non_effect_allele", "alt"],
        ["effectAllele", "nonEffectAllele", "pValue"]
    )
    assert result.get("non_effect_allele") == "nonEffectAllele", f"Expected non_effect_allele -> nonEffectAllele, got {result}"
    assert result.get("alt") == "nonEffectAllele", f"Expected alt -> nonEffectAllele, got {result}"


def test_standard_error_aliases():
    """se, stderr, sebeta, standard_error map to standardError"""
    for alias in ["se", "stderr", "sebeta", "standard_error"]:
        result = suggest_column_map(
            [alias],
            ["standardError", "beta", "pValue"]
        )
        assert result.get(alias) == "standardError", f"Expected {alias} -> standardError, got {result}"


def test_hwe_alias():
    """hwe, hwe_p, p_hwe map to hweP"""
    for alias in ["hwe", "hwe_p", "p_hwe"]:
        result = suggest_column_map([alias], ["hweP", "pValue"])
        assert result.get(alias) == "hweP", f"Expected {alias} -> hweP, got {result}"


def test_imputation_quality_aliases():
    """info, rsq, imputation_quality map to imputationQuality"""
    for alias in ["info", "rsq", "imputation_quality"]:
        result = suggest_column_map([alias], ["imputationQuality", "pValue"])
        assert result.get(alias) == "imputationQuality", f"Expected {alias} -> imputationQuality, got {result}"


def test_is_imputed_aliases():
    """imputed, is_imputed map to isImputed"""
    for alias in ["imputed", "is_imputed"]:
        result = suggest_column_map([alias], ["isImputed", "pValue"])
        assert result.get(alias) == "isImputed", f"Expected {alias} -> isImputed, got {result}"


def test_existing_aliases_still_work():
    """Chromosome, position, p-value aliases should still work"""
    result = suggest_column_map(
        ["chr", "bp", "pval"],
        ["chromosome", "position", "pValue"]
    )
    assert result["chr"] == "chromosome"
    assert result["bp"] == "position"
    assert result["pval"] == "pValue"
