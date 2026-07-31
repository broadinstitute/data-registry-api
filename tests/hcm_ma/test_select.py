import json
import uuid
from sqlalchemy import text
from tests.conftest import db
from hcm_ma import select as sel


def _mk_file(c, *, cohort, ancestry="EUR", sex="ALL", sarc="ALL",
             build="GRCh38", cm=None, cases=10, controls=20):
    fid = uuid.uuid4().hex
    cm = cm or {"chromosome": "CHR", "position": "BP", "effect_allele": "A1",
                "non_effect_allele": "A0", "beta": "BETA", "standard_error": "SE",
                "p_value": "P", "effect_allele_frequency": "AF",
                "imputation_quality": "INFO", "sample_size": "N"}
    c.execute(text("""
        INSERT INTO hcm_gwas_files
          (id, cohort_name, sarc, ancestry, sex, genome_build, software, analyst,
           file_name, file_size, s3_path, uploaded_by, column_mapping, cases, controls)
        VALUES (:id,:cohort,:sarc,:anc,:sex,:build,'REGENIE','a',
           :fname, 1, :s3, 'u', :cm, :cases, :controls)
    """), {"id": fid, "cohort": cohort, "sarc": sarc, "anc": ancestry, "sex": sex,
           "build": build, "fname": f"{cohort}.gz", "s3": f"hcm/gwas/{cohort}.gz",
           "cm": json.dumps(cm), "cases": cases, "controls": controls})
    return fid


def test_colmap_adapter_full():
    cm = {"chromosome": "CHR", "position": "BP", "effect_allele": "A1",
          "non_effect_allele": "A0", "beta": "BETA", "standard_error": "SE",
          "p_value": "P", "effect_allele_frequency": "AF",
          "imputation_quality": "INFO", "sample_size": "N"}
    assert sel.hcm_colmap_to_ma(cm) == {
        "col_chromosome": "CHR", "col_position": "BP", "col_effect_allele": "A1",
        "col_non_effect_allele": "A0", "col_beta": "BETA", "col_se": "SE",
        "col_pvalue": "P", "col_effect_allele_freq": "AF",
        "col_imputation_quality": "INFO", "col_variant_n": "N"}


def test_colmap_adapter_partial_only_maps_present_keys():
    assert sel.hcm_colmap_to_ma({"chromosome": "CHR", "beta": "BETA"}) == {
        "col_chromosome": "CHR", "col_beta": "BETA"}


def test_list_eligible_flags_grch37_ineligible(api_client):
    with db.get_engine().connect() as c:
        _mk_file(c, cohort="MGB", build="GRCh38")
        _mk_file(c, cohort="HUNT", build="GRCh37")
        c.commit()
    rows = {r["cohort_name"]: r for r in sel.list_eligible_files(db.get_engine())}
    assert rows["MGB"]["eligible"] is True and rows["MGB"]["genome_build"] == "GRCh38"
    assert rows["HUNT"]["eligible"] is False and rows["HUNT"]["genome_build"] == "GRCh37"


def test_select_files_by_ids_returns_worker_rows(api_client):
    with db.get_engine().connect() as c:
        fid = _mk_file(c, cohort="MGB")
        c.commit()
    rows = sel.select_files_by_ids(db.get_engine(), [fid])
    assert len(rows) == 1
    r = rows[0]
    assert r["cohort"] == "MGB" and r["dataset"] == "MGB.gz"
    assert r["s3_path"] == "hcm/gwas/MGB.gz"
    assert isinstance(r["column_mapping"], dict) and r["cases"] == 10
