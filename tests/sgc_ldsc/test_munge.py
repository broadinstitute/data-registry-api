from sgc_ldsc.munge import sgc_var_id, p_to_z, effective_n, munge_records

SNPMAP = {"1:100:A:G": "rs1", "2:200:C:T": "rs2"}          # var_id -> rs (REF:ALT order)
SNPMAP_FLIP = {"1:100:G:A": "rs1f"}                         # allele-order-swapped


def test_sgc_var_id_uses_other_then_effect_allele():
    # SGC: effect allele (EA) is ALT, other allele (OA) is REF -> var_id = chr:pos:OA:EA
    assert sgc_var_id("1", "100", oa="a", ea="g") == "1:100:A:G"


def test_p_to_z_sign_follows_beta():
    assert p_to_z(0.05, 0.3) > 0
    assert p_to_z(0.05, -0.3) < 0
    assert abs(p_to_z(1.0, 0.1)) < 1e-9  # chi2.isf(1,1)=0


def test_effective_n_case_control_harmonic():
    # 4 / (1/1000 + 1/3000) = 3000
    assert abs(effective_n(1000.0, 3000.0) - 3000.0) < 1e-6


def test_munge_records_maps_flips_and_signs():
    col_map = {"chrom": "CHR", "pos": "BP", "ea": "EA", "oa": "OA",
               "p": "P", "beta": "BETA", "ncase": "N_case", "ncontrol": "N_control"}
    rows = [
        {"CHR": "1", "BP": "100", "OA": "A", "EA": "G", "P": "0.01", "BETA": "0.5",
         "N_case": "1000", "N_control": "3000"},           # standard hit -> rs1
        {"CHR": "1", "BP": "100", "OA": "G", "EA": "A", "P": "0.01", "BETA": "0.5",
         "N_case": "1000", "N_control": "3000"},           # flipped hit -> rs1f, beta sign reversed
        {"CHR": "9", "BP": "999", "OA": "A", "EA": "T", "P": "0.01", "BETA": "0.5",
         "N_case": "1", "N_control": "1"},                 # not in snpmap -> dropped
    ]
    out = munge_records(rows, col_map, SNPMAP, SNPMAP_FLIP)
    by_rs = {r[0]: r for r in out}
    assert set(by_rs) == {"rs1", "rs1f"}
    assert by_rs["rs1"][1] > 0      # beta +0.5 -> +Z
    assert by_rs["rs1f"][1] < 0     # flipped -> sign reversed
    assert abs(by_rs["rs1"][2] - 3000.0) < 1e-6   # effective N
