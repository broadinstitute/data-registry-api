from sgc_metal.script import outfile_prefix, render_script, tbl_path

EXPECTED = """SCHEME STDERR
AVERAGEFREQ ON
MINMAXFREQ ON
CUSTOMVARIABLE Ncases
CUSTOMVARIABLE Ncontrols
MARKER SNP_ID
ALLELE Effect_allele Non-effect_allele
EFFECT Beta
STDERR SE
PVALUE P-value
FREQLABEL Effect_AF
LABEL Ncases AS N_CAS
LABEL Ncontrols AS N_CON
PROCESS work/A_f1.tsv.gz
PROCESS work/B_f2.tsv.gz
OUTFILE PSOR_Combined_All_ .tbl
ANALYZE HETEROGENEITY
QUIT
"""


def test_render_script_byte_exact():
    assert render_script(["work/A_f1.tsv.gz", "work/B_f2.tsv.gz"], outfile_prefix("PSOR", "Combined", "All")) == EXPECTED


def test_tbl_path():
    assert tbl_path("/w", "PSOR_Combined_All_") == "/w/PSOR_Combined_All_1.tbl"


def test_render_script_without_heterogeneity_byte_exact():
    # the frequency pass: plain ANALYZE (one read pass instead of two)
    expected = EXPECTED.replace("ANALYZE HETEROGENEITY\n", "ANALYZE\n")
    assert expected != EXPECTED
    assert render_script(["work/A_f1.tsv.gz", "work/B_f2.tsv.gz"], outfile_prefix("PSOR", "Combined", "All"),
                         heterogeneity=False) == expected
    assert render_script(["work/A_f1.tsv.gz", "work/B_f2.tsv.gz"], outfile_prefix("PSOR", "Combined", "All"),
                         heterogeneity=True) == EXPECTED


def test_outfile_prefix_sanitises_dirty_inputs():
    # a space would split METAL's OUTFILE arguments; a slash would point into a directory
    assert outfile_prefix("T2D adj/BMI", "EUR (UKB)", "All") == "T2D_adj_BMI_EUR__UKB__All_"
    assert outfile_prefix("PSOR.v2", "Combined", "Fe-male") == "PSOR.v2_Combined_Fe-male_"
