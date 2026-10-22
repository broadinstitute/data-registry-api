import math, pytest
from sgc_munge.rows import ColumnIndex, parse_row, RowError, normalize_chrom
from tests.sgc_munge.fixtures import HDR, MAP

def test_from_header_maps_indices_and_optionals():
    idx = ColumnIndex.from_header(HDR, MAP)
    assert (idx.chrom, idx.pos, idx.oa, idx.ea, idx.eaf, idx.info, idx.beta, idx.se, idx.pvalue) == (0,1,2,3,4,5,7,8,9)
    idx2 = ColumnIndex.from_header(HDR, {k:v for k,v in MAP.items() if k != "col_imputation_quality"})
    assert idx2.info is None

def test_from_header_missing_required_key():
    with pytest.raises(ValueError, match="column_mapping missing required keys: \\['col_se'\\]"):
        ColumnIndex.from_header(HDR, {k:v for k,v in MAP.items() if k != "col_se"})

def test_from_header_missing_column_in_file():
    with pytest.raises(ValueError, match="file missing required columns: \\['P'\\]"):
        ColumnIndex.from_header(HDR[:-1], MAP)

@pytest.mark.parametrize("raw,exp", [("chr1","1"),("22","22"),("23","X"),("chrY","Y"),("MT",None),("chrM",None),("Un",None)])
def test_normalize_chrom(raw, exp):
    assert normalize_chrom(raw) == exp

def test_parse_row_happy_as_reported():
    idx = ColumnIndex.from_header(HDR, MAP)
    r = parse_row(["chr1","12345","A","g","0.25","0.9","1000","0.1","0.02","1e-5"], idx)
    assert (r.chrom, r.pos, r.oa, r.ea, r.beta, r.se, r.pvalue, r.eaf, r.info) == ("1",12345,"A","G",0.1,0.02,1e-5,0.25,0.9)

def test_parse_row_sci_position():
    idx = ColumnIndex.from_header(HDR, MAP)
    assert parse_row(["1","1e+05","A","G","0.2","0.9","1","0.1","0.02","0.5"], idx).pos == 100000

def test_parse_row_zero_p_is_invalid():
    idx = ColumnIndex.from_header(HDR, MAP)
    with pytest.raises(RowError) as e:
        parse_row(["1","100","A","G","0.2","0.9","1","0.1","0.02","0"], idx)
    assert e.value.kind == "invalid"

@pytest.mark.parametrize("fields,kind", [
    (["MT","100","A","G","0.2","0.9","1","0.1","0.02","0.5"], "chrom"),
    (["1","100","A","A","0.2","0.9","1","0.1","0.02","0.5"], "invalid"),
    (["1","100","A","N","0.2","0.9","1","0.1","0.02","0.5"], "invalid"),
    (["1","100","A","G","0.2","0.9","1","0.1","0","0.5"], "invalid"),
    (["1","100","A","G","0.2","0.9","1","inf","0.02","0.5"], "invalid"),
    (["1","100","A","G","0.2","0.9","1","0.1","0.02","1.5"], "invalid"),
    (["1","100","A","G"], "invalid"),
])
def test_parse_row_drop_kinds(fields, kind):
    idx = ColumnIndex.from_header(HDR, MAP)
    with pytest.raises(RowError) as e:
        parse_row(fields, idx)
    assert e.value.kind == kind

def test_parse_row_missing_optionals_are_nan():
    idx = ColumnIndex.from_header(HDR, MAP)
    r = parse_row(["1","100","A","G","NA","","1","0.1","0.02","0.5"], idx)
    assert math.isnan(r.eaf) and math.isnan(r.info)
