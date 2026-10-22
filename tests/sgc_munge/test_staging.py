from sgc_munge import staging
from sgc_munge.staging import Reading, StagedRow, Staging

REPORTED = ["1", "120", "G", "A", "0.1", "0.05", "0.01", "0.3", "0.9"]


def test_encode_decode_round_trip_without_alternate():
    row = StagedRow(False, Reading(True, 120, "A", "G", False), None, REPORTED)
    line = staging.encode(row)
    assert line.count("\t") == 19 and "\n" not in line
    assert staging.decode(line + "\n") == row


def test_encode_decode_round_trip_with_alternate():
    row = StagedRow(True, Reading(False, 161, "CA", "C", False),
                    Reading(True, 160, "C", "CA", True), REPORTED)
    assert staging.decode(staging.encode(row)) == row


def test_staging_groups_by_contig_in_canonical_order(tmp_path):
    r = StagedRow(False, Reading(False, 1, "A", "G", False), None, REPORTED)
    s = Staging(str(tmp_path / "stage"))
    for chrom, pos in [("X", 5), ("2", 7), ("1", 9), ("2", 3), ("1", 1)]:
        s.write(chrom, r._replace(primary=r.primary._replace(pos=pos)))
    s.close()
    assert s.contigs() == ["1", "2", "X"]
    assert [row.primary.pos for row in s.rows("1")] == [9, 1]      # write order kept
    assert [row.primary.pos for row in s.rows("2")] == [7, 3]
    assert list(s.rows("7")) == []
    s.close()                                                        # idempotent
