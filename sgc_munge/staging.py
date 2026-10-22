"""On-disk staging of placed rows, one file per contig.

Pass 1 of the munge places every input row (orientation and indel
normalisation) and parks it here. Pass 2 then walks the contigs one at a time
in canonical order to join rsIDs, so only one contig's worth of data is ever
needed at once.
"""
import os
from typing import Iterator, NamedTuple, Optional

CANONICAL_CONTIGS = [str(i) for i in range(1, 23)] + ["X", "Y"]


class Reading(NamedTuple):
    swapped: bool
    pos: int
    ref: str
    alt: str
    exhausted: bool


class StagedRow(NamedTuple):
    ambiguous: bool                 # indel whose alleles both match the reference
    primary: Reading                # the only reading, or the `prefer` reading of a tie
    alternate: Optional[Reading]    # the other reading; set only for tie-break rows
    reported: list                  # the 9 reported columns


def _flag(value: bool) -> str:
    return "1" if value else "0"


def _reading_fields(reading: Reading) -> list:
    return [_flag(reading.swapped), str(reading.pos), reading.ref, reading.alt,
            _flag(reading.exhausted)]


def _reading_from(fields: list) -> Reading:
    return Reading(fields[0] == "1", int(fields[1]), fields[2], fields[3], fields[4] == "1")


def encode(row: StagedRow) -> str:
    """One line of 20 tab-separated fields, no trailing newline."""
    alternate = _reading_fields(row.alternate) if row.alternate else [""] * 5
    return "\t".join([_flag(row.ambiguous)] + _reading_fields(row.primary) + alternate
                     + list(row.reported))


def decode(line: str) -> StagedRow:
    fields = line.rstrip("\r\n").split("\t")
    alternate = _reading_from(fields[6:11]) if fields[6] else None
    return StagedRow(fields[0] == "1", _reading_from(fields[1:6]), alternate, fields[11:20])


def _contig_order(chrom: str):
    if chrom in CANONICAL_CONTIGS:
        return (0, CANONICAL_CONTIGS.index(chrom), chrom)
    return (1, 0, chrom)


class Staging:
    def __init__(self, stage_dir: str):
        self._dir = stage_dir
        os.makedirs(stage_dir, exist_ok=True)
        self._writers = {}

    def _path(self, chrom: str) -> str:
        return os.path.join(self._dir, f"{chrom}.tsv")

    def write(self, chrom: str, row: StagedRow) -> None:
        handle = self._writers.get(chrom)
        if handle is None:
            handle = self._writers[chrom] = open(self._path(chrom), "w")
        handle.write(encode(row) + "\n")

    def close(self) -> None:
        for handle in self._writers.values():
            handle.close()

    def contigs(self) -> list:
        return sorted(self._writers, key=_contig_order)

    def rows(self, chrom: str) -> Iterator[StagedRow]:
        """Yield a contig's rows in write order. Call only after close()."""
        if chrom not in self._writers:
            return
        with open(self._path(chrom)) as handle:
            for line in handle:
                yield decode(line)
