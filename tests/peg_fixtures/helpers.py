"""Loaders for the PEG toy submission and generators for broken variants.

Each function returns {"list": (filename, bytes), "matrix": (...), "metadata": (...)}.
Broken variants are derived at test time so only the valid set is committed.
"""
import io
from pathlib import Path

import openpyxl

FIXTURES = Path(__file__).parent
LIST_NAME = "list_toydata_PEGSt000000.tsv"
MATRIX_NAME = "matrix_toydata_PEGSt000000.tsv"
METADATA_NAME = "metadata_toydata_PEGSt000000.xlsx"


def valid_files() -> dict:
    return {
        "list": (LIST_NAME, (FIXTURES / LIST_NAME).read_bytes()),
        "matrix": (MATRIX_NAME, (FIXTURES / MATRIX_NAME).read_bytes()),
        "metadata": (METADATA_NAME, (FIXTURES / METADATA_NAME).read_bytes()),
    }


def broken_list_header(files: dict) -> dict:
    name, data = files["list"]
    header, _, rest = data.partition(b"\n")
    header = header.replace(b"\tGeneSymbol", b"\tGeneSym")
    return {**files, "list": (name, header + b"\n" + rest)}


def broken_list_row_mismatch(files: dict) -> dict:
    """Change the first data row's PrimaryVariantID so it no longer matches
    any matrix row for that gene, triggering a cross-validation mismatch."""
    name, data = files["list"]
    lines = data.split(b"\n")
    fields = lines[1].split(b"\t")
    fields[0] = b"chr2:20000:A:G"
    lines[1] = b"\t".join(fields)
    return {**files, "list": (name, b"\n".join(lines))}


def broken_matrix_row(files: dict) -> dict:
    name, data = files["matrix"]
    lines = data.split(b"\n")
    lines[1] = b"BAD" + lines[1]
    return {**files, "matrix": (name, b"\n".join(lines))}


def metadata_missing_sheet(files: dict) -> dict:
    name, data = files["metadata"]
    wb = openpyxl.load_workbook(io.BytesIO(data))
    del wb[wb.sheetnames[-1]]  # "Method"
    out = io.BytesIO()
    wb.save(out)
    return {**files, "metadata": (name, out.getvalue())}


def last_sheet_name() -> str:
    """Name of the sheet metadata_missing_sheet() removes (the workbook's last sheet)."""
    _, data = valid_files()["metadata"]
    return openpyxl.load_workbook(io.BytesIO(data)).sheetnames[-1]


def garbage_metadata(files: dict) -> dict:
    name, _ = files["metadata"]
    return {**files, "metadata": (name, b"not an xlsx")}


def as_multipart(files: dict) -> dict:
    """Shape for TestClient(files=...)."""
    ct = {
        "list": "text/tab-separated-values",
        "matrix": "text/tab-separated-values",
        "metadata": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    }
    return {f"peg_{key}": (name, data, ct[key]) for key, (name, data) in files.items()}
