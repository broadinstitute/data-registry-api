"""Run the PEGASUS toolkit's validators on an uploaded PEG submission.

The toolkit works on files on disk and detects file type by name prefix, so
the uploads are written to a temp directory under fixed names, validated, and
the resulting UI report is returned with the original filenames restored.
"""
import tempfile
from pathlib import Path
from typing import Callable

from pegasus.main import (
    create_ui_response,
    cross_validate_list_matrix,
    validate_list,
    validate_matrix,
    validate_metadata,
)

CROSS_VALIDATION_SKIPPED = {
    "step": "Cross-validation skipped",
    "type": "warning",
    "message": "Skipped because one or more files failed validation.",
}


def _run(label: str, fn: Callable[[], list]) -> list:
    """Run one validator; a crash becomes a reported error, never a 500."""
    try:
        return fn()
    except Exception as exc:  # noqa: BLE001 - any toolkit failure is a user-facing error
        return [{"step": f"{label} - Parse", "type": "error", "message": str(exc)}]


def _has_errors(results: list) -> bool:
    return any(r.get("type") == "error" for r in results)


def validate_peg_files(
    list_bytes: bytes, list_name: str,
    matrix_bytes: bytes, matrix_name: str,
    metadata_bytes: bytes, metadata_name: str,
    error_limit: int = 50,
) -> dict:
    with tempfile.TemporaryDirectory(prefix="peg-validate-") as tmp:
        root = Path(tmp)
        list_path = root / "list_upload.tsv"
        matrix_path = root / "matrix_upload.tsv"
        metadata_path = root / "metadata_upload.xlsx"
        list_path.write_bytes(list_bytes)
        matrix_path.write_bytes(matrix_bytes)
        metadata_path.write_bytes(metadata_bytes)

        results = {
            "list": _run("List", lambda: validate_list(list_path, error_limit=error_limit)),
            "matrix": _run("Matrix", lambda: validate_matrix(matrix_path)),
            "metadata": _run("Metadata", lambda: validate_metadata(metadata_path, error_limit=error_limit)),
        }
        if any(_has_errors(r) for r in results.values()):
            results["cross_validation"] = [dict(CROSS_VALIDATION_SKIPPED)]
        else:
            results["cross_validation"] = _run(
                "Cross-validation",
                lambda: cross_validate_list_matrix(list_path, matrix_path, metadata_path),
            )

        report = create_ui_response(results, {
            "list": list_path, "matrix": matrix_path, "metadata": metadata_path, "cross_validation": None,
        })

    original_names = {"list": list_name, "matrix": matrix_name, "metadata": metadata_name}
    for key, info in report["files"].items():
        info["path"] = original_names.get(key)
    return report
