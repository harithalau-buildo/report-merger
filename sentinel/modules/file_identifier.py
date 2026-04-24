from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .date_filter import read_tabular_file


ONDUTY_STATUS_HEADER = "\u72b6\u6001/Status"
ONDUTY_STATUS_VALUE = "Moderation Task"

FILE_TYPE_KEYWORDS = {
    "productivity": "Prod(Case/h)",
    "accuracy": "Sampled",
}

SUPPORTED_EXTENSIONS = {".xlsx", ".csv"}


@dataclass(frozen=True)
class IdentifiedFile:
    file_type: str
    path: Path
    modified_time: float
    header_row: int
    headers: list[str]


def _normalize_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def _safe_preview_text(value: object) -> str:
    text = _normalize_text(value)
    return text.encode("ascii", "backslashreplace").decode("ascii")


def _read_headers(path: Path, header_row: int) -> list[str]:
    frame = read_tabular_file(path, header=header_row, nrows=0)
    return [str(column).strip() for column in frame.columns]


def _print_first_rows(path: Path) -> None:
    preview = read_tabular_file(path, header=None, nrows=3)
    print(f"[FILES] Preview {path.name}:")
    for row_index in range(3):
        if row_index < len(preview.index):
            row_values = [_safe_preview_text(value) for value in preview.iloc[row_index].tolist()]
        else:
            row_values = []
        print(f"[FILES]   row {row_index}: {row_values}")


def _contains_header_keyword(headers: list[str], keyword: str) -> bool:
    return any(keyword.lower() in header.lower() for header in headers)


def _is_ur_file(headers: list[str]) -> bool:
    lowered = {str(header).strip().lower() for header in headers}
    return "ur" in lowered and "occupancy" in lowered


def _is_onduty_file(path: Path, header_row: int, headers: list[str]) -> bool:
    if header_row != 2:
        return False

    status_column = next((column for column in headers if str(column).strip() == ONDUTY_STATUS_HEADER), None)
    if status_column is None:
        return False

    frame = read_tabular_file(path, header=header_row)
    return frame[status_column].fillna("").astype(str).str.contains(ONDUTY_STATUS_VALUE, case=False).any()


def identify_input_files(input_folder: Path) -> dict[str, IdentifiedFile]:
    print(f"[FILES] Scanning input folder: {input_folder}")
    if not input_folder.exists():
        raise FileNotFoundError(f"Input folder missing. Expected path: {input_folder}")

    candidates: dict[str, list[IdentifiedFile]] = {key: [] for key in ["onduty", "ur", *FILE_TYPE_KEYWORDS]}

    for path in input_folder.iterdir():
        if not path.is_file() or path.suffix.lower() not in SUPPORTED_EXTENSIONS:
            continue

        try:
            _print_first_rows(path)
        except Exception as exc:
            print(f"[FILES] Skipping unreadable preview for {path.name}: {exc}")
            continue

        for header_row in (0, 2):
            try:
                headers = _read_headers(path, header_row)
            except Exception as exc:
                print(f"[FILES] Skipping header row {header_row} for {path.name}: {exc}")
                continue

            print(f"[FILES] Inspected {path.name} with header row {header_row} and {len(headers)} columns")

            if _is_onduty_file(path, header_row, headers):
                candidates["onduty"].append(
                    IdentifiedFile(
                        file_type="onduty",
                        path=path,
                        modified_time=path.stat().st_mtime,
                        header_row=header_row,
                        headers=headers,
                    )
                )

            if _is_ur_file(headers):
                candidates["ur"].append(
                    IdentifiedFile(
                        file_type="ur",
                        path=path,
                        modified_time=path.stat().st_mtime,
                        header_row=header_row,
                        headers=headers,
                    )
                )

            for file_type, keyword in FILE_TYPE_KEYWORDS.items():
                if not _contains_header_keyword(headers, keyword):
                    continue

                candidates[file_type].append(
                    IdentifiedFile(
                        file_type=file_type,
                        path=path,
                        modified_time=path.stat().st_mtime,
                        header_row=header_row,
                        headers=headers,
                    )
                )

    selected: dict[str, IdentifiedFile] = {}
    for file_type, matches in candidates.items():
        if not matches:
            raise FileNotFoundError(
                f"Input file type not detected: {file_type}. "
                + (
                    "Expected a file in "
                    f"{input_folder} with header row 2 containing column '{ONDUTY_STATUS_HEADER}' "
                    f"and value '{ONDUTY_STATUS_VALUE}'."
                    if file_type == "onduty"
                    else f"Expected a file in {input_folder} containing header keyword '{FILE_TYPE_KEYWORDS[file_type]}'."
                )
            )

        newest = sorted(matches, key=lambda item: item.modified_time, reverse=True)[0]
        selected[file_type] = newest
        print(f"[FILES] Identified {file_type}: {newest.path.name} (header row {newest.header_row})")

    return selected
