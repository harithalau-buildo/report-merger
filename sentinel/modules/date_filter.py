from __future__ import annotations

from datetime import datetime, date
from pathlib import Path

import pandas as pd


DATE_FORMAT = "%m-%d-%Y"
DATE_FORMAT_CANDIDATES = (DATE_FORMAT, "%Y-%m-%d")
DATE_COLUMN_CANDIDATES = [
    "date",
    "work date",
    "task date",
    "on duty date",
    "sample date",
    "sampled date",
    "created date",
    "day",
]

CSV_ENCODINGS = ("utf-8-sig", "utf-8", "cp1252", "latin1")


def read_tabular_file(path: Path, **kwargs) -> pd.DataFrame:
    if path.suffix.lower() != ".csv":
        return pd.read_excel(path, **kwargs)

    last_error: UnicodeDecodeError | None = None
    for encoding in CSV_ENCODINGS:
        try:
            return pd.read_csv(path, encoding=encoding, **kwargs)
        except UnicodeDecodeError as exc:
            last_error = exc

    if last_error is not None:
        raise last_error
    return pd.read_csv(path, **kwargs)


def read_dataset_file(path: Path, dataset_name: str, header_row: int | None = None, **kwargs) -> pd.DataFrame:
    if header_row is None:
        header_row = 2 if dataset_name == "onduty" else 0
    return read_tabular_file(path, header=header_row, **kwargs)


def _find_date_column(dataframe: pd.DataFrame, dataset_name: str) -> str:
    lowered = {str(column).strip().lower(): column for column in dataframe.columns}
    for candidate in DATE_COLUMN_CANDIDATES:
        if candidate in lowered:
            return lowered[candidate]

    for column in dataframe.columns:
        column_name = str(column).strip().lower()
        if "date" in column_name or column_name.endswith("day"):
            return column

    raise ValueError(
        f"Unable to locate a date column for dataset '{dataset_name}'. "
        "Expected a column containing 'date' or one of the common date column names."
    )


def _parse_cell_to_date(value: object, dataset_name: str, column_name: str) -> date | None:
    if pd.isna(value):
        return None

    if isinstance(value, pd.Timestamp):
        return value.date()

    if hasattr(value, "date") and not isinstance(value, str):
        try:
            return value.date()
        except Exception:
            pass

    value_str = str(value).strip()
    if not value_str:
        return None

    if not any(character.isdigit() for character in value_str):
        return None

    for date_format in DATE_FORMAT_CANDIDATES:
        try:
            return datetime.strptime(value_str, date_format).date()
        except ValueError:
            continue

    raise ValueError(
        f"Date format wrong in dataset '{dataset_name}', column '{column_name}': "
        f"'{value_str}'. Expected one of: MM-DD-YYYY, YYYY-MM-DD."
    )


def filter_dataframe_by_date(
    dataframe: pd.DataFrame,
    dataset_name: str,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    before_count = len(dataframe)
    print(f"[DATE] {dataset_name}: before date filter = {before_count}")

    if dataframe.empty:
        print(f"[DATE] {dataset_name}: dataframe is empty before filtering")
        return dataframe.copy()

    date_column = _find_date_column(dataframe, dataset_name)
    parsed_dates = dataframe[date_column].apply(
        lambda value: _parse_cell_to_date(value, dataset_name, str(date_column))
    )

    filtered = dataframe.loc[parsed_dates.between(start_date, end_date, inclusive="both").fillna(False)].copy()
    after_count = len(filtered)
    print(f"[DATE] {dataset_name}: after date filter = {after_count}")
    if after_count == 0:
        print(f"[DATE] Warning: no rows remain after date filter for {dataset_name}. Continuing.")
    return filtered
