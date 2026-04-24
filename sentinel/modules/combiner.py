from __future__ import annotations

import pandas as pd

from .account_filter import _resolve_email_column


PROJECT_COLUMN_CANDIDATES = ["project_name", "project name"]


def _find_project_column(dataframe: pd.DataFrame) -> str:
    lowered = {str(column).replace("\ufeff", "").strip().lower(): column for column in dataframe.columns}
    for candidate in PROJECT_COLUMN_CANDIDATES:
        if candidate in lowered:
            return lowered[candidate]
    raise ValueError(
        "Unable to locate project name column during combine step. "
        "Expected one of: project_name, project name."
    )


def _normalize_for_merge(dataframe: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
    working = dataframe.copy()
    email_column = _resolve_email_column(working, dataset_name)
    project_column = _find_project_column(working)

    working["email"] = working[email_column].fillna("").astype(str).str.strip().str.lower()
    working["project_name"] = working[project_column].fillna("").astype(str).str.strip()
    working = working.rename(
        columns={
            column: column if column in {"email", "project_name"} else f"{dataset_name}__{column}"
            for column in working.columns
        }
    )
    return working


def _first_non_empty(series: pd.Series) -> object:
    for value in series:
        if pd.isna(value):
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return series.iloc[0] if not series.empty else None


def _collapse_duplicate_keys(dataframe: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe

    key_columns = ["email", "project_name"]
    duplicate_count = len(dataframe) - len(dataframe[key_columns].drop_duplicates())
    if duplicate_count <= 0:
        return dataframe

    print(f"[COMBINE] {dataset_name}: collapsing {duplicate_count} duplicate rows on email + project_name before merge")
    value_columns = [column for column in dataframe.columns if column not in key_columns]
    aggregated = (
        dataframe.groupby(key_columns, dropna=False, sort=False)[value_columns]
        .agg(_first_non_empty)
        .reset_index()
    )
    print(f"[COMBINE] {dataset_name}: rows after collapse = {len(aggregated)}")
    return aggregated


def combine_dataframes(filtered_frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    print("[COMBINE] Combining filtered dataframes into master dataframe")
    ordered_names = ["onduty", "productivity", "ur", "accuracy"]

    normalized_frames = [
        _collapse_duplicate_keys(_normalize_for_merge(filtered_frames[name], name), name)
        for name in ordered_names
    ]

    master = normalized_frames[0]
    for frame in normalized_frames[1:]:
        master = master.merge(frame, how="outer", on=["email", "project_name"])

    print(f"[COMBINE] Master dataframe shape = {master.shape}")
    return master
