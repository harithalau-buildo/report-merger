from __future__ import annotations

from pathlib import Path
import unicodedata

import pandas as pd

from .date_filter import read_tabular_file


EMAIL_COLUMN_CANDIDATES_BY_DATASET = {
    "onduty": ["email", "moderator", "name"],
    "productivity": ["moderator", "email", "name"],
    "ur": ["name"],
    "accuracy": ["moderator", "email", "name"],
}

PROJECT_COLUMN_CANDIDATES = [
    "project_name",
    "project name",
    "queue",
    "department",
]


def _normalize_text(value: object) -> str:
    if pd.isna(value):
        return ""
    normalized = unicodedata.normalize("NFKC", str(value))
    normalized = normalized.replace("\ufeff", "").replace("\u200b", "").replace("\u200e", "").replace("\u200f", "")
    return normalized.strip().lower()


def _normalize_column_name(value: object) -> str:
    normalized = unicodedata.normalize("NFKC", str(value))
    normalized = normalized.replace("\ufeff", "").replace("\u200b", "").replace("\u200e", "").replace("\u200f", "")
    return " ".join(normalized.strip().lower().split())


def _sample_values(series: pd.Series, limit: int = 3) -> list[str]:
    samples = []
    for value in series.fillna(""):
        normalized = _normalize_text(value)
        if not normalized:
            continue
        if normalized not in samples:
            samples.append(normalized)
        if len(samples) >= limit:
            break
    return samples


def _print_columns(dataframe: pd.DataFrame, label: str) -> None:
    print(f"[ACCOUNT] {label}: exact columns found = {list(dataframe.columns)}")


def _find_column(dataframe: pd.DataFrame, candidates: list[str], description: str) -> str:
    lowered = {_normalize_column_name(column): column for column in dataframe.columns}
    for candidate in candidates:
        normalized_candidate = _normalize_column_name(candidate)
        if normalized_candidate in lowered:
            return lowered[normalized_candidate]
    raise ValueError(f"Required column not found: {description}. Looked for: {', '.join(candidates)}")


def _find_column_containing(dataframe: pd.DataFrame, fragment: str) -> str | None:
    normalized_fragment = _normalize_column_name(fragment)
    for column in dataframe.columns:
        if normalized_fragment in _normalize_column_name(column):
            return column
    return None


def load_account_list(accounts_folder: Path) -> pd.DataFrame:
    account_path = accounts_folder / "account_list.xlsx"
    print(f"[ACCOUNT] Reading account list: {account_path}")
    if not account_path.exists():
        raise FileNotFoundError(f"Missing account list file: account_list.xlsx. Expected path: {account_path}")

    accounts = read_tabular_file(account_path)
    _print_columns(accounts, "account_list")
    email_column = _find_column_containing(accounts, "email") or _find_column(
        accounts,
        ["moderator", "name"],
        "account email",
    )
    project_column = _find_column(accounts, ["project_name", "project name"], "project_name")
    queue_column = _find_column(accounts, ["queue"], "queue")

    prepared = accounts.copy()
    prepared["account_email"] = prepared[email_column].apply(_normalize_text)
    prepared["project_name"] = prepared[project_column].fillna("").astype(str).str.strip()
    prepared["queue"] = prepared[queue_column].fillna("").astype(str).str.strip()
    prepared = prepared[prepared["account_email"] != ""].copy()
    prepared = prepared[["account_email", "project_name", "queue"]].drop_duplicates(ignore_index=True)

    print(f"[ACCOUNT] account_list: email column = {email_column}")
    print(f"[ACCOUNT] Loaded {prepared['account_email'].nunique()} accounts across {prepared['project_name'].nunique()} projects")
    return prepared


def _resolve_project_column(dataframe: pd.DataFrame) -> str:
    project_column = _find_column_containing(dataframe, "queue")
    if project_column is not None:
        return project_column

    project_column = _find_column_containing(dataframe, "department")
    if project_column is not None:
        return project_column

    return _find_column(dataframe, PROJECT_COLUMN_CANDIDATES, "project name / queue / department")


def _resolve_email_column(dataframe: pd.DataFrame, dataset_name: str) -> str:
    if dataset_name == "onduty":
        email_column = _find_column_containing(dataframe, "email")
        if email_column is not None:
            return email_column

    if dataset_name == "ur":
        return _find_column(dataframe, ["name"], "email column for dataset 'ur'")

    email_column = _find_column_containing(dataframe, "email")
    if email_column is not None:
        return email_column

    return _find_column(
        dataframe,
        EMAIL_COLUMN_CANDIDATES_BY_DATASET[dataset_name],
        f"email column for dataset '{dataset_name}'",
    )


def filter_dataframe_by_accounts(
    dataframe: pd.DataFrame,
    dataset_name: str,
    accounts_df: pd.DataFrame,
) -> pd.DataFrame:
    print(f"[ACCOUNT] {dataset_name}: starting account filter")
    if dataframe.empty:
        print(f"[ACCOUNT] {dataset_name}: no rows to filter")
        return dataframe.copy()

    _print_columns(dataframe, dataset_name)
    email_column_name = _resolve_email_column(dataframe, dataset_name)
    print(f"[ACCOUNT] {dataset_name}: matched email column = {email_column_name}")

    working = dataframe.copy()
    working["_normalized_email"] = working[email_column_name].apply(_normalize_text)
    working = working[working["_normalized_email"] != ""].copy()

    file_samples = _sample_values(working["_normalized_email"])
    account_samples = _sample_values(accounts_df["account_email"])
    print(f"[ACCOUNT] {dataset_name}: sample emails from file = {file_samples}")
    print(f"[ACCOUNT] {dataset_name}: sample emails from account_list = {account_samples}")

    account_emails = accounts_df[["account_email"]].drop_duplicates().copy()
    filtered = working.merge(
        account_emails,
        how="inner",
        left_on="_normalized_email",
        right_on="account_email",
    )
    filtered = filtered.merge(
        accounts_df[["account_email", "project_name", "queue"]].drop_duplicates(),
        how="left",
        on="account_email",
    ).drop(columns=["account_email", "_normalized_email"])

    print(f"[ACCOUNT] {dataset_name}: after account filter = {len(filtered)}")
    if filtered.empty:
        print(f"[ACCOUNT] Warning: no rows remain after account filter for {dataset_name}. Continuing.")
    return filtered
