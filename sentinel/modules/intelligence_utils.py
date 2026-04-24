from __future__ import annotations

from typing import Any

import pandas as pd

from .config_reader import LoadedConfig


def project_config_dataframe(loaded_config: LoadedConfig) -> pd.DataFrame:
    rows = []
    for project in loaded_config.projects:
        rows.append(
            {
                "project_name": project.project_name,
                "fte_requested": coerce_number(project.fte_requested),
                "billing_rate": coerce_number(project.billing_rate),
                "accuracy_percent": coerce_number(project.targets.get("accuracy_percent")),
                "productivity_per_labeller_per_day": coerce_number(
                    project.targets.get("productivity_per_labeller_per_day")
                ),
                "ur_percent": coerce_number(project.targets.get("ur_percent")),
            }
        )
    return pd.DataFrame(rows)


def coerce_number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, str):
        cleaned = value.strip().replace(",", "").replace("%", "")
        if not cleaned:
            return None
        value = cleaned
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return None
    return float(numeric)


def find_column_by_fragments(
    dataframe: pd.DataFrame,
    required_fragments: list[str],
    preferred_prefix: str | None = None,
    forbidden_prefixes: list[str] | None = None,
) -> str:
    forbidden_prefixes = forbidden_prefixes or []
    preferred_matches: list[str] = []
    fallback_matches: list[str] = []

    for column in dataframe.columns:
        column_name = str(column).strip().lower()
        if any(column_name.startswith(prefix.lower()) for prefix in forbidden_prefixes):
            continue
        if not all(fragment.lower() in column_name for fragment in required_fragments):
            continue
        if preferred_prefix and column_name.startswith(preferred_prefix.lower()):
            preferred_matches.append(column)
        else:
            fallback_matches.append(column)

    matches = preferred_matches or fallback_matches
    if not matches:
        raise ValueError(f"Required column not found with fragments: {required_fragments}")
    return matches[0]


def find_columns_by_any_fragments(
    dataframe: pd.DataFrame,
    fragments: list[str],
    preferred_prefix: str | None = None,
) -> list[str]:
    preferred_matches: list[str] = []
    fallback_matches: list[str] = []

    for column in dataframe.columns:
        column_name = str(column).strip().lower()
        if not any(fragment.lower() in column_name for fragment in fragments):
            continue
        if preferred_prefix and column_name.startswith(preferred_prefix.lower()):
            preferred_matches.append(column)
        else:
            fallback_matches.append(column)

    return preferred_matches or fallback_matches


def to_numeric_series(series: pd.Series) -> pd.Series:
    if series.empty:
        return pd.Series(dtype="float64")
    cleaned = series.astype(str).str.replace(",", "", regex=False).str.replace("%", "", regex=False).str.strip()
    cleaned = cleaned.replace({"": None, "nan": None, "None": None})
    return pd.to_numeric(cleaned, errors="coerce")


def normalize_percentage_series(series: pd.Series) -> pd.Series:
    numeric = to_numeric_series(series)
    if numeric.dropna().empty:
        return numeric
    non_null = numeric.dropna()
    if ((non_null >= 0) & (non_null <= 1)).all():
        return numeric * 100.0
    return numeric


def safe_divide(numerator: float | int | None, denominator: float | int | None) -> float | None:
    if numerator is None or denominator in (None, 0):
        return None
    return float(numerator) / float(denominator)


def achievement_to_rag(achievement_pct: float | None) -> str:
    if achievement_pct is None or pd.isna(achievement_pct):
        return "RED"
    if achievement_pct >= 90:
        return "GREEN"
    if achievement_pct >= 75:
        return "AMBER"
    return "RED"
