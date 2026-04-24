from __future__ import annotations

import pandas as pd

from .config_reader import LoadedConfig
from .intelligence_utils import (
    achievement_to_rag,
    find_columns_by_any_fragments,
    project_config_dataframe,
    safe_divide,
    to_numeric_series,
)


def calculate_fte(master_df: pd.DataFrame, loaded_config: LoadedConfig, working_days_elapsed: int) -> pd.DataFrame:
    print("[FTE] Calculating FTE achievement")
    project_config = project_config_dataframe(loaded_config)[["project_name", "fte_requested"]]

    hour_columns = find_columns_by_any_fragments(master_df, ["hours", "hrs"], preferred_prefix="onduty__")
    if not hour_columns:
        raise ValueError("No OnDuty hour columns found for FTE calculation.")

    working = master_df.copy()
    working["non_billable"] = working.get("non_billable", False).fillna(False).astype(bool)
    working = working.loc[~working["non_billable"]].copy()

    if working.empty:
        grouped = pd.DataFrame({"project_name": project_config["project_name"], "actual_hours": 0.0})
    else:
        numeric_hours = working[hour_columns].apply(to_numeric_series)
        working["actual_hours"] = numeric_hours.fillna(0).sum(axis=1)
        grouped = working.groupby("project_name", dropna=False, as_index=False)["actual_hours"].sum()

    result = project_config.merge(grouped, on="project_name", how="left")
    result["actual_hours"] = result["actual_hours"].fillna(0.0)
    result["target_hours"] = result["fte_requested"].fillna(0.0) * 8.0 * float(working_days_elapsed)
    result["achievement_pct"] = [
        safe_divide(actual, target) * 100 if safe_divide(actual, target) is not None else None
        for actual, target in zip(result["actual_hours"], result["target_hours"])
    ]
    result["surplus"] = result["actual_hours"] - result["target_hours"]
    result["rag_status"] = result["achievement_pct"].apply(achievement_to_rag)

    result = result[
        ["project_name", "actual_hours", "target_hours", "achievement_pct", "surplus", "rag_status"]
    ].sort_values("project_name", ignore_index=True)

    print(f"[FTE] Completed for {len(result)} projects")
    return result
