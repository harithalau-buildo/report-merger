from __future__ import annotations

import pandas as pd

from .config_reader import LoadedConfig
from .intelligence_utils import (
    achievement_to_rag,
    find_column_by_fragments,
    project_config_dataframe,
    safe_divide,
    to_numeric_series,
)


def track_productivity(
    master_df: pd.DataFrame,
    loaded_config: LoadedConfig,
    working_days_elapsed: int,
    working_days_remaining: int,
    total_working_days_month: int,
) -> pd.DataFrame:
    print("[PRODUCTIVITY] Tracking productivity")
    project_config = project_config_dataframe(loaded_config)[
        ["project_name", "fte_requested", "productivity_per_labeller_per_day"]
    ]

    submitted_column = find_column_by_fragments(master_df, ["submitted"], preferred_prefix="productivity__")

    working = master_df.copy()
    working["submitted_tasks"] = to_numeric_series(working[submitted_column]).fillna(0.0)
    grouped = working.groupby("project_name", dropna=False, as_index=False)["submitted_tasks"].sum()
    grouped = grouped.rename(columns={"submitted_tasks": "actual_tasks"})

    result = project_config.merge(grouped, on="project_name", how="left")
    result["actual_tasks"] = result["actual_tasks"].fillna(0.0)
    result["target_to_date"] = (
        result["fte_requested"].fillna(0.0)
        * result["productivity_per_labeller_per_day"].fillna(0.0)
        * float(working_days_elapsed)
    )
    result["monthly_target"] = (
        result["fte_requested"].fillna(0.0)
        * result["productivity_per_labeller_per_day"].fillna(0.0)
        * float(total_working_days_month)
    )
    result["remaining_target"] = result["monthly_target"] - result["actual_tasks"]
    if working_days_remaining > 0:
        result["required_daily_rate"] = result["remaining_target"] / float(working_days_remaining)
    else:
        result["required_daily_rate"] = None
    result["achievement_pct"] = [
        safe_divide(actual, target) * 100 if safe_divide(actual, target) is not None else None
        for actual, target in zip(result["actual_tasks"], result["target_to_date"])
    ]
    result["rag_status"] = result["achievement_pct"].apply(achievement_to_rag)

    result = result[
        [
            "project_name",
            "actual_tasks",
            "target_to_date",
            "monthly_target",
            "remaining_target",
            "required_daily_rate",
            "achievement_pct",
            "rag_status",
        ]
    ].sort_values("project_name", ignore_index=True)

    print(f"[PRODUCTIVITY] Completed for {len(result)} projects")
    return result
