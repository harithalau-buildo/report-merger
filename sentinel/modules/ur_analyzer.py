from __future__ import annotations

import pandas as pd

from .config_reader import LoadedConfig
from .intelligence_utils import (
    achievement_to_rag,
    find_column_by_fragments,
    normalize_percentage_series,
    project_config_dataframe,
    safe_divide,
)


def analyze_ur(master_df: pd.DataFrame, loaded_config: LoadedConfig) -> pd.DataFrame:
    print("[UR] Analyzing utilization rate")
    project_config = project_config_dataframe(loaded_config)[["project_name", "ur_percent"]]

    ur_column = next(
        (
            column
            for column in master_df.columns
            if str(column).strip().lower() in {"ur__ur", "ur"}
        ),
        None,
    )
    if ur_column is None:
        ur_column = find_column_by_fragments(master_df, ["ur"], preferred_prefix="ur__")

    working = master_df.copy()
    working["ur_pct"] = normalize_percentage_series(working[ur_column])
    grouped = working.groupby("project_name", dropna=False, as_index=False)["ur_pct"].mean()
    grouped = grouped.rename(columns={"ur_pct": "avg_ur_pct"})

    result = project_config.merge(grouped, on="project_name", how="left")
    result["flagged_count"] = 0

    for index, row in result.iterrows():
        target = row["ur_percent"]
        if target is None or pd.isna(target):
            continue
        project_rows = working.loc[working["project_name"] == row["project_name"]]
        flagged_count = int((project_rows["ur_pct"] < float(target)).fillna(False).sum())
        result.at[index, "flagged_count"] = flagged_count

    result["achievement_pct"] = [
        safe_divide(actual, target) * 100 if safe_divide(actual, target) is not None else None
        for actual, target in zip(result["avg_ur_pct"], result["ur_percent"])
    ]
    result["rag_status"] = result["achievement_pct"].apply(achievement_to_rag)

    result = result[
        ["project_name", "avg_ur_pct", "achievement_pct", "flagged_count", "rag_status"]
    ].sort_values("project_name", ignore_index=True)

    print(f"[UR] Completed for {len(result)} projects")
    return result
