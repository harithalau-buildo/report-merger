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


def analyze_accuracy(master_df: pd.DataFrame, loaded_config: LoadedConfig) -> pd.DataFrame:
    print("[ACCURACY] Analyzing project accuracy")
    project_config = project_config_dataframe(loaded_config)[["project_name", "accuracy_percent"]]

    pre_column = find_column_by_fragments(master_df, ["pre", "sar"], preferred_prefix="accuracy__")
    post_column = find_column_by_fragments(master_df, ["post", "sar"], preferred_prefix="accuracy__")

    working = master_df.copy()
    working["pre_accuracy"] = normalize_percentage_series(working[pre_column])
    working["post_accuracy"] = normalize_percentage_series(working[post_column])
    working["delta"] = working["post_accuracy"] - working["pre_accuracy"]

    grouped = (
        working.groupby("project_name", dropna=False, as_index=False)
        .agg(
            avg_pre_accuracy=("pre_accuracy", "mean"),
            avg_post_accuracy=("post_accuracy", "mean"),
            avg_delta=("delta", "mean"),
        )
    )

    result = project_config.merge(grouped, on="project_name", how="left")
    result["flagged_count"] = 0

    for index, row in result.iterrows():
        target = row["accuracy_percent"]
        if target is None or pd.isna(target):
            continue
        project_rows = working.loc[working["project_name"] == row["project_name"]]
        flagged_count = int((project_rows["post_accuracy"] < float(target)).fillna(False).sum())
        result.at[index, "flagged_count"] = flagged_count

    result["achievement_pct"] = [
        safe_divide(actual, target) * 100 if safe_divide(actual, target) is not None else None
        for actual, target in zip(result["avg_post_accuracy"], result["accuracy_percent"])
    ]
    result["rag_status"] = result["achievement_pct"].apply(achievement_to_rag)

    result = result[
        [
            "project_name",
            "avg_pre_accuracy",
            "avg_post_accuracy",
            "avg_delta",
            "achievement_pct",
            "flagged_count",
            "rag_status",
        ]
    ].sort_values("project_name", ignore_index=True)

    print(f"[ACCURACY] Completed for {len(result)} projects")
    return result
