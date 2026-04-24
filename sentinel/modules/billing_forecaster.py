from __future__ import annotations

import pandas as pd

from .config_reader import LoadedConfig
from .intelligence_utils import achievement_to_rag, project_config_dataframe, safe_divide


def forecast_billing(
    fte_df: pd.DataFrame,
    loaded_config: LoadedConfig,
    total_working_days_month: int,
    working_days_elapsed: int,
) -> pd.DataFrame:
    print("[BILLING] Forecasting billing")
    project_config = project_config_dataframe(loaded_config)[["project_name", "fte_requested", "billing_rate"]]

    result = project_config.merge(
        fte_df[["project_name", "actual_hours"]], on="project_name", how="left"
    )
    result["actual_hours"] = result["actual_hours"].fillna(0.0)

    if working_days_elapsed > 0:
        result["daily_billable_rate"] = result["actual_hours"] / float(working_days_elapsed)
        result["projected_month_end"] = result["daily_billable_rate"] * float(total_working_days_month)
    else:
        result["daily_billable_rate"] = None
        result["projected_month_end"] = None

    result["fte_monthly_target"] = result["fte_requested"].fillna(0.0) * 8.0 * float(total_working_days_month)
    result["surplus_cost_bearing"] = result["projected_month_end"] - result["fte_monthly_target"]
    result["deficit_at_risk"] = result["fte_monthly_target"] - result["projected_month_end"]
    result["billing_amount"] = result["projected_month_end"] * result["billing_rate"]

    achievement_pct = [
        safe_divide(projected, target) * 100 if safe_divide(projected, target) is not None else None
        for projected, target in zip(result["projected_month_end"], result["fte_monthly_target"])
    ]
    result["rag_status"] = pd.Series(achievement_pct).apply(achievement_to_rag)

    result = result[
        [
            "project_name",
            "actual_hours",
            "projected_month_end",
            "fte_monthly_target",
            "surplus_cost_bearing",
            "deficit_at_risk",
            "billing_amount",
            "rag_status",
        ]
    ].sort_values("project_name", ignore_index=True)

    print(f"[BILLING] Completed for {len(result)} projects")
    return result
