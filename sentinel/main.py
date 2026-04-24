from __future__ import annotations

from calendar import monthrange
from datetime import date, timedelta
from pathlib import Path
import sys

import pandas as pd

from modules.account_filter import filter_dataframe_by_accounts, load_account_list
from modules.accuracy_analyzer import analyze_accuracy
from modules.billing_forecaster import forecast_billing
from modules.combiner import combine_dataframes
from modules.config_reader import DATE_FORMAT, LoadedConfig, load_configs
from modules.date_filter import filter_dataframe_by_date, read_dataset_file
from modules.file_identifier import IdentifiedFile, identify_input_files
from modules.fte_calculator import calculate_fte
from modules.productivity_tracker import track_productivity
from modules.report_generator import generate_report
from modules.ur_analyzer import analyze_ur


def ensure_output_folder(output_folder: Path) -> None:
    if output_folder.exists():
        print(f"[SETUP] Output folder exists: {output_folder}")
        return
    output_folder.mkdir(parents=True, exist_ok=True)
    print(f"[SETUP] Created output folder: {output_folder}")


def _is_business_day(current_date: date, public_holidays: set[date]) -> bool:
    return current_date.weekday() < 5 and current_date not in public_holidays


def count_business_days(start_date: date, end_date: date, public_holidays: list[date]) -> int:
    holidays = set(public_holidays)
    total = 0
    cursor = start_date
    while cursor <= end_date:
        if _is_business_day(cursor, holidays):
            total += 1
        cursor += timedelta(days=1)
    return total


def calculate_working_days(loaded_config: LoadedConfig) -> tuple[int, int]:
    run_config = loaded_config.run_config
    today = run_config.end_date
    last_day = monthrange(today.year, today.month)[1]
    last_day_of_month = date(today.year, today.month, last_day)

    elapsed = count_business_days(run_config.start_date, run_config.end_date, run_config.public_holidays)
    remaining_start = today + timedelta(days=1)
    remaining = 0
    if remaining_start <= last_day_of_month:
        remaining = count_business_days(remaining_start, last_day_of_month, run_config.public_holidays)

    print(
        "[DATES] Working days calculated: "
        f"elapsed={elapsed}, remaining={remaining}, "
        f"period={run_config.start_date.strftime(DATE_FORMAT)} to {run_config.end_date.strftime(DATE_FORMAT)}"
    )
    return elapsed, remaining


def calculate_total_working_days_month(loaded_config: LoadedConfig) -> int:
    run_config = loaded_config.run_config
    first_day = date(run_config.end_date.year, run_config.end_date.month, 1)
    last_day = monthrange(run_config.end_date.year, run_config.end_date.month)[1]
    last_day_of_month = date(run_config.end_date.year, run_config.end_date.month, last_day)
    total = count_business_days(first_day, last_day_of_month, run_config.public_holidays)
    print(f"[DATES] Total working days in month = {total}")
    return total


def _read_dataframe(file_info: IdentifiedFile) -> pd.DataFrame:
    print(f"[LOAD] Reading file: {file_info.path} with header row {file_info.header_row}")
    return read_dataset_file(file_info.path, file_info.file_type, header_row=file_info.header_row)


def _report_project_row_counts(
    dataframe: pd.DataFrame,
    dataset_name: str,
    expected_projects: list[str],
) -> None:
    project_column = next(
        (
            column
            for column in dataframe.columns
            if str(column).strip().lower() in {"project_name", "project name", "queue"}
        ),
        None,
    )
    if project_column is None:
        print(f"[ACCOUNT] {dataset_name}: project count skipped because no project column was found")
        return

    counts = dataframe[project_column].fillna("").astype(str).value_counts().to_dict()
    for project_name in expected_projects:
        count = int(counts.get(project_name, 0))
        print(f"[ACCOUNT] {dataset_name}: project '{project_name}' rows after account filter = {count}")
        if count == 0:
            print(f"[ACCOUNT] Warning: no rows remain after account filter for project '{project_name}' in {dataset_name}. Continuing.")


def _periods_overlap(start_a: date, end_a: date, start_b: date, end_b: date) -> bool:
    return max(start_a, start_b) <= min(end_a, end_b)


def flag_training_labellers(master_df: pd.DataFrame, loaded_config: LoadedConfig) -> pd.DataFrame:
    print("[TRAINING] Flagging training labellers")
    working = master_df.copy()
    working["non_billable"] = False

    if working.empty:
        print("[TRAINING] Master dataframe is empty; flagged count = 0")
        return working

    run_config = loaded_config.run_config
    active_training_emails = {
        item.email
        for item in loaded_config.training_labellers
        if _periods_overlap(
            run_config.start_date,
            run_config.end_date,
            item.training_start_date,
            item.training_end_date,
        )
    }

    working["non_billable"] = working["email"].astype(str).str.strip().str.lower().isin(active_training_emails)
    print(f"[TRAINING] Training labellers flagged count = {int(working['non_billable'].sum())}")
    return working


def _print_module_summary(label: str, dataframe: pd.DataFrame) -> None:
    print(f"[SUMMARY] {label}: {len(dataframe)} rows")
    if dataframe.empty:
        return
    print(dataframe.to_string(index=False))


def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
        sys.stderr.reconfigure(encoding="utf-8", errors="backslashreplace")
    except AttributeError:
        pass

    base_dir = Path(__file__).resolve().parent
    print("[MAIN] SENTINEL v2.0 Week 2 pipeline starting")

    try:
        loaded_config = load_configs(base_dir)
        ensure_output_folder(loaded_config.run_config.output_folder)
        working_days_elapsed, working_days_remaining = calculate_working_days(loaded_config)
        total_working_days_month = calculate_total_working_days_month(loaded_config)

        identified_files = identify_input_files(loaded_config.run_config.input_folder)
        print("[FILES] Files identified successfully")
        for file_type, file_info in identified_files.items():
            print(f"[FILES] {file_type}: {file_info.path} (header row {file_info.header_row})")

        raw_frames = {name: _read_dataframe(file_info) for name, file_info in identified_files.items()}

        date_filtered_frames = {}
        for name, frame in raw_frames.items():
            date_filtered_frames[name] = filter_dataframe_by_date(
                dataframe=frame,
                dataset_name=name,
                start_date=loaded_config.run_config.start_date,
                end_date=loaded_config.run_config.end_date,
            )

        accounts_df = load_account_list(loaded_config.run_config.accounts_folder)
        expected_projects = [project.project_name for project in loaded_config.projects]
        account_filtered_frames = {}
        for name, frame in date_filtered_frames.items():
            account_filtered_frames[name] = filter_dataframe_by_accounts(
                dataframe=frame,
                dataset_name=name,
                accounts_df=accounts_df,
            )
            _report_project_row_counts(account_filtered_frames[name], name, expected_projects)

        master_df = combine_dataframes(account_filtered_frames)
        master_df = flag_training_labellers(master_df, loaded_config)

        print("[SUMMARY] Config loaded summary complete")
        print("[SUMMARY] Working days calculated")
        print("[SUMMARY] Files identified")
        print(f"[SUMMARY] Master dataframe shape = {master_df.shape}")
        print(f"[SUMMARY] Training labellers flagged count = {int(master_df['non_billable'].sum())}")

        fte_df = calculate_fte(master_df, loaded_config, working_days_elapsed)
        accuracy_df = analyze_accuracy(master_df, loaded_config)
        productivity_df = track_productivity(
            master_df,
            loaded_config,
            working_days_elapsed=working_days_elapsed,
            working_days_remaining=working_days_remaining,
            total_working_days_month=total_working_days_month,
        )
        ur_df = analyze_ur(master_df, loaded_config)
        billing_df = forecast_billing(
            fte_df,
            loaded_config,
            total_working_days_month=total_working_days_month,
            working_days_elapsed=working_days_elapsed,
        )

        _print_module_summary("FTE Analysis", fte_df)
        _print_module_summary("Accuracy", accuracy_df)
        _print_module_summary("Productivity", productivity_df)
        _print_module_summary("UR Analysis", ur_df)
        _print_module_summary("Billing Forecast", billing_df)

        output_path = generate_report(
            fte_df=fte_df,
            accuracy_df=accuracy_df,
            productivity_df=productivity_df,
            ur_df=ur_df,
            billing_df=billing_df,
            master_df=master_df,
            onduty_filtered=account_filtered_frames["onduty"],
            productivity_filtered=account_filtered_frames["productivity"],
            ur_filtered=account_filtered_frames["ur"],
            accuracy_filtered=account_filtered_frames["accuracy"],
            loaded_config=loaded_config,
        )
        print(f"[OUTPUT] Report saved to: {output_path}")

    except FileNotFoundError as exc:
        print(f"[ERROR] {exc}")
        raise SystemExit(1) from exc
    except ValueError as exc:
        print(f"[ERROR] {exc}")
        raise SystemExit(1) from exc
    except Exception as exc:
        print(f"[ERROR] Unexpected failure: {exc}")
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
