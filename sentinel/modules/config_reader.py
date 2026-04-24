from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, date
from pathlib import Path
from typing import Any


DATE_FORMAT = "%m-%d-%Y"


@dataclass(frozen=True)
class RunConfig:
    start_date: date
    end_date: date
    public_holidays: list[date]
    input_folder: Path
    accounts_folder: Path
    output_folder: Path
    accuracy_threshold: float
    ur_threshold: float
    raw_end_date: str


@dataclass(frozen=True)
class ProjectConfig:
    project_name: str
    fte_requested: Any
    billing_rate: Any
    targets: dict[str, Any]


@dataclass(frozen=True)
class TrainingLabeller:
    email: str
    training_start_date: date
    training_end_date: date


@dataclass(frozen=True)
class LoadedConfig:
    run_config: RunConfig
    projects: list[ProjectConfig]
    training_labellers: list[TrainingLabeller]


def _parse_date(value: str, source_name: str) -> date:
    try:
        return datetime.strptime(value.strip(), DATE_FORMAT).date()
    except ValueError as exc:
        raise ValueError(
            f"Invalid date value '{value}' in {source_name}. Expected format: MM-DD-YYYY."
        ) from exc


def _expect_file(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing config file: {path.name}. Expected path: {path}")


def _parse_run_config(path: Path) -> RunConfig:
    _expect_file(path)
    print(f"[CONFIG] Reading run config: {path}")

    parsed: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        parsed[key.strip()] = value.strip()

    required_keys = {
        "start_date",
        "end_date",
        "public_holidays",
        "input_folder",
        "accounts_folder",
        "output_folder",
        "accuracy_threshold",
        "ur_threshold",
    }
    missing_keys = sorted(required_keys - parsed.keys())
    if missing_keys:
        raise ValueError(f"Missing keys in run_config.txt: {', '.join(missing_keys)}")

    start_date = _parse_date(parsed["start_date"], path.name)
    raw_end_date = parsed["end_date"]
    end_date = date.today() if raw_end_date.lower() == "auto" else _parse_date(raw_end_date, path.name)

    public_holidays = []
    if parsed["public_holidays"]:
        public_holidays = [
            _parse_date(value, path.name)
            for value in parsed["public_holidays"].split(",")
            if value.strip()
        ]

    base_dir = path.parent.parent
    return RunConfig(
        start_date=start_date,
        end_date=end_date,
        public_holidays=public_holidays,
        input_folder=base_dir / parsed["input_folder"],
        accounts_folder=base_dir / parsed["accounts_folder"],
        output_folder=base_dir / parsed["output_folder"],
        accuracy_threshold=float(parsed["accuracy_threshold"]),
        ur_threshold=float(parsed["ur_threshold"]),
        raw_end_date=raw_end_date,
    )


def _parse_projects_config(path: Path) -> list[ProjectConfig]:
    _expect_file(path)
    print(f"[CONFIG] Reading projects config: {path}")

    payload = json.loads(path.read_text(encoding="utf-8"))
    projects = payload.get("projects")
    if not isinstance(projects, list) or not projects:
        raise ValueError(f"projects_config.json must contain a non-empty 'projects' list. File: {path}")

    parsed_projects: list[ProjectConfig] = []
    for item in projects:
        project_name = str(item.get("project_name", "")).strip()
        if not project_name:
            raise ValueError(f"Encountered project with missing project_name in {path}")
        parsed_projects.append(
            ProjectConfig(
                project_name=project_name,
                fte_requested=item.get("fte_requested"),
                billing_rate=item.get("billing_rate"),
                targets=item.get("targets", {}),
            )
        )

    return parsed_projects


def _parse_training_labellers(path: Path) -> list[TrainingLabeller]:
    _expect_file(path)
    print(f"[CONFIG] Reading training labellers: {path}")

    labellers: list[TrainingLabeller] = []
    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        parts = [part.strip() for part in line.split(",")]
        if len(parts) != 3:
            raise ValueError(
                f"Invalid training_labellers.txt line {line_number}: '{raw_line}'. "
                "Expected: email, MM-DD-YYYY, MM-DD-YYYY"
            )

        email, start_value, end_value = parts
        training_start_date = _parse_date(start_value, path.name)
        training_end_date = _parse_date(end_value, path.name)
        if training_start_date > training_end_date:
            raise ValueError(
                f"Invalid training range for {email} in {path.name}: start date is after end date."
            )

        labellers.append(
            TrainingLabeller(
                email=email.lower(),
                training_start_date=training_start_date,
                training_end_date=training_end_date,
            )
        )

    return labellers


def load_configs(base_dir: Path) -> LoadedConfig:
    config_dir = base_dir / "config"
    run_config = _parse_run_config(config_dir / "run_config.txt")
    projects = _parse_projects_config(config_dir / "projects_config.json")
    training_labellers = _parse_training_labellers(config_dir / "training_labellers.txt")

    print(
        "[CONFIG] Loaded summary: "
        f"{len(projects)} projects, {len(training_labellers)} training labellers, "
        f"reporting period {run_config.start_date.strftime(DATE_FORMAT)} to "
        f"{run_config.end_date.strftime(DATE_FORMAT)}"
    )

    return LoadedConfig(
        run_config=run_config,
        projects=projects,
        training_labellers=training_labellers,
    )
