"""Bronze file contracts enforced before external storage upload."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class DatasetContract:
    filename: str
    keys: tuple[str, ...]
    required_columns: frozenset[str]
    metric_columns: frozenset[str]
    allow_empty: bool = False


@dataclass(frozen=True)
class ValidationIssue:
    level: str
    dataset: str
    code: str
    message: str


@dataclass
class BronzeValidationReport:
    issues: list[ValidationIssue]

    @property
    def errors(self) -> list[ValidationIssue]:
        return [issue for issue in self.issues if issue.level == "error"]

    @property
    def warnings(self) -> list[ValidationIssue]:
        return [issue for issue in self.issues if issue.level == "warning"]

    def raise_for_errors(self) -> None:
        if not self.errors:
            return
        details = "; ".join(
            f"{issue.dataset}:{issue.code} ({issue.message})" for issue in self.errors
        )
        raise ValueError(f"Bronze contract validation failed: {details}")


CONTRACTS: dict[str, DatasetContract] = {
    "ultra_nowcast": DatasetContract(
        filename="ultra_nowcast.parquet",
        keys=("baseDate", "baseTime", "nx", "ny"),
        required_columns=frozenset({"baseDate", "baseTime", "nx", "ny"}),
        metric_columns=frozenset({"RN1", "T1H", "REH", "WSD", "PTY"}),
    ),
    "ultra_shortfcst": DatasetContract(
        filename="ultra_shortfcst.parquet",
        keys=("fcstDate", "fcstTime", "nx", "ny"),
        required_columns=frozenset(
            {"baseDate", "baseTime", "fcstDate", "fcstTime", "nx", "ny"}
        ),
        metric_columns=frozenset({"RN1", "T1H", "REH", "WSD", "PTY", "SKY"}),
    ),
    "short_fcst": DatasetContract(
        filename="short_fcst.parquet",
        keys=("fcstDate", "fcstTime", "nx", "ny"),
        required_columns=frozenset({"fcstDate", "fcstTime", "nx", "ny"}),
        metric_columns=frozenset({"PCP", "POP", "TMP", "REH", "WSD", "PTY", "SKY"}),
    ),
    "typhoon": DatasetContract(
        filename="typhoon.parquet",
        keys=("fcstDate", "fcstTime", "nx", "ny"),
        required_columns=frozenset(
            {
                "fcstDate",
                "fcstTime",
                "nx",
                "ny",
                "TY_DISTANCE_KM",
                "TY_MAX_WIND",
                "TY_WARNING",
            }
        ),
        metric_columns=frozenset(
            {"TY_DISTANCE_KM", "TY_MAX_WIND", "TY_WARNING"}
        ),
        allow_empty=True,
    ),
    "uv": DatasetContract(
        filename="uv.parquet",
        keys=("fcstDate", "fcstTime", "nx", "ny"),
        required_columns=frozenset({"fcstDate", "fcstTime", "nx", "ny", "UVI"}),
        metric_columns=frozenset({"UVI"}),
        allow_empty=True,
    ),
}


def _issue(level: str, dataset: str, code: str, message: str) -> ValidationIssue:
    return ValidationIssue(level=level, dataset=dataset, code=code, message=message)


def validate_bronze_frame(dataset: str, frame: pd.DataFrame) -> BronzeValidationReport:
    """Validate one normalized collector output against its Bronze contract."""
    try:
        contract = CONTRACTS[dataset]
    except KeyError as exc:
        raise KeyError(f"Unknown Bronze dataset: {dataset}") from exc

    issues: list[ValidationIssue] = []
    columns = set(frame.columns)
    missing = sorted(contract.required_columns - columns)
    if missing:
        issues.append(_issue("error", dataset, "missing_columns", ", ".join(missing)))
        return BronzeValidationReport(issues)

    if not columns.intersection(contract.metric_columns):
        issues.append(
            _issue("error", dataset, "missing_metrics", "no expected metric column")
        )

    if frame.empty:
        level = "warning" if contract.allow_empty else "error"
        issues.append(_issue(level, dataset, "empty", "dataset has no rows"))
        return BronzeValidationReport(issues)

    null_counts = frame[list(contract.keys)].isna().sum()
    null_keys = {column: int(count) for column, count in null_counts.items() if count}
    if null_keys:
        issues.append(_issue("error", dataset, "null_keys", str(null_keys)))

    duplicate_count = int(frame.duplicated(list(contract.keys), keep=False).sum())
    if duplicate_count:
        issues.append(
            _issue(
                "error",
                dataset,
                "duplicate_keys",
                f"{duplicate_count} rows share a key",
            )
        )

    for prefix in ("base", "fcst"):
        date_column, time_column = f"{prefix}Date", f"{prefix}Time"
        if {date_column, time_column}.issubset(columns):
            value = (
                frame[date_column].astype(str).str.replace(".0", "", regex=False)
                + frame[time_column]
                .astype(str)
                .str.replace(".0", "", regex=False)
                .str.zfill(4)
            )
            invalid_count = int(
                pd.to_datetime(value, format="%Y%m%d%H%M", errors="coerce")
                .isna()
                .sum()
            )
            if invalid_count:
                issues.append(
                    _issue(
                        "error",
                        dataset,
                        f"invalid_{prefix}_time",
                        f"{invalid_count} rows",
                    )
                )

    for coordinate in ("nx", "ny"):
        invalid_count = int(pd.to_numeric(frame[coordinate], errors="coerce").isna().sum())
        if invalid_count:
            issues.append(
                _issue(
                    "error",
                    dataset,
                    "invalid_coordinates",
                    f"{coordinate}: {invalid_count} rows",
                )
            )

    return BronzeValidationReport(issues)


def validate_bronze_directory(directory: str | Path) -> BronzeValidationReport:
    """Validate all collector files expected for one pipeline run."""
    directory = Path(directory)
    issues: list[ValidationIssue] = []
    for dataset, contract in CONTRACTS.items():
        path = directory / contract.filename
        if not path.is_file():
            issues.append(_issue("error", dataset, "missing_file", str(path)))
            continue
        try:
            frame = pd.read_parquet(path)
        except Exception as exc:
            issues.append(_issue("error", dataset, "unreadable_file", str(exc)))
            continue
        issues.extend(validate_bronze_frame(dataset, frame).issues)
    return BronzeValidationReport(issues)
