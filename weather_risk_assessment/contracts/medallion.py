"""Data contracts for Silver and Gold pipeline outputs."""

from __future__ import annotations

from dataclasses import dataclass
from functools import reduce
from typing import Any

import pandas as pd


RISK_COLUMNS = (
    "R_rain",
    "R_heat",
    "R_wind",
    "R_uv",
    "R_typhoon",
    "R_total",
)


@dataclass(frozen=True)
class FrameContract:
    keys: tuple[str, ...]
    required_columns: frozenset[str]
    non_nullable: frozenset[str]
    score_columns: tuple[str, ...]


@dataclass(frozen=True)
class ValidationIssue:
    dataset: str
    code: str
    message: str


@dataclass
class ValidationReport:
    issues: list[ValidationIssue]

    def raise_for_errors(self) -> None:
        if not self.issues:
            return
        details = "; ".join(
            f"{issue.dataset}:{issue.code} ({issue.message})"
            for issue in self.issues
        )
        raise ValueError(f"Medallion contract validation failed: {details}")


CONTRACTS: dict[str, FrameContract] = {
    "silver_risk_enriched": FrameContract(
        keys=("nx", "ny", "fcstDate", "fcstTime"),
        required_columns=frozenset(
            {
                "nx",
                "ny",
                "fcstDate",
                "fcstTime",
                "dt",
                "fcst_ts",
                "admin_names",
                *RISK_COLUMNS,
            }
        ),
        non_nullable=frozenset(
            {"nx", "ny", "fcstDate", "fcstTime", "dt", "fcst_ts", *RISK_COLUMNS}
        ),
        score_columns=RISK_COLUMNS,
    ),
    "gold_risk_latest": FrameContract(
        keys=("admin_names",),
        required_columns=frozenset(
            {"admin_names", "fcst_ts", "dt", "risk_level", *RISK_COLUMNS}
        ),
        non_nullable=frozenset(
            {"admin_names", "fcst_ts", "dt", "risk_level", *RISK_COLUMNS}
        ),
        score_columns=RISK_COLUMNS,
    ),
    "gold_risk_daily": FrameContract(
        keys=("date", "admin_names"),
        required_columns=frozenset(
            {
                "date",
                "admin_names",
                "r_total_avg",
                "r_total_max",
                "obs_cnt",
                "max_time",
            }
        ),
        non_nullable=frozenset(
            {
                "date",
                "admin_names",
                "r_total_avg",
                "r_total_max",
                "obs_cnt",
                "max_time",
            }
        ),
        score_columns=("r_total_avg", "r_total_max"),
    ),
}


def _issue(dataset: str, code: str, message: str) -> ValidationIssue:
    return ValidationIssue(dataset=dataset, code=code, message=message)


def _expected_risk_level(scores: pd.Series) -> pd.Series:
    result = pd.Series("LOW", index=scores.index, dtype="object")
    result.loc[scores >= 0.4] = "MED"
    result.loc[scores >= 0.6] = "HIGH"
    result.loc[scores >= 0.8] = "VERY_HIGH"
    return result


def _invalid_datetime_count(values: pd.Series, fmt: str) -> int:
    return int(pd.to_datetime(values, format=fmt, errors="coerce").isna().sum())


def validate_frame(dataset: str, frame: pd.DataFrame) -> ValidationReport:
    """Validate a small or sampled Silver/Gold frame without requiring Spark."""
    try:
        contract = CONTRACTS[dataset]
    except KeyError as exc:
        raise KeyError(f"Unknown medallion dataset: {dataset}") from exc

    issues: list[ValidationIssue] = []
    columns = set(frame.columns)
    missing = sorted(contract.required_columns - columns)
    if missing:
        return ValidationReport(
            [_issue(dataset, "missing_columns", ", ".join(missing))]
        )
    if frame.empty:
        return ValidationReport([_issue(dataset, "empty", "dataset has no rows")])

    null_counts = frame[list(contract.non_nullable)].isna().sum()
    nulls = {column: int(count) for column, count in null_counts.items() if count}
    if nulls:
        issues.append(_issue(dataset, "null_values", str(nulls)))

    duplicate_count = int(frame.duplicated(list(contract.keys), keep=False).sum())
    if duplicate_count:
        issues.append(
            _issue(dataset, "duplicate_keys", f"{duplicate_count} rows share a key")
        )

    for column in contract.score_columns:
        values = pd.to_numeric(frame[column], errors="coerce")
        invalid_count = int((values.isna() | ~values.between(0.0, 1.0)).sum())
        if invalid_count:
            issues.append(
                _issue(
                    dataset,
                    "invalid_score",
                    f"{column}: {invalid_count} rows outside [0, 1]",
                )
            )

    if dataset == "silver_risk_enriched":
        forecast_values = (
            frame["fcstDate"].astype(str).str.replace(".0", "", regex=False)
            + frame["fcstTime"]
            .astype(str)
            .str.replace(".0", "", regex=False)
            .str.zfill(4)
        )
        invalid_forecast = _invalid_datetime_count(forecast_values, "%Y%m%d%H%M")
        invalid_run = _invalid_datetime_count(frame["dt"].astype(str), "%Y%m%d%H")
        invalid_timestamp = int(pd.to_datetime(frame["fcst_ts"], errors="coerce").isna().sum())
        if invalid_forecast or invalid_timestamp:
            issues.append(
                _issue(
                    dataset,
                    "invalid_forecast_time",
                    f"key={invalid_forecast}, fcst_ts={invalid_timestamp}",
                )
            )
        if invalid_run:
            issues.append(_issue(dataset, "invalid_run_dt", f"{invalid_run} rows"))

    elif dataset == "gold_risk_latest":
        invalid_timestamp = int(pd.to_datetime(frame["fcst_ts"], errors="coerce").isna().sum())
        if invalid_timestamp:
            issues.append(
                _issue(dataset, "invalid_forecast_time", f"{invalid_timestamp} rows")
            )
        scores = pd.to_numeric(frame["R_total"], errors="coerce")
        mismatch_count = int((frame["risk_level"] != _expected_risk_level(scores)).sum())
        if mismatch_count:
            issues.append(
                _issue(dataset, "risk_level_mismatch", f"{mismatch_count} rows")
            )

    elif dataset == "gold_risk_daily":
        invalid_date = int(pd.to_datetime(frame["date"], errors="coerce").isna().sum())
        times = frame["max_time"].astype(str).str.replace(".0", "", regex=False).str.zfill(4)
        invalid_time = _invalid_datetime_count(times, "%H%M")
        obs = pd.to_numeric(frame["obs_cnt"], errors="coerce")
        invalid_obs = int((obs.isna() | (obs <= 0) | (obs % 1 != 0)).sum())
        averages = pd.to_numeric(frame["r_total_avg"], errors="coerce")
        maxima = pd.to_numeric(frame["r_total_max"], errors="coerce")
        invalid_order = int((maxima < averages).sum())
        if invalid_date or invalid_time:
            issues.append(
                _issue(
                    dataset,
                    "invalid_period",
                    f"date={invalid_date}, max_time={invalid_time}",
                )
            )
        if invalid_obs:
            issues.append(_issue(dataset, "invalid_obs_count", f"{invalid_obs} rows"))
        if invalid_order:
            issues.append(
                _issue(dataset, "invalid_aggregate", f"{invalid_order} rows")
            )

    return ValidationReport(issues)


def _spark_risk_level(functions: Any, score: Any) -> Any:
    return (
        functions.when(score >= 0.8, "VERY_HIGH")
        .when(score >= 0.6, "HIGH")
        .when(score >= 0.4, "MED")
        .otherwise("LOW")
    )


def validate_spark_frame(dataset: str, frame: Any) -> ValidationReport:
    """Validate a Spark DataFrame with distributed checks before writing it."""
    try:
        contract = CONTRACTS[dataset]
    except KeyError as exc:
        raise KeyError(f"Unknown medallion dataset: {dataset}") from exc

    columns = set(frame.columns)
    missing = sorted(contract.required_columns - columns)
    if missing:
        return ValidationReport(
            [_issue(dataset, "missing_columns", ", ".join(missing))]
        )

    from pyspark.sql import functions as functions

    if frame.limit(1).count() == 0:
        return ValidationReport([_issue(dataset, "empty", "dataset has no rows")])

    checks: dict[str, Any] = {}
    checks["null_values"] = reduce(
        lambda left, right: left | right,
        (functions.col(column).isNull() for column in contract.non_nullable),
    )
    for column in contract.score_columns:
        score = functions.col(column).cast("double")
        checks[f"invalid_score:{column}"] = (
            score.isNull() | functions.isnan(score) | (score < 0.0) | (score > 1.0)
        )

    if dataset == "silver_risk_enriched":
        forecast = functions.concat(
            functions.col("fcstDate").cast("string"),
            functions.lpad(functions.col("fcstTime").cast("string"), 4, "0"),
        )
        checks["invalid_forecast_time"] = (
            functions.to_timestamp(forecast, "yyyyMMddHHmm").isNull()
            | functions.col("fcst_ts").isNull()
        )
        checks["invalid_run_dt"] = functions.to_timestamp(
            functions.col("dt").cast("string"), "yyyyMMddHH"
        ).isNull()
    elif dataset == "gold_risk_latest":
        checks["invalid_forecast_time"] = functions.col("fcst_ts").isNull()
        checks["risk_level_mismatch"] = functions.col("risk_level") != _spark_risk_level(
            functions, functions.col("R_total")
        )
    elif dataset == "gold_risk_daily":
        checks["invalid_period"] = (
            functions.col("date").cast("date").isNull()
            | functions.to_timestamp(
                functions.concat(
                    functions.lit("20000101"),
                    functions.lpad(functions.col("max_time").cast("string"), 4, "0"),
                ),
                "yyyyMMddHHmm",
            ).isNull()
        )
        obs = functions.col("obs_cnt").cast("double")
        checks["invalid_obs_count"] = obs.isNull() | (obs <= 0) | (obs != functions.floor(obs))
        checks["invalid_aggregate"] = (
            functions.col("r_total_max") < functions.col("r_total_avg")
        )

    aggregates = [
        functions.max(functions.when(condition, 1).otherwise(0)).alias(code)
        for code, condition in checks.items()
    ]
    result = frame.agg(*aggregates).first().asDict()
    issues = [
        _issue(dataset, code, "at least one invalid row")
        for code, failed in result.items()
        if failed
    ]

    duplicate = (
        frame.groupBy(*contract.keys)
        .count()
        .filter(functions.col("count") > 1)
        .limit(1)
        .count()
    )
    if duplicate:
        issues.append(_issue(dataset, "duplicate_keys", "at least one duplicate key"))

    return ValidationReport(issues)
