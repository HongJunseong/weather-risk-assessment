from __future__ import annotations

import argparse
from datetime import datetime

from weather_risk_assessment.jobs.spark_runtime import (
    create_spark_session,
    resolve_storage_paths,
)


def validate_export_sources(latest, daily, run_dt: str = "") -> tuple[int, int]:
    """Reject empty or stale Gold inputs before overwriting exported Parquet."""
    from pyspark.sql import functions as functions

    latest_count, daily_count = latest.count(), daily.count()
    if not latest_count or not daily_count:
        raise ValueError(f"Gold export inputs empty: latest={latest_count}, daily={daily_count}")
    if run_dt:
        bounds = latest.agg(
            functions.min("dt").alias("first_run"),
            functions.max("dt").alias("last_run"),
            functions.min("fcst_ts").alias("first_forecast"),
        ).first()
        run_time = datetime.strptime(run_dt, "%Y%m%d%H")
        if (
            str(bounds.first_run) != run_dt
            or str(bounds.last_run) != run_dt
            or bounds.first_forecast < run_time
        ):
            raise ValueError(f"Gold export is stale for {run_dt}: {bounds}")
    return latest_count, daily_count


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", default="")
    parser.add_argument("--latest-source", default="")
    parser.add_argument("--daily-source", default="")
    parser.add_argument("--latest-output", default="")
    parser.add_argument("--daily-output", default="")
    parser.add_argument("--run_dt", default="")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    paths = resolve_storage_paths(
        args.bucket,
        {
            "latest_source": args.latest_source,
            "daily_source": args.daily_source,
            "latest_output": args.latest_output,
            "daily_output": args.daily_output,
        },
        {
            "latest_source": ("gold/risk_latest",),
            "daily_source": ("gold/risk_daily",),
            "latest_output": ("gold_export/risk_latest",),
            "daily_output": ("gold_export/risk_daily",),
        },
    )
    spark = create_spark_session("export-gold-parquet")

    try:
        latest = spark.read.format("delta").load(paths["latest_source"])
        daily = spark.read.format("delta").load(paths["daily_source"])
        latest_count, daily_count = validate_export_sources(latest, daily, args.run_dt)
        latest_out = latest.select(
            "nx",
            "ny",
            "admin_names",
            "fcst_ts",
            "R_total",
            "R_rain",
            "R_heat",
            "R_wind",
            "R_uv",
            "R_typhoon",
            "risk_level",
        )
        daily_out = daily.select(
            "date",
            "admin_names",
            "r_total_avg",
            "r_total_max",
            "max_time",
            "obs_cnt",
        )
        latest_out.write.mode("overwrite").parquet(paths["latest_output"])
        daily_out.write.mode("overwrite").partitionBy("date").parquet(
            paths["daily_output"]
        )
        print("[OK] exported parquet:", flush=True)
        print(f" - rows: latest={latest_count}, daily={daily_count}", flush=True)
        print(f" - {paths['latest_output']}", flush=True)
        print(f" - {paths['daily_output']}", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
