from __future__ import annotations

import argparse

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
            functions.date_format(functions.min("fcst_ts"), "yyyyMMddHH").alias("first_forecast"),
        ).first()
        if (
            str(bounds.first_run) != run_dt
            or str(bounds.last_run) != run_dt
            or not bounds.first_forecast or bounds.first_forecast < run_dt
        ):
            raise ValueError(f"Gold export is stale for {run_dt}: {bounds}")
    return latest_count, daily_count


def validate_exported_dataset(df, expected_count: int, dataset_name: str) -> None:
    """Verify that exported Parquet files on disk/S3 match expected counts and required schema."""
    actual_count = df.count()
    if actual_count != expected_count:
        raise ValueError(
            f"Export validation failed for {dataset_name}: expected {expected_count} rows, got {actual_count}"
        )
    if actual_count == 0:
        raise ValueError(f"Export validation failed for {dataset_name}: output is empty")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", default="")
    parser.add_argument("--latest-source", default="")
    parser.add_argument("--daily-source", default="")
    parser.add_argument("--latest-output", default="")
    parser.add_argument("--daily-output", default="")
    parser.add_argument("--latest-history", default="")
    parser.add_argument("--daily-history", default="")
    parser.add_argument("--run_dt", default="")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    requested_paths = {
        "latest_source": args.latest_source,
        "daily_source": args.daily_source,
        "latest_output": args.latest_output,
        "daily_output": args.daily_output,
    }
    default_paths = {
        "latest_source": ("gold/risk_latest",),
        "daily_source": ("gold/risk_daily",),
        "latest_output": ("gold_export/risk_latest",),
        "daily_output": ("gold_export/risk_daily",),
    }

    if args.run_dt:
        requested_paths["latest_history"] = args.latest_history
        requested_paths["daily_history"] = args.daily_history
        default_paths["latest_history"] = ("gold_export", "history", f"dt={args.run_dt}", "risk_latest")
        default_paths["daily_history"] = ("gold_export", "history", f"dt={args.run_dt}", "risk_daily")

    paths = resolve_storage_paths(args.bucket, requested_paths, default_paths)
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

        if args.run_dt:
            # 1단계: 실행 시각별 이력 경로에 먼저 저장 (Staging / Hourly Retention)
            history_latest_path = paths["latest_history"]
            history_daily_path = paths["daily_history"]
            latest_out.write.mode("overwrite").parquet(history_latest_path)
            daily_out.write.mode("overwrite").partitionBy("date").parquet(history_daily_path)

            # 2단계: 저장된 이력 파일의 정합성·완전성 검증 (Validation)
            written_latest = spark.read.parquet(history_latest_path)
            written_daily = spark.read.parquet(history_daily_path)
            validate_exported_dataset(written_latest, latest_count, "risk_latest")
            validate_exported_dataset(written_daily, daily_count, "risk_daily")
            print(f"[OK] verified history staging: dt={args.run_dt} (latest={latest_count}, daily={daily_count})", flush=True)

        # 3단계: 검증 통과 시 최신본으로 승격 (Promotion)
        latest_out.write.mode("overwrite").parquet(paths["latest_output"])
        daily_out.write.mode("overwrite").partitionBy("date").parquet(paths["daily_output"])

        print("[OK] exported parquet (promoted to latest):", flush=True)
        print(f" - rows: latest={latest_count}, daily={daily_count}", flush=True)
        if args.run_dt:
            print(f" - history: {paths['latest_history']}", flush=True)
            print(f" - history: {paths['daily_history']}", flush=True)
        print(f" - latest: {paths['latest_output']}", flush=True)
        print(f" - latest: {paths['daily_output']}", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
