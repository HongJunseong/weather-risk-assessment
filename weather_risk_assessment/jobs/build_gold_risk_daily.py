from __future__ import annotations

import argparse
from typing import Any

from weather_risk_assessment.contracts.medallion import validate_spark_frame
from weather_risk_assessment.jobs.spark_runtime import (
    create_spark_session,
    resolve_storage_paths,
)


def build_daily(frame: Any) -> Any:
    """Aggregate Silver risk rows by forecast date and administrative region."""
    from pyspark.sql import functions as functions

    dated = frame.withColumn(
        "date", functions.to_date(functions.col("fcstDate"), "yyyyMMdd")
    )
    return (
        dated.filter(functions.col("admin_names").isNotNull())
        .groupBy("date", "admin_names")
        .agg(
            functions.avg("R_total").alias("r_total_avg"),
            functions.max("R_total").alias("r_total_max"),
            functions.count("*").alias("obs_cnt"),
            functions.max(
                functions.struct(functions.col("R_total"), functions.col("fcstTime"))
            ).alias("max_struct"),
        )
        .withColumn("max_time", functions.col("max_struct")["fcstTime"])
        .drop("max_struct")
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", default="")
    parser.add_argument("--silver", default="")
    parser.add_argument("--gold", default="")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    paths = resolve_storage_paths(
        args.bucket,
        {"silver": args.silver, "gold": args.gold},
        {
            "silver": ("silver/kma_wide/risk_enriched",),
            "gold": ("gold/risk_daily",),
        },
    )
    spark = create_spark_session("build-gold-risk-daily")

    try:
        output = build_daily(spark.read.format("delta").load(paths["silver"]))
        validate_spark_frame("gold_risk_daily", output).raise_for_errors()
        (
            output.write.format("delta")
            .mode("overwrite")
            .partitionBy("date")
            .save(paths["gold"])
        )
        print(f"[OK] gold wrote: {paths['gold']}", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
