from __future__ import annotations

import argparse
from typing import Any

from weather_risk_assessment.contracts.medallion import validate_spark_frame
from weather_risk_assessment.jobs.spark_runtime import (
    create_spark_session,
    resolve_storage_paths,
)


def build_latest(frame: Any) -> Any:
    """Select one latest forecast per administrative region."""
    from pyspark.sql import functions as functions
    from pyspark.sql.window import Window

    if "fcst_ts" not in frame.columns and {"fcstDate", "fcstTime"}.issubset(
        frame.columns
    ):
        frame = frame.withColumn(
            "fcst_ts",
            functions.to_timestamp(
                functions.concat_ws(
                    " ", functions.col("fcstDate"), functions.col("fcstTime")
                ),
                "yyyyMMdd HHmm",
            ),
        )

    window = Window.partitionBy("admin_names").orderBy(
        functions.col("fcst_ts").desc_nulls_last()
    )
    latest = (
        frame.filter(functions.col("admin_names").isNotNull())
        .withColumn("rn", functions.row_number().over(window))
        .filter(functions.col("rn") == 1)
        .drop("rn")
    )
    latest = latest.withColumn(
        "risk_level",
        functions.when(functions.col("R_total") >= 0.8, "VERY_HIGH")
        .when(functions.col("R_total") >= 0.6, "HIGH")
        .when(functions.col("R_total") >= 0.4, "MED")
        .otherwise("LOW"),
    )
    return latest.select(
        "nx",
        "ny",
        "admin_names",
        "fcst_ts",
        "dt",
        "R_total",
        "R_rain",
        "R_heat",
        "R_wind",
        "R_uv",
        "R_typhoon",
        "risk_level",
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
            "gold": ("gold/risk_latest",),
        },
    )
    spark = create_spark_session("build-gold-risk-latest")

    try:
        output = build_latest(spark.read.format("delta").load(paths["silver"]))
        validate_spark_frame("gold_risk_latest", output).raise_for_errors()
        output.write.format("delta").mode("overwrite").save(paths["gold"])
        print(f"[OK] gold wrote: {paths['gold']}", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
