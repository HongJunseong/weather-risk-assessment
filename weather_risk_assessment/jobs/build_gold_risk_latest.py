from __future__ import annotations
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

BUCKET = "junseong-weather-risk-stream"
SILVER = f"s3a://{BUCKET}/silver/kma_wide/risk_enriched"
GOLD   = f"s3a://{BUCKET}/gold/risk_latest"

spark = (
    SparkSession.builder
    .appName("build-gold-risk-latest")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

def main():
    df = spark.read.format("delta").load(SILVER)

    # fcst_ts가 없으면 생성
    if "fcst_ts" not in df.columns and {"fcstDate","fcstTime"}.issubset(df.columns):
        df = df.withColumn(
            "fcst_ts",
            F.to_timestamp(F.concat_ws(" ", F.col("fcstDate"), F.col("fcstTime")), "yyyyMMdd HHmm")
        )

    # 지역키: admin_names 기준 최신 1개
    w = Window.partitionBy("admin_names").orderBy(F.col("fcst_ts").desc_nulls_last())
    latest = (
        df.filter(F.col("admin_names").isNotNull())
          .withColumn("rn", F.row_number().over(w))
          .filter(F.col("rn") == 1)
          .drop("rn")
    )

    # 위험등급(원하면 임계값만 바꾸면 됨)
    latest = latest.withColumn(
        "risk_level",
        F.when(F.col("R_total") >= 0.8, "VERY_HIGH")
         .when(F.col("R_total") >= 0.6, "HIGH")
         .when(F.col("R_total") >= 0.4, "MED")
         .otherwise("LOW")
    )

    out = latest.select(
        "admin_names","fcst_ts","dt","R_total",
        "R_rain","R_heat","R_wind","R_uv","R_typhoon",
        "risk_level"
    )

    (out.write.format("delta")
        .mode("overwrite")
        .save(GOLD))

    print(f"[OK] gold wrote: {GOLD}", flush=True)

if __name__ == "__main__":
    main()
    spark.stop()
