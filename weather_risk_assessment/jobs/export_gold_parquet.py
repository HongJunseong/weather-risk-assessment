from __future__ import annotations
from pyspark.sql import SparkSession, functions as F

BUCKET = "junseong-weather-risk-stream"

GOLD_LATEST_DELTA = f"s3a://{BUCKET}/gold/risk_latest"
GOLD_DAILY_DELTA  = f"s3a://{BUCKET}/gold/risk_daily"

EXPORT_LATEST = f"s3a://{BUCKET}/gold_export/risk_latest"
EXPORT_DAILY  = f"s3a://{BUCKET}/gold_export/risk_daily"

spark = (
    SparkSession.builder
    .appName("export-gold-parquet")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

def main():
    latest = spark.read.format("delta").load(GOLD_LATEST_DELTA)
    daily  = spark.read.format("delta").load(GOLD_DAILY_DELTA)

    # 보기/적재 편하게 최소 컬럼 정리(원하면 늘려도 됨)
    latest_out = latest.select("admin_names","fcst_ts","R_total","risk_level")
    daily_out  = daily.select("date","admin_names","r_total_avg","r_total_max","max_time","obs_cnt")

    # Parquet으로 export (overwrite)
    (latest_out.write.mode("overwrite").parquet(EXPORT_LATEST))
    (daily_out.write.mode("overwrite").partitionBy("date").parquet(EXPORT_DAILY))

    print("[OK] exported parquet:")
    print(" -", EXPORT_LATEST)
    print(" -", EXPORT_DAILY)

if __name__ == "__main__":
    main()
    spark.stop()