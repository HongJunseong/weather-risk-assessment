from __future__ import annotations
from pyspark.sql import SparkSession, functions as F

BUCKET = "junseong-weather-risk-stream"
SILVER = f"s3a://{BUCKET}/silver/kma_wide/risk_enriched"
GOLD   = f"s3a://{BUCKET}/gold/risk_daily"

spark = (
    SparkSession.builder
    .appName("build-gold-risk-daily")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

def main():
    df = spark.read.format("delta").load(SILVER)

    # 날짜 컬럼
    df = df.withColumn("date", F.to_date(F.col("fcstDate"), "yyyyMMdd"))

    daily = (
        df.filter(F.col("admin_names").isNotNull())
          .groupBy("date", "admin_names")
          .agg(
              F.avg("R_total").alias("r_total_avg"),
              F.max("R_total").alias("r_total_max"),
              F.count("*").alias("obs_cnt"),
              F.max(F.struct(F.col("R_total"), F.col("fcstTime"))).alias("max_struct")
          )
          .withColumn("max_time", F.col("max_struct")["fcstTime"])
          .drop("max_struct")
    )

    (daily.write.format("delta")
          .mode("overwrite")
          .partitionBy("date")
          .save(GOLD))

    print(f"[OK] gold wrote: {GOLD}", flush=True)

if __name__ == "__main__":
    main()
    spark.stop()
