# weather_risk_assessment_streaming/spark_risk_streaming.py
from __future__ import annotations

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)
from pyspark.sql.functions import from_json, col, when

from config import KafkaConfig, AwsConfig


def build_spark_session(app_name: str = "WeatherRiskStreaming") -> SparkSession:
    """
    SparkSession 생성 + S3A 설정
    """
    aws_cfg = AwsConfig()

    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    spark = (
        SparkSession.builder
        .appName(app_name)
        # S3A 파일시스템
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            f"s3.{aws_cfg.region}.amazonaws.com",
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .getOrCreate()
    )
    return spark


def risk_message_schema() -> StructType:
    """Kafka value(JSON) 파싱용 스키마 – Producer에서 보낸 필드와 일치해야 함"""
    return StructType([
        StructField("nx", IntegerType(), True),
        StructField("ny", IntegerType(), True),
        StructField("admin_names", StringType(), True),
        StructField("baseDate", StringType(), True),
        StructField("fcstDate", StringType(), True),
        StructField("fcstTime", StringType(), True),
        StructField("R_rain", DoubleType(), True),
        StructField("R_heat", DoubleType(), True),
        StructField("R_wind", DoubleType(), True),
        StructField("R_uv", DoubleType(), True),
        StructField("R_typhoon", DoubleType(), True),
        StructField("R_total", DoubleType(), True),
        StructField("fcst_ts", StringType(), True),
    ])


def enrich_risk(df: DataFrame) -> DataFrame:
    """위험도 등급 컬럼 추가 등 간단한 후처리"""
    return df.withColumn(
        "risk_level",
        when(col("R_total") >= 0.8, "HIGH")
        .when(col("R_total") >= 0.5, "MEDIUM")
        .otherwise("LOW"),
    )


def write_to_rds(batch_df: DataFrame, batch_id: int):
    """foreachBatch용 RDS sink (옵션)"""
    aws_cfg = AwsConfig()
    if not aws_cfg.rds_jdbc_url:
        print("RDS_JDBC_URL not set – skip RDS sink for this batch.")
        return

    (
        batch_df
        .select(
            "nx", "ny", "admin_names",
            "fcst_ts", "R_rain", "R_heat",
            "R_wind", "R_uv", "R_typhoon",
            "R_total", "risk_level",
        )
        .write
        .format("jdbc")
        .option("url", aws_cfg.rds_jdbc_url)
        .option("dbtable", "weather_risk_streaming")
        .option("user", aws_cfg.rds_user)
        .option("password", aws_cfg.rds_password)
        .mode("append")
        .save()
    )


def main():
    kafka_cfg = KafkaConfig()
    aws_cfg = AwsConfig()

    # 1) SparkSession 생성
    spark = build_spark_session()

    # 2) Kafka에서 raw stream 읽기
    schema = risk_message_schema()

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_cfg.bootstrap_servers)
        .option("subscribe", kafka_cfg.topic_risk_latest)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        raw.select(from_json(col("value").cast("string"), schema).alias("data"))
        .select("data.*")
    )

    enriched = enrich_risk(parsed)

    # 3) S3 sink 설정
    if not aws_cfg.s3_bucket:
        raise RuntimeError("S3_RISK_STREAM_BUCKET 환경변수가 설정되어야 합니다.")

    # e.g. s3a://junseong-weather-risk-stream/weather_risk_stream
    s3_base = f"s3a://{aws_cfg.s3_bucket}/{aws_cfg.s3_prefix.strip('/')}"
    print(f"[INFO] S3 base path = {s3_base}")

    # (1) S3 Parquet sink
    query_s3 = (
        enriched
        .writeStream
        .format("parquet")
        .option("path", f"{s3_base}/data")
        .option("checkpointLocation", f"{s3_base}/checkpoints")
        .outputMode("append")
        .start()
    )

    # (2) RDS sink (옵션 – 설정되어 있을 때만)
    if aws_cfg.rds_jdbc_url:
        query_rds = (
            enriched
            .writeStream
            .foreachBatch(write_to_rds)
            .outputMode("append")
            .start()
        )
    else:
        query_rds = None

    # (3) 디버깅용 console sink (원하면 켜기)
    enable_console = os.getenv("ENABLE_CONSOLE_SINK", "false").lower() == "true"
    if enable_console:
        query_console = (
            enriched
            .writeStream
            .outputMode("append")
            .format("console")
            .option("truncate", False)
            .start()
        )
    else:
        query_console = None

    # 4) 스트리밍 쿼리들이 종료될 때까지 대기
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
