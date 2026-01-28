# weather_risk_assessment/jobs/build_silver_risk_enriched_from_bronze.py
from __future__ import annotations

import argparse
from datetime import datetime
import pendulum

import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
)

# === 너 프로젝트 고정값 (env 없이) ===
BUCKET = "junseong-weather-risk-stream"
BRONZE_PREFIX = "bronze/kma"

# silver base (너가 말한 경로 기준) + 결과는 risk_enriched로 분리 추천
SILVER_OUT = f"s3a://{BUCKET}/silver/kma_wide/risk_enriched"

# admin map (컨테이너/리포에 실제 존재하는 경로로 맞춰)
DEFAULT_ADMIN_MAP = "/opt/***/src/weather_risk_assessment/data/admin_centroids.csv"

KEYS = ["nx", "ny", "fcstDate", "fcstTime"]

# --- 너가 이미 갖고 있는 계산 함수들 ---
from weather_risk_assessment.risk.heat_risk import compute_heat_risk
from weather_risk_assessment.risk.rain_risk import compute_rain_risk
from weather_risk_assessment.risk.typhoon_risk import compute_typhoon_risk
from weather_risk_assessment.risk.wind_risk import compute_wind_risk
from weather_risk_assessment.risk.uv_risk import compute_uv_risk


def now_run_dt_kst() -> str:
    # YYYYMMDDHH (KST)
    return pendulum.now("Asia/Seoul").format("YYYYMMDDHH")


def s3_dt(dataset: str, run_dt: str) -> str:
    return f"s3a://{BUCKET}/{BRONZE_PREFIX}/{dataset}/dt={run_dt}/"


def read_parquet_dir(spark: SparkSession, path: str) -> DataFrame | None:
    try:
        return spark.read.parquet(path)
    except Exception as e:
        print(f"[SKIP] {path} not readable: {e}", flush=True)
        return None


NUM_COLS = [
    "RN1","PCP","POP","WSD","WGS","GUST","UUU","VVV",
    "T1H","TMP","REH","PTY","SKY","UVI","UV_INDEX",
    "TY_DISTANCE_KM","TY_MAX_WIND","TY_WARNING"
]

def canon(df: DataFrame) -> DataFrame:
    if "nx" in df.columns: df = df.withColumn("nx", F.col("nx").cast("int"))
    if "ny" in df.columns: df = df.withColumn("ny", F.col("ny").cast("int"))

    if "fcstDate" in df.columns:
        df = df.withColumn("fcstDate", F.regexp_replace(F.col("fcstDate").cast("string"), "-", ""))
    if "fcstTime" in df.columns:
        df = df.withColumn("fcstTime", F.lpad(F.regexp_replace(F.col("fcstTime").cast("string"), ":", ""), 4, "0"))

    if "baseDate" in df.columns:
        df = df.withColumn("baseDate", F.regexp_replace(F.col("baseDate").cast("string"), "-", ""))
    if "baseTime" in df.columns:
        df = df.withColumn("baseTime", F.lpad(F.regexp_replace(F.col("baseTime").cast("string"), ":", ""), 4, "0"))

    # 숫자 컬럼 Double로 통일
    for c in NUM_COLS:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("double"))

    return df


def safe_join(base: DataFrame, right: DataFrame | None, keep_cols: list[str], suffix: str) -> DataFrame:
    if right is None:
        return base

    for k in KEYS:
        if k not in right.columns:
            print(f"[INFO] join-skip: right missing key={k}, right_cols={right.columns}", flush=True)
            return base

    keep = [c for c in keep_cols if c in right.columns]
    r = right.select(*KEYS, *[c for c in keep if c not in KEYS]).dropDuplicates(KEYS)

    for c in r.columns:
        if c not in KEYS:
            r = r.withColumnRenamed(c, f"{c}{suffix}")

    out = base.join(r, on=KEYS, how="left")

    for c in keep:
        rc = f"{c}{suffix}"
        if rc in out.columns:
            if c in out.columns:
                out = out.withColumn(c, F.coalesce(F.col(c), F.col(rc))).drop(rc)
            else:
                out = out.withColumnRenamed(rc, c)

    return out


def attach_admin_names(spark: SparkSession, df: DataFrame, admin_map_path: str) -> DataFrame:
    # admin_centroids.csv: nx, ny, admin_name 컬럼 가정
    adm = spark.read.option("header", True).csv(admin_map_path)

    # nx/ny 캐스팅
    if "nx" in adm.columns:
        adm = adm.withColumn("nx", F.col("nx").cast("int"))
    if "ny" in adm.columns:
        adm = adm.withColumn("ny", F.col("ny").cast("int"))

    if not {"nx", "ny", "admin_name"}.issubset(set(adm.columns)):
        print(f"[WARN] admin map missing columns. columns={adm.columns}", flush=True)
        return df.withColumn("admin_names", F.lit(None).cast("string"))

    # 같은 격자에 admin_name 여러 개면 | 로 묶기
    adm_list = (
        adm.groupBy("nx", "ny")
           .agg(F.sort_array(F.collect_set(F.col("admin_name").cast("string"))).alias("admin_list"))
           .withColumn("admin_names", F.concat_ws("|", F.col("admin_list")))
           .select("nx", "ny", "admin_names")
    )

    return df.join(adm_list, on=["nx", "ny"], how="left")


def add_risks_map_in_pandas(df: DataFrame) -> DataFrame:
    """
    pandas 기반 compute_*_risk를 Spark에 안전하게 붙이는 방법:
    - mapInPandas로 파티션 단위 pandas DF를 받아서 계산 후 반환
    """
    # 기존 컬럼 스키마 + 위험도 6개 추가
    base_fields = []
    for f in df.schema.fields:
        base_fields.append(f)

    out_schema = StructType(base_fields + [
        StructField("R_rain", DoubleType(), True),
        StructField("R_heat", DoubleType(), True),
        StructField("R_wind", DoubleType(), True),
        StructField("R_uv", DoubleType(), True),
        StructField("R_typhoon", DoubleType(), True),
        StructField("R_total", DoubleType(), True),
    ])

    def _calc(iterator):
        for pdf in iterator:
            # pandas에서 계산
            # (필요시 숫자 캐스팅)
            for c in ["PCP", "POP", "WSD", "RN1", "REH", "T1H", "TMP", "UVI", "UV_INDEX"]:
                if c in pdf.columns:
                    pdf[c] = pd.to_numeric(pdf[c], errors="coerce")

            pdf["R_rain"]    = compute_rain_risk(pdf)
            pdf["R_heat"]    = compute_heat_risk(pdf)
            pdf["R_wind"]    = compute_wind_risk(pdf)
            pdf["R_uv"]      = compute_uv_risk(pdf)
            pdf["R_typhoon"] = compute_typhoon_risk(pdf)

            w = {"rain":0.28, "heat":0.18, "wind":0.22, "uv":0.12, "typhoon":0.20}
            weighted = (
                w["rain"]*pdf["R_rain"] + w["heat"]*pdf["R_heat"] +
                w["wind"]*pdf["R_wind"] + w["uv"]*pdf["R_uv"] +
                w["typhoon"]*pdf["R_typhoon"]
            ).clip(0, 1)

            peak = pd.concat(
                [pdf["R_rain"], pdf["R_heat"], pdf["R_wind"], pdf["R_uv"], pdf["R_typhoon"]],
                axis=1
            ).max(axis=1)

            pdf["R_total"] = (0.7*peak + 0.3*weighted).clip(0, 1)

            yield pdf

    return df.mapInPandas(_calc, schema=out_schema)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_dt", default="", help="YYYYMMDDHH. 비우면 KST 현재시각으로 자동")
    parser.add_argument("--mode", default="overwrite", choices=["overwrite", "append"])
    parser.add_argument("--silver_out", default=SILVER_OUT)
    parser.add_argument("--admin_map", default=DEFAULT_ADMIN_MAP)
    args = parser.parse_args()

    run_dt = args.run_dt.strip() or now_run_dt_kst()

    spark = (
        SparkSession.builder
        .appName("build-silver-risk-enriched-from-bronze")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    # base = ultra_shortfcst
    df = read_parquet_dir(spark, s3_dt("ultra_shortfcst", run_dt))
    if df is None:
        raise SystemExit(f"[FATAL] ultra_shortfcst bronze not found on S3 for dt={run_dt}")
    df = canon(df)

    df_nc  = read_parquet_dir(spark, s3_dt("ultra_nowcast", run_dt))
    df_vil = read_parquet_dir(spark, s3_dt("short_fcst", run_dt))
    df_ty  = read_parquet_dir(spark, s3_dt("typhoon", run_dt))
    df_uv  = read_parquet_dir(spark, s3_dt("uv", run_dt))

    df_nc  = canon(df_nc)  if df_nc  is not None else None
    df_vil = canon(df_vil) if df_vil is not None else None
    df_ty  = canon(df_ty)  if df_ty  is not None else None
    df_uv  = canon(df_uv)  if df_uv  is not None else None

    # compute_risk.py와 같은 “의미”로 병합
    df = safe_join(df, df_nc,  ["RN1","REH","T1H","UUU","VVV","WSD","PTY","SKY","VEC"], "_nc")
    df = safe_join(df, df_vil, ["PCP","POP","WSD"], "_vil")
    df = safe_join(df, df_ty,  ["TY_DISTANCE_KM","TY_MAX_WIND","TY_WARNING"], "_ty")
    df = safe_join(df, df_uv,  ["UVI","UV_INDEX"], "_uv")

    # 부가 컬럼
    df = df.withColumn("dt", F.lit(run_dt))
    df = df.withColumn("ingested_at", F.current_timestamp())

    # 중복 제거
    if set(KEYS).issubset(df.columns):
        df = df.dropDuplicates(KEYS)

    # admin_names 붙이기
    df = attach_admin_names(spark, df, args.admin_map)

    # 위험도 계산 붙이기 (R_* + R_total 생성)
    df = df.repartition(4)
    df = add_risks_map_in_pandas(df)

    # fcst_ts 생성 (gold에서 쓰기 편하게)
    if "fcstDate" in df.columns and "fcstTime" in df.columns:
        df = df.withColumn(
            "fcst_ts",
            F.to_timestamp(F.concat_ws(" ", F.col("fcstDate"), F.col("fcstTime")), "yyyyMMdd HHmm")
        )

    # silver 저장 (Delta)
    (df.write
      .format("delta")
      .mode(args.mode)
      .partitionBy("dt")
      .save(args.silver_out))

    print(f"[OK] silver risk_enriched wrote: {args.silver_out} dt={run_dt}", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
