# weather_risk_assessment/scripts/upload_bronze_to_s3.py
from __future__ import annotations
import os
from pathlib import Path
import boto3
import pendulum

KST = pendulum.timezone("Asia/Seoul")

def main(run_dt: str | None = None) -> None:
    """
    data/live 아래 원천 parquet들을 S3 bronze로 업로드.
    run_dt: "YYYYMMDDHH" (없으면 현재 KST 기준)
    """
    bucket = os.environ["S3_RISK_STREAM_BUCKET"]  # 너 프로젝트에서 쓰는 버킷 env
    prefix = os.getenv("BRONZE_PREFIX", "bronze/kma")

    project_root = Path(os.getenv("PROJECT_DRE_ROOT", "/opt/airflow/src")).resolve()
    sink_dir = Path(os.getenv("DRE_SINK_DIR", (project_root / "weather_risk_assessment" / "data" / "live").as_posix()))
    sink_dir = Path(sink_dir)

    if not run_dt:
        run_dt = pendulum.now("Asia/Seoul").format("YYYYMMDDHH")

    # upload files
    files = [
        ("ultra_nowcast.parquet", "ultra_nowcast"),
        ("ultra_shortfcst.parquet", "ultra_shortfcst"),
        ("short_fcst.parquet", "short_fcst"),
        ("typhoon.parquet", "typhoon"),
        ("uv.parquet", "uv"),
    ]

    s3 = boto3.client("s3")
    for fname, dataset in files:
        src = sink_dir / fname
        if not src.exists():
            print(f"[SKIP] not found: {src}")
            continue

        # dt 파티션
        key = f"{prefix}/{dataset}/dt={run_dt}/{fname}"
        print(f"[UPLOAD] {src} -> s3://{bucket}/{key}")
        s3.upload_file(str(src), bucket, key)

    print("[DONE] bronze upload")
