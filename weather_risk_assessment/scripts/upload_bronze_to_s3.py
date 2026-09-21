# weather_risk_assessment/scripts/upload_bronze_to_s3.py
from __future__ import annotations
import os
from pathlib import Path
import boto3

from weather_risk_assessment.contracts.bronze import (
    CONTRACTS,
    validate_bronze_directory,
)
from weather_risk_assessment.utils.run_time import normalize_run_dt

def main(run_dt: str | None = None, sink_dir: str | Path | None = None) -> None:
    """
    실행별 로컬 디렉터리의 원천 parquet들을 S3 Bronze로 업로드.
    run_dt: "YYYYMMDDHH" (없으면 현재 KST 기준)
    """
    bucket = os.environ["S3_RISK_STREAM_BUCKET"]  # 너 프로젝트에서 쓰는 버킷 env
    prefix = os.getenv("BRONZE_PREFIX", "bronze/kma")
    run_dt = normalize_run_dt(run_dt)

    from weather_risk_assessment.paths import SINK_DIR
    sink_dir = Path(sink_dir) if sink_dir is not None else SINK_DIR / f"dt={run_dt}"
    if sink_dir.name != f"dt={run_dt}":
        raise ValueError(
            f"sink_dir must end with dt={run_dt}, got {sink_dir}"
        )

    report = validate_bronze_directory(sink_dir)
    for issue in report.issues:
        print(
            f"[CONTRACT][{issue.level.upper()}] "
            f"{issue.dataset}:{issue.code} {issue.message}"
        )
    report.raise_for_errors()

    s3 = boto3.client("s3")
    for dataset, contract in CONTRACTS.items():
        src = sink_dir / contract.filename

        # dt 파티션
        key = f"{prefix}/{dataset}/dt={run_dt}/{contract.filename}"
        print(f"[UPLOAD] {src} -> s3://{bucket}/{key}")
        s3.upload_file(str(src), bucket, key)

    print("[DONE] bronze upload")
