"""Pipeline execution and data quality report generator.

Collects ingestion statistics, missing rates, transformation counts,
and risk distributions for a given run_dt, generating both structured JSON
and readable Markdown summaries without external observation tools or LLMs.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
import pendulum

from weather_risk_assessment.paths import DATA_ROOT, SINK_DIR
from weather_risk_assessment.utils.run_time import normalize_run_dt

log = logging.getLogger(__name__)
if not log.handlers:
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")

KST = pendulum.timezone("Asia/Seoul")
TOTAL_TARGET_REGIONS = 247


def collect_ingestion_metrics(sink_dir: Path) -> Dict[str, Any]:
    """Inspect local bronze staging parquet files to calculate row counts and missing rates."""
    datasets = {
        "ultra_nowcast": "ultra_nowcast.parquet",
        "ultra_shortfcst": "ultra_shortfcst.parquet",
        "short_fcst": "short_fcst.parquet",
        "uv": "uv.parquet",
        "typhoon": "typhoon.parquet",
    }
    metrics: Dict[str, Any] = {}

    for name, filename in datasets.items():
        file_path = sink_dir / filename
        if not file_path.exists():
            metrics[name] = {
                "exists": False,
                "rows": 0,
                "unique_locations": 0,
                "coverage_pct": 0.0,
                "missing_pct": 100.0,
            }
            continue

        try:
            df = pd.read_parquet(file_path)
            rows = len(df)
            if "admin_code" in df.columns:
                unique_locs = df["admin_code"].nunique()
            elif "nx" in df.columns and "ny" in df.columns:
                unique_locs = df[["nx", "ny"]].drop_duplicates().shape[0]
            else:
                unique_locs = rows

            # Coverage based on target 247 administrative regions
            coverage = min(100.0, round((unique_locs / TOTAL_TARGET_REGIONS) * 100, 2))
            missing = round(max(0.0, 100.0 - coverage), 2)

            metrics[name] = {
                "exists": True,
                "rows": rows,
                "unique_locations": unique_locs,
                "coverage_pct": coverage,
                "missing_pct": missing,
            }
        except Exception as exc:
            log.warning("Failed to inspect %s: %s", file_path, exc)
            metrics[name] = {
                "exists": True,
                "error": str(exc),
                "rows": 0,
                "unique_locations": 0,
                "coverage_pct": 0.0,
                "missing_pct": 100.0,
            }

    return metrics


def collect_risk_metrics(latest_path: Optional[str | Path]) -> Dict[str, Any]:
    """Inspect Gold latest risk Parquet to calculate risk levels and highest risk areas."""
    if not latest_path:
        return {"status": "SKIPPED", "total_rows": 0}

    src = str(latest_path)
    try:
        df = pd.read_parquet(src)
    except Exception as exc:
        log.warning("Could not read latest risk from %s: %s", src, exc)
        return {"status": "UNAVAILABLE", "error": str(exc), "total_rows": 0}

    total_rows = len(df)
    if total_rows == 0:
        return {"status": "EMPTY", "total_rows": 0}

    very_high = int((df["R_total"] >= 0.8).sum()) if "R_total" in df.columns else 0
    high = int(((df["R_total"] >= 0.6) & (df["R_total"] < 0.8)).sum()) if "R_total" in df.columns else 0
    normal = total_rows - very_high - high

    highest_row = df.loc[df["R_total"].idxmax()] if "R_total" in df.columns and total_rows > 0 else None
    highest_info = {}
    if highest_row is not None:
        highest_info = {
            "region": str(highest_row.get("admin_names", "")),
            "r_total": round(float(highest_row.get("R_total", 0.0)), 3),
            "risk_level": str(highest_row.get("risk_level", "NORMAL")),
            "fcst_ts": str(highest_row.get("fcst_ts", "")),
        }

    return {
        "status": "VALID",
        "total_rows": total_rows,
        "very_high_count": very_high,
        "high_count": high,
        "normal_count": normal,
        "highest_risk": highest_info,
    }


def format_markdown_report(report: Dict[str, Any]) -> str:
    """Format quality report dictionary into clean, portfolio-ready Markdown."""
    run_dt = report["run_dt"]
    ts = report["generated_at_kst"]
    ingest = report["ingestion"]
    risk = report["risk_summary"]

    lines = [
        f"# 파이프라인 실행 및 데이터 품질 리포트 (`dt={run_dt}`)",
        f"*생성 시각: {ts} (KST)*",
        "",
        "## 1. 원천 데이터 수집 품질 (Ingestion Quality)",
        "",
        "| 데이터셋 (Dataset) | 상태 | 수집 행 수 (Rows) | 유효 지역 (Locations) | 수집률 (Coverage) | 결측률 (Missing) |",
        "|---|:---:|---:|---:|---:|---:|",
    ]

    dataset_labels = {
        "ultra_nowcast": "초단기실황 (Nowcast)",
        "ultra_shortfcst": "초단기예보 (Ultra Forecast)",
        "short_fcst": "단기예보 (Short Forecast)",
        "uv": "자외선지수 (UV Index)",
        "typhoon": "태풍예측 (Typhoon Track)",
    }

    for key, label in dataset_labels.items():
        m = ingest.get(key, {})
        status = "✅ 정상" if m.get("exists") and m.get("rows", 0) > 0 else "❌ 결측"
        rows = f"{m.get('rows', 0):,}"
        locs = f"{m.get('unique_locations', 0)} / {TOTAL_TARGET_REGIONS}"
        cov = f"{m.get('coverage_pct', 0.0)}%"
        miss = f"{m.get('missing_pct', 0.0)}%"
        lines.append(f"| {label} | {status} | {rows} | {locs} | {cov} | {miss} |")

    lines.extend([
        "",
        "## 2. 골드 레이어 및 위험도 산출 결과 (Gold & Serving)",
        "",
    ])

    if risk.get("status") == "VALID":
        highest = risk.get("highest_risk", {})
        lines.extend([
            f"- **총 평가 지역 수**: `{risk.get('total_rows', 0)}`개 행정구역",
            f"- **위험 등급 분포**: VERY_HIGH (`{risk.get('very_high_count', 0)}`), HIGH (`{risk.get('high_count', 0)}`), NORMAL (`{risk.get('normal_count', 0)}`)",
            f"- **최고 위험 지역**: {highest.get('region', 'N/A')} (종합 위험도 `{highest.get('r_total', 0.0)}`, 등급 `{highest.get('risk_level', 'NORMAL')}`)",
        ])
    else:
        lines.append(f"- **상태**: `{risk.get('status')}`")

    lines.extend([
        "",
        "---",
        "*본 리포트는 파이프라인 완료 시 실측 데이터로부터 자동 집계되었습니다.*",
    ])
    return "\n".join(lines) + "\n"


def generate_report(
    run_dt: Optional[str] = None,
    sink_dir: Optional[Path | str] = None,
    latest_path: Optional[str | Path] = None,
    out_json: Optional[Path | str] = None,
    out_md: Optional[Path | str] = None,
) -> Dict[str, Any]:
    """Generate pipeline quality report and write JSON / Markdown outputs."""
    resolved_run_dt = normalize_run_dt(run_dt)
    resolved_sink_dir = Path(sink_dir) if sink_dir is not None else SINK_DIR / f"dt={resolved_run_dt}"

    # Auto-resolve latest path if not provided
    if latest_path is None:
        bucket = os.getenv("S3_RISK_STREAM_BUCKET", "").strip()
        if bucket:
            latest_path = f"s3://{bucket}/gold_export/risk_latest"
        else:
            local_export = DATA_ROOT / "gold_export" / "risk_latest"
            if local_export.exists():
                latest_path = local_export

    now_kst = pendulum.now(KST).to_iso8601_string()
    ingestion_metrics = collect_ingestion_metrics(resolved_sink_dir)
    risk_metrics = collect_risk_metrics(latest_path)

    report = {
        "run_dt": resolved_run_dt,
        "generated_at_kst": now_kst,
        "target_regions_total": TOTAL_TARGET_REGIONS,
        "ingestion": ingestion_metrics,
        "risk_summary": risk_metrics,
    }

    # Save JSON report
    resolved_json = (
        Path(out_json)
        if out_json is not None
        else DATA_ROOT / "metrics" / f"dt={resolved_run_dt}" / "quality_report.json"
    )
    resolved_json.parent.mkdir(parents=True, exist_ok=True)
    resolved_json.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info("[REPORT] Saved JSON metrics to %s", resolved_json)

    # Save Markdown report
    resolved_md = (
        Path(out_md)
        if out_md is not None
        else Path(__file__).resolve().parents[2] / "docs" / "latest_execution_report.md"
    )
    resolved_md.parent.mkdir(parents=True, exist_ok=True)
    md_content = format_markdown_report(report)
    resolved_md.write_text(md_content, encoding="utf-8")
    log.info("[REPORT] Saved Markdown summary to %s", resolved_md)

    # Optional S3 upload if bucket configured
    bucket = os.getenv("S3_RISK_STREAM_BUCKET", "").strip()
    if bucket:
        try:
            import boto3
            s3 = boto3.client("s3")
            s3_key = f"gold_export/metrics/dt={resolved_run_dt}/quality_report.json"
            s3.upload_file(str(resolved_json), bucket, s3_key)
            log.info("[REPORT] Uploaded metrics to s3://%s/%s", bucket, s3_key)
        except Exception as exc:
            log.warning("Could not upload metrics to S3: %s", exc)

    return report


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Generate execution quality report")
    parser.add_argument("--run_dt", default="", help="Partition time YYYYMMDDHH")
    parser.add_argument("--sink_dir", default="", help="Path to local bronze staging dir")
    parser.add_argument("--latest-path", default="", help="Path or URI to latest risk parquet")
    parser.add_argument("--out-json", default="", help="Destination path for JSON report")
    parser.add_argument("--out-md", default="", help="Destination path for Markdown summary")
    args = parser.parse_args(argv)

    generate_report(
        run_dt=args.run_dt or None,
        sink_dir=args.sink_dir or None,
        latest_path=args.latest_path or None,
        out_json=args.out_json or None,
        out_md=args.out_md or None,
    )


if __name__ == "__main__":
    main()
