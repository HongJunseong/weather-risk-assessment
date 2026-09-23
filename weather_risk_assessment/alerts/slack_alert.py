from __future__ import annotations

import os
import logging

import requests

log = logging.getLogger(__name__)
if not log.handlers:
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")

HIGH_RISK_THRESHOLD = 0.6

RISK_EMOJI = {
    "VERY_HIGH": "🔴",
    "HIGH":      "🟠",
}


def resolve_risk_path(path: str | None = None) -> str:
    if path:
        return path
    configured = os.getenv("RISK_LATEST_PATH", "").strip()
    if configured:
        return configured
    bucket = os.getenv("S3_RISK_STREAM_BUCKET", "").strip()
    if not bucket:
        raise ValueError(
            "Risk data path is required. Pass path, set RISK_LATEST_PATH, "
            "or set S3_RISK_STREAM_BUCKET."
        )
    return f"s3://{bucket}/gold_export/risk_latest"


def load_high_risk_regions(path: str | None = None):
    import pandas as pd

    src = resolve_risk_path(path)
    log.info("Reading risk data from %s", src)

    df = pd.read_parquet(src)

    required = {"R_total", "admin_names", "risk_level"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns in risk data: {missing}")

    high_risk = df[df["R_total"] >= HIGH_RISK_THRESHOLD].copy()
    log.info("High-risk regions: %d / %d total", len(high_risk), len(df))
    return high_risk


def _post_slack(text: str) -> None:
    webhook_url = os.getenv("SLACK_WEBHOOK_URL", "").strip()
    if not webhook_url:
        raise ValueError("SLACK_WEBHOOK_URL is required to send Slack notifications.")
    try:
        resp = requests.post(webhook_url, json={"text": text}, timeout=10)
    except requests.RequestException as exc:
        raise RuntimeError(
            f"Slack notification failed. error={type(exc).__name__}"
        ) from None
    if resp.status_code == 200:
        log.info("Slack notification sent.")
    else:
        raise RuntimeError(
            f"Slack notification failed. status={resp.status_code} body={resp.text}"
        )


def send_high_risk_alerts(path: str | None = None) -> int:
    import pandas as pd

    df = load_high_risk_regions(path)

    if df.empty:
        log.info("No high-risk regions detected.")
        _post_slack("✅ *기상 위험 알림* | 현재 위험 지역 없음")
        return 0

    lines = []
    for _, row in df.iterrows():
        emoji      = RISK_EMOJI.get(str(row.get("risk_level", "")), "⚠️")
        admin      = row.get("admin_names", "알 수 없음")
        r_total    = row.get("R_total")
        risk_level = row.get("risk_level", "")
        fcst_ts    = row.get("fcst_ts", "")
        score_str  = f"{r_total:.2f}" if pd.notna(r_total) else "-"
        lines.append(f"{emoji} *{admin}*  |  위험도: `{score_str}` ({risk_level})  |  예보: {fcst_ts}")

    text = "*🚨 기상 위험 지역 알림*\n" + "\n".join(lines)
    _post_slack(text)

    return len(df)


def send_task_failure_alert(context: dict) -> None:
    task_instance = context["task_instance"]
    text = (
        "❌ *기상 위험 파이프라인 실패*\n"
        f"DAG: `{task_instance.dag_id}`\n"
        f"Task: `{task_instance.task_id}`\n"
        f"Run: `{task_instance.run_id}`\n"
        f"<{task_instance.log_url}|Airflow 로그 열기>"
    )
    try:
        _post_slack(text)
    except Exception as exc:
        log.error("Unable to send task failure alert (%s).", type(exc).__name__)


def main():
    path = os.getenv("RISK_LATEST_PATH") or None
    send_high_risk_alerts(path)


if __name__ == "__main__":
    main()
