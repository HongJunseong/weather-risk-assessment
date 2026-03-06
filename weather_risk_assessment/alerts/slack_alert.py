from __future__ import annotations

import os
import logging

import requests
import pandas as pd

log = logging.getLogger(__name__)
if not log.handlers:
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")

BUCKET = os.environ["S3_RISK_STREAM_BUCKET"]
DEFAULT_S3_PATH = f"s3://{BUCKET}/gold_export/risk_latest"

HIGH_RISK_THRESHOLD = 0.6

RISK_EMOJI = {
    "VERY_HIGH": "🔴",
    "HIGH":      "🟠",
}


def load_high_risk_regions(path: str | None = None) -> pd.DataFrame:
    src = path or os.getenv("RISK_LATEST_PATH") or DEFAULT_S3_PATH
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
    webhook_url = os.getenv("SLACK_WEBHOOK_URL", "")
    if not webhook_url:
        log.info("SLACK_WEBHOOK_URL not set. Skipping Slack notification.")
        return
    resp = requests.post(webhook_url, json={"text": text}, timeout=10)
    if resp.status_code == 200:
        log.info("Slack notification sent.")
    else:
        log.warning("Slack notification failed. status=%s body=%s", resp.status_code, resp.text)


def send_high_risk_alerts(path: str | None = None) -> int:
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


def main():
    path = os.getenv("RISK_LATEST_PATH") or None
    send_high_risk_alerts(path)


if __name__ == "__main__":
    main()
