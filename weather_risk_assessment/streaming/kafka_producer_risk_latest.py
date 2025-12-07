# weather_risk_assessment_streaming/kafka_producer_risk_latest.py
from __future__ import annotations

import os
import json
import logging
from pathlib import Path
from typing import Any, Dict
import hashlib

import pandas as pd
from kafka import KafkaProducer

from weather_risk_assessment.streaming.config import KafkaConfig

log = logging.getLogger(__name__)
if not log.handlers:
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")

# 해시 저장 위치 (컨테이너 기준 /opt/airflow/data)
HASH_PATH = Path("/opt/airflow/data/last_risk_latest_hash.txt")


# =========================
#  해시 관련 유틸
# =========================
def compute_payload_hash(records: list[dict]) -> str:
    """
    risk_latest 레코드 리스트(딕셔너리 리스트)를 받아서
    내용 기반 SHA-256 해시 문자열을 반환
    """
    # 정렬된 JSON 문자열로 만들어서 순서 차이로 인한 오차 방지
    s = json.dumps(records, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def has_same_as_last_hash(new_hash: str) -> bool:
    """저장된 마지막 해시와 비교해서 동일한지 여부 반환"""
    if HASH_PATH.exists():
        last_hash = HASH_PATH.read_text().strip()
    else:
        last_hash = ""
    return new_hash == last_hash


def save_hash(new_hash: str) -> None:
    HASH_PATH.parent.mkdir(parents=True, exist_ok=True)
    HASH_PATH.write_text(new_hash)


# =========================
#  risk_latest 로딩
# =========================
def _default_risk_latest_path() -> Path:
    """
    v1 프로젝트 구조를 기준으로 risk_latest.parquet 기본 경로를 추론.
    (Airflow 컨테이너 기준: /opt/airflow/weather_risk_assessment/data/risk_latest.parquet)
    """
    project_root = Path(os.getenv("PROJECT_DRE_ROOT", "/opt/airflow")).resolve()
    return project_root / "weather_risk_assessment" / "data" / "risk_latest.parquet"


def load_risk_latest(path: str | None = None) -> pd.DataFrame:
    risk_path = Path(path) if path else _default_risk_latest_path()
    if not risk_path.exists():
        raise FileNotFoundError(f"risk_latest.parquet not found: {risk_path}")

    log.info("Loading risk_latest from %s", risk_path)
    df = pd.read_parquet(risk_path)

    keep_cols = [
        "nx", "ny", "admin_names",
        "baseDate", "fcstDate", "fcstTime",
        "R_rain", "R_heat", "R_wind", "R_uv", "R_typhoon", "R_total",
    ]
    existing = [c for c in keep_cols if c in df.columns]
    df = df[existing].copy()
    return df


# =========================
#  row → Kafka 메시지 변환
# =========================
def row_to_message(row: pd.Series) -> Dict[str, Any]:
    """pandas row → JSON 직렬화 가능한 dict로 변환"""
    def _to_python(v):
        # pandas의 NaN, Int64 등 직렬화 안전하게 변환
        if pd.isna(v):
            return None
        if hasattr(v, "item"):
            try:
                return v.item()
            except Exception:
                return v
        return v

    payload = {k: _to_python(v) for k, v in row.items()}

    # fcst_ts 같은 편의 필드 추가
    if "fcstDate" in row and "fcstTime" in row:
        payload["fcst_ts"] = f"{row['fcstDate']} {row['fcstTime']}"

    return payload


# =========================
#  메인 전송 로직
# =========================
def push_risk_latest_to_kafka(risk_latest_path: str | None = None) -> int:
    """
    risk_latest.parquet의 모든 row를 Kafka 토픽으로 전송.
    반환값: 전송한 메시지 개수
    """
    cfg = KafkaConfig()
    df = load_risk_latest(risk_latest_path)

    if df.empty:
        log.warning("risk_latest DataFrame is empty. Kafka 전송 생략.")
        return 0

    # ★ 1) 먼저 모든 row를 dict 형태의 records로 변환
    records: list[Dict[str, Any]] = []
    for _, row in df.iterrows():
        msg = row_to_message(row)
        records.append(msg)

    # ★ 2) 내용 기반 해시 계산
    payload_hash = compute_payload_hash(records)

    # ★ 3) 이전 배치와 내용이 완전히 같으면 Kafka 전송 스킵
    if has_same_as_last_hash(payload_hash):
        log.info("[risk_latest] 내용이 이전 배치와 동일 -> Kafka 전송 생략")
        return 0

    # ★ 4) 다를 때만 Kafka Producer 생성 + 전송
    producer = KafkaProducer(
        bootstrap_servers=cfg.bootstrap_servers.split(","),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )

    count = 0
    log.info("Start sending records to Kafka topic=%s", cfg.topic_risk_latest)

    for msg in records:
        producer.send(cfg.topic_risk_latest, value=msg)
        count += 1

    producer.flush()
    producer.close()

    # ★ 5) 성공적으로 전송한 후 해시 파일 업데이트
    save_hash(payload_hash)

    log.info(
        "Finished sending %d messages to Kafka (새 payload hash 저장 완료)", count
    )
    return count


def main():
    risk_latest_path = os.getenv("RISK_LATEST_PATH", None)
    push_risk_latest_to_kafka(risk_latest_path)


if __name__ == "__main__":
    main()
