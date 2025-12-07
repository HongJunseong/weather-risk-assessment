# weather_risk_assessment_streaming/config.py
from dataclasses import dataclass
import os


@dataclass
class KafkaConfig:
    """Kafka 연결 및 토픽 설정"""
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic_risk_latest: str = os.getenv("KAFKA_TOPIC_RISK_LATEST", "weather_risk_latest")


@dataclass
class AwsConfig:
    """AWS S3 / RDS 설정"""
    region: str = os.getenv("AWS_REGION", "ap-northeast-2")

    # S3 (데이터 레이크)
    s3_bucket: str = os.getenv("S3_RISK_STREAM_BUCKET", "")
    s3_prefix: str = os.getenv("S3_RISK_STREAM_PREFIX", "weather_risk_stream/")

    # RDS (옵션: 스트리밍 테이블)
    rds_jdbc_url: str = os.getenv("RDS_JDBC_URL", "")  # e.g. jdbc:postgresql://host:5432/dbname
    rds_user: str = os.getenv("RDS_USER", "")
    rds_password: str = os.getenv("RDS_PASSWORD", "")
