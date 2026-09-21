"""Runtime configuration shared by Spark command-line jobs."""

from __future__ import annotations

import os
from typing import Any


BUCKET_ENV = "S3_RISK_STREAM_BUCKET"


def resolve_bucket(value: str | None = None) -> str:
    """Return an explicit or environment-provided S3 bucket name."""
    bucket = (value or os.getenv(BUCKET_ENV, "")).strip()
    if not bucket:
        raise ValueError(
            f"S3 bucket is required. Pass --bucket or set {BUCKET_ENV}."
        )
    if "://" in bucket or "/" in bucket:
        raise ValueError("S3 bucket must be a name without a scheme or path")
    return bucket


def s3a_uri(bucket: str, *parts: str) -> str:
    """Build an s3a URI from a bucket name and path segments."""
    clean_parts = [part.strip("/") for part in parts if part.strip("/")]
    suffix = "/".join(clean_parts)
    return f"s3a://{bucket}/{suffix}" if suffix else f"s3a://{bucket}"


def resolve_storage_paths(
    bucket_value: str | None,
    provided: dict[str, str],
    defaults: dict[str, tuple[str, ...]],
) -> dict[str, str]:
    """Fill missing storage paths from a bucket while preserving explicit URIs."""
    missing = [name for name, value in provided.items() if not value]
    if not missing:
        return provided.copy()

    bucket = resolve_bucket(bucket_value)
    return {
        name: value or s3a_uri(bucket, *defaults[name])
        for name, value in provided.items()
    }


def create_spark_session(app_name: str) -> Any:
    """Create a Delta-enabled Spark session only when a job starts."""
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
