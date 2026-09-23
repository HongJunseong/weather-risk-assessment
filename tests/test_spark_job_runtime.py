import importlib
import os
import sys
import unittest
from unittest.mock import patch

from weather_risk_assessment.jobs.spark_runtime import (
    resolve_bucket,
    resolve_storage_paths,
    s3a_uri,
)


SPARK_JOB_MODULES = (
    "weather_risk_assessment.jobs.build_silver_from_bronze",
    "weather_risk_assessment.jobs.build_gold_risk_latest",
    "weather_risk_assessment.jobs.build_gold_risk_daily",
    "weather_risk_assessment.jobs.export_gold_parquet",
)


class SparkJobRuntimeTests(unittest.TestCase):
    def test_jobs_import_without_spark_or_environment(self):
        with patch.dict(os.environ, {}, clear=True):
            for module_name in SPARK_JOB_MODULES:
                sys.modules.pop(module_name, None)
                module = importlib.import_module(module_name)
                self.assertTrue(callable(module.main))

    def test_bucket_prefers_explicit_value(self):
        with patch.dict(os.environ, {"S3_RISK_STREAM_BUCKET": "environment-bucket"}):
            self.assertEqual(resolve_bucket("argument-bucket"), "argument-bucket")

    def test_bucket_uses_environment_fallback(self):
        with patch.dict(os.environ, {"S3_RISK_STREAM_BUCKET": "environment-bucket"}):
            self.assertEqual(resolve_bucket(), "environment-bucket")

    def test_missing_bucket_has_actionable_error(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, "--bucket"):
                resolve_bucket()

    def test_bucket_rejects_uri_or_path(self):
        for value in ("s3://risk-data", "risk-data/prefix"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                resolve_bucket(value)

    def test_s3a_uri_normalizes_path_segments(self):
        self.assertEqual(
            s3a_uri("risk-data", "/silver/", "risk_latest"),
            "s3a://risk-data/silver/risk_latest",
        )

    def test_explicit_local_paths_do_not_require_bucket(self):
        with patch.dict(os.environ, {}, clear=True):
            paths = resolve_storage_paths(
                "",
                {"source": "file:///tmp/source", "output": "file:///tmp/output"},
                {"source": ("silver",), "output": ("gold",)},
            )
        self.assertEqual(
            paths,
            {"source": "file:///tmp/source", "output": "file:///tmp/output"},
        )

    def test_missing_paths_use_bucket_defaults(self):
        paths = resolve_storage_paths(
            "risk-data",
            {"source": "", "output": "file:///tmp/output"},
            {"source": ("silver",), "output": ("gold",)},
        )
        self.assertEqual(paths["source"], "s3a://risk-data/silver")
        self.assertEqual(paths["output"], "file:///tmp/output")

    def test_export_gold_parquet_parses_history_args(self):
        from weather_risk_assessment.jobs.export_gold_parquet import parse_args

        args = parse_args(["--run_dt", "2026092318", "--bucket", "my-bucket"])
        self.assertEqual(args.run_dt, "2026092318")
        self.assertEqual(args.bucket, "my-bucket")
        self.assertEqual(args.latest_history, "")
        self.assertEqual(args.daily_history, "")


if __name__ == "__main__":
    unittest.main()
