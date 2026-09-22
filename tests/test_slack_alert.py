import os
import unittest
from unittest.mock import patch

from weather_risk_assessment.alerts.slack_alert import resolve_risk_path


class SlackAlertPathTests(unittest.TestCase):
    def test_explicit_path_needs_no_environment(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(resolve_risk_path("/tmp/risk.parquet"), "/tmp/risk.parquet")

    def test_configured_path_precedes_bucket_default(self):
        environment = {
            "RISK_LATEST_PATH": "/tmp/configured.parquet",
            "S3_RISK_STREAM_BUCKET": "risk-data",
        }
        with patch.dict(os.environ, environment, clear=True):
            self.assertEqual(resolve_risk_path(), "/tmp/configured.parquet")

    def test_bucket_builds_default_export_path(self):
        with patch.dict(
            os.environ, {"S3_RISK_STREAM_BUCKET": "risk-data"}, clear=True
        ):
            self.assertEqual(
                resolve_risk_path(),
                "s3://risk-data/gold_export/risk_latest",
            )

    def test_missing_path_has_actionable_error(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, "RISK_LATEST_PATH"):
                resolve_risk_path()


if __name__ == "__main__":
    unittest.main()
