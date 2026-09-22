import os
import unittest
from unittest.mock import Mock, patch

from weather_risk_assessment.alerts.slack_alert import _post_slack, resolve_risk_path


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


class SlackDeliveryTests(unittest.TestCase):
    @patch("weather_risk_assessment.alerts.slack_alert.requests.post")
    def test_success_posts_with_timeout(self, post):
        post.return_value = Mock(status_code=200)

        environment = {"SLACK_WEBHOOK_URL": "https://hooks.example/test"}
        with patch.dict(os.environ, environment, clear=True):
            _post_slack("alert")

        post.assert_called_once_with(
            "https://hooks.example/test", json={"text": "alert"}, timeout=10
        )

    def test_missing_webhook_fails(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, "SLACK_WEBHOOK_URL"):
                _post_slack("alert")

    @patch("weather_risk_assessment.alerts.slack_alert.requests.post")
    def test_http_error_fails_without_exposing_webhook(self, post):
        post.return_value = Mock(status_code=500, text="server error")

        environment = {"SLACK_WEBHOOK_URL": "https://hooks.example/secret"}
        with patch.dict(os.environ, environment, clear=True):
            with self.assertRaisesRegex(RuntimeError, "status=500") as raised:
                _post_slack("alert")

        self.assertNotIn("secret", str(raised.exception))


if __name__ == "__main__":
    unittest.main()
