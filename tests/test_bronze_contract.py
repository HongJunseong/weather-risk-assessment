from contextlib import redirect_stdout
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import pandas as pd

from weather_risk_assessment.contracts.bronze import (
    CONTRACTS,
    validate_bronze_directory,
    validate_bronze_frame,
)
from weather_risk_assessment.scripts.upload_bronze_to_s3 import main as upload_bronze


def valid_frames():
    common_forecast = {
        "fcstDate": ["20250102"],
        "fcstTime": ["0800"],
        "nx": [60],
        "ny": [127],
    }
    return {
        "ultra_nowcast": pd.DataFrame(
            {
                "baseDate": ["20250102"],
                "baseTime": ["0700"],
                "nx": [60],
                "ny": [127],
                "T1H": [2.0],
            }
        ),
        "ultra_shortfcst": pd.DataFrame(
            {
                **common_forecast,
                "baseDate": ["20250102"],
                "baseTime": ["0730"],
                "T1H": [2.4],
            }
        ),
        "short_fcst": pd.DataFrame({**common_forecast, "TMP": [2.0]}),
        "typhoon": pd.DataFrame(
            {
                **common_forecast,
                "TY_DISTANCE_KM": [850.0],
                "TY_MAX_WIND": [30.0],
                "TY_WARNING": [0.0],
            }
        ),
        "uv": pd.DataFrame({**common_forecast, "UVI": [1]}),
    }


class BronzeContractTests(unittest.TestCase):
    @staticmethod
    def write_valid_run(directory):
        for dataset, frame in valid_frames().items():
            frame.to_parquet(directory / CONTRACTS[dataset].filename, index=False)

    def test_valid_run_directory_passes(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            self.write_valid_run(directory)
            report = validate_bronze_directory(directory)
            self.assertEqual(report.errors, [])
            self.assertEqual(report.warnings, [])

    def test_missing_file_fails_the_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            report = validate_bronze_directory(tmp)
            self.assertEqual(len(report.errors), len(CONTRACTS))
            self.assertTrue(all(issue.code == "missing_file" for issue in report.errors))

    def test_duplicate_forecast_key_is_rejected(self):
        frame = pd.concat(
            [valid_frames()["short_fcst"]] * 2,
            ignore_index=True,
        )
        report = validate_bronze_frame("short_fcst", frame)
        self.assertEqual([issue.code for issue in report.errors], ["duplicate_keys"])

    def test_invalid_forecast_time_is_rejected(self):
        frame = valid_frames()["uv"]
        frame.loc[0, "fcstTime"] = "2561"
        report = validate_bronze_frame("uv", frame)
        self.assertEqual([issue.code for issue in report.errors], ["invalid_fcst_time"])

    def test_source_time_older_than_fallback_window_is_rejected(self):
        frame = valid_frames()["ultra_shortfcst"]
        frame.loc[0, ["baseDate", "baseTime"]] = ["20250102", "0300"]

        report = validate_bronze_frame(
            "ultra_shortfcst", frame, run_dt="2025010208"
        )

        self.assertEqual([issue.code for issue in report.errors], ["stale_source_time"])

    def test_four_hour_old_source_is_within_fallback_window(self):
        frame = valid_frames()["ultra_nowcast"]
        frame.loc[0, ["baseDate", "baseTime"]] = ["20250102", "0400"]

        report = validate_bronze_frame(
            "ultra_nowcast", frame, run_dt="2025010208"
        )

        self.assertEqual(report.errors, [])

    def test_empty_optional_dataset_warns(self):
        columns = list(CONTRACTS["uv"].required_columns)
        report = validate_bronze_frame("uv", pd.DataFrame(columns=columns))
        self.assertEqual(report.errors, [])
        self.assertEqual([issue.code for issue in report.warnings], ["empty"])

    @patch.dict("os.environ", {"S3_RISK_STREAM_BUCKET": "test-risk-bucket"})
    @patch("weather_risk_assessment.scripts.upload_bronze_to_s3.boto3.client")
    def test_upload_uses_validated_run_partition(self, client_factory):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "dt=2025010208"
            run_dir.mkdir()
            self.write_valid_run(run_dir)
            client = Mock()
            client_factory.return_value = client

            with redirect_stdout(io.StringIO()):
                upload_bronze(run_dt="2025010208", sink_dir=run_dir)

            self.assertEqual(client.upload_file.call_count, len(CONTRACTS))
            keys = [call.args[2] for call in client.upload_file.call_args_list]
            self.assertTrue(all("/dt=2025010208/" in key for key in keys))

    @patch.dict("os.environ", {"S3_RISK_STREAM_BUCKET": "test-risk-bucket"})
    @patch("weather_risk_assessment.scripts.upload_bronze_to_s3.boto3.client")
    def test_contract_failure_happens_before_s3_client_creation(self, client_factory):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "dt=2025010208"
            run_dir.mkdir()
            with redirect_stdout(io.StringIO()), self.assertRaises(ValueError):
                upload_bronze(run_dt="2025010208", sink_dir=run_dir)
            client_factory.assert_not_called()

    @patch.dict("os.environ", {"S3_RISK_STREAM_BUCKET": "test-risk-bucket"})
    @patch("weather_risk_assessment.scripts.upload_bronze_to_s3.boto3.client")
    def test_stale_source_fails_before_s3_client_creation(self, client_factory):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "dt=2025010208"
            run_dir.mkdir()
            self.write_valid_run(run_dir)
            stale = valid_frames()["ultra_shortfcst"]
            stale.loc[0, ["baseDate", "baseTime"]] = ["20250102", "0300"]
            stale.to_parquet(run_dir / CONTRACTS["ultra_shortfcst"].filename, index=False)

            with redirect_stdout(io.StringIO()), self.assertRaisesRegex(
                ValueError, "stale_source_time"
            ):
                upload_bronze(run_dt="2025010208", sink_dir=run_dir)

            client_factory.assert_not_called()
