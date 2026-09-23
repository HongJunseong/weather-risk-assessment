import importlib.util
from pathlib import Path
import tempfile
import unittest

import pandas as pd

from weather_risk_assessment.jobs.export_gold_parquet import (
    parse_args,
    validate_exported_dataset,
)

HAS_PYSPARK = importlib.util.find_spec("pyspark") is not None


class ExportPromotionUnitTests(unittest.TestCase):
    def test_validate_exported_dataset_checks_row_count(self):
        class MockDataFrame:
            def __init__(self, count):
                self._count = count

            def count(self):
                return self._count

        # Valid count
        validate_exported_dataset(MockDataFrame(247), 247, "risk_latest")

        # Row count mismatch
        with self.assertRaisesRegex(ValueError, "expected 247 rows, got 200"):
            validate_exported_dataset(MockDataFrame(200), 247, "risk_latest")

        # Empty dataset
        with self.assertRaisesRegex(ValueError, "output is empty"):
            validate_exported_dataset(MockDataFrame(0), 0, "risk_latest")


@unittest.skipUnless(HAS_PYSPARK, "pyspark is required for export integration test")
class ExportPromotionIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("export-promotion-integration-test")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.session.timeZone", "Asia/Seoul")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_export_preserves_history_and_promotes_to_latest(self):
        from weather_risk_assessment.jobs.export_gold_parquet import main as export_main

        run_dt = "2026092318"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            latest_src = root / "gold" / "risk_latest"
            daily_src = root / "gold" / "risk_daily"

            latest_src.mkdir(parents=True)
            daily_src.mkdir(parents=True)

            # Sample latest delta/parquet data
            latest_df = pd.DataFrame([{
                "dt": run_dt,
                "nx": 60,
                "ny": 127,
                "admin_names": "종로구",
                "fcst_ts": "2026-09-23 18:00:00",
                "R_total": 0.7,
                "R_rain": 0.8,
                "R_heat": 0.2,
                "R_wind": 0.4,
                "R_uv": 0.1,
                "R_typhoon": 0.0,
                "risk_level": "HIGH",
            }])
            latest_df.to_parquet(latest_src / "part-00000.parquet", index=False)

            # Sample daily data
            daily_df = pd.DataFrame([{
                "date": "2026-09-23",
                "admin_names": "종로구",
                "r_total_avg": 0.5,
                "r_total_max": 0.7,
                "max_time": "1800",
                "obs_cnt": 1,
            }])
            daily_df.to_parquet(daily_src / "part-00000.parquet", index=False)

            latest_history = root / "gold_export" / "history" / f"dt={run_dt}" / "risk_latest"
            daily_history = root / "gold_export" / "history" / f"dt={run_dt}" / "risk_daily"
            latest_out = root / "gold_export" / "risk_latest"
            daily_out = root / "gold_export" / "risk_daily"

            argv = [
                "--latest-source", str(latest_src),
                "--daily-source", str(daily_src),
                "--latest-output", str(latest_out),
                "--daily-output", str(daily_out),
                "--latest-history", str(latest_history),
                "--daily-history", str(daily_history),
                "--run_dt", run_dt,
            ]
            export_main(argv)

            # Assert history exists
            self.assertTrue(latest_history.exists())
            self.assertTrue(daily_history.exists())
            history_read = pd.read_parquet(latest_history)
            self.assertEqual(len(history_read), 1)

            # Assert promoted output exists
            self.assertTrue(latest_out.exists())
            self.assertTrue(daily_out.exists())
            promoted_read = pd.read_parquet(latest_out)
            self.assertEqual(len(promoted_read), 1)
            self.assertEqual(promoted_read.iloc[0]["risk_level"], "HIGH")


if __name__ == "__main__":
    unittest.main()
