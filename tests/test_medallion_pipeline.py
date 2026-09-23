import importlib.util
from pathlib import Path
import tempfile
import unittest

import pandas as pd

from weather_risk_assessment.contracts.medallion import validate_spark_frame
from weather_risk_assessment.jobs.build_gold_risk_daily import build_daily
from weather_risk_assessment.jobs.build_gold_risk_latest import build_latest
from weather_risk_assessment.jobs.export_gold_parquet import validate_export_sources
from weather_risk_assessment.jobs.build_silver_from_bronze import build_silver_frame


HAS_PYSPARK = importlib.util.find_spec("pyspark") is not None
RUN_DT = "2025010207"


def _write_bronze(root: Path, dataset: str, rows: list[dict]) -> None:
    directory = root / dataset / f"dt={RUN_DT}"
    directory.mkdir(parents=True)
    pd.DataFrame(rows).to_parquet(directory / "part-00000.parquet", index=False)


@unittest.skipUnless(HAS_PYSPARK, "pyspark is required for integration tests")
class MedallionPipelineIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("medallion-pipeline-integration-test")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_sample_bronze_builds_valid_silver_and_gold(self):
        keys = [
            {"nx": 60, "ny": 127, "fcstDate": "20250102", "fcstTime": "0800"},
            {"nx": 60, "ny": 127, "fcstDate": "20250102", "fcstTime": "0900"},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "bronze"
            _write_bronze(
                root,
                "ultra_shortfcst",
                [
                    {**key, "baseDate": "20250102", "baseTime": "0630", "T1H": temp,
                     "RN1": rain, "REH": 70, "WSD": wind, "PTY": 0, "SKY": 2}
                    for key, temp, rain, wind in zip(keys, [25, 32], [0, 8], [2, 10])
                ],
            )
            _write_bronze(
                root,
                "ultra_nowcast",
                [
                    {**key, "baseDate": "20250102", "baseTime": "0700", "T1H": temp,
                     "RN1": rain, "REH": 70, "WSD": wind}
                    for key, temp, rain, wind in zip(keys, [25, 32], [0, 8], [2, 10])
                ],
            )
            _write_bronze(
                root,
                "short_fcst",
                [{**key, "PCP": rain, "POP": pop} for key, rain, pop in zip(keys, [0, 8], [10, 80])],
            )
            _write_bronze(
                root,
                "typhoon",
                [{**key, "TY_DISTANCE_KM": distance, "TY_MAX_WIND": 25, "TY_WARNING": warning}
                 for key, distance, warning in zip(keys, [900, 250], [0, 1])],
            )
            _write_bronze(
                root,
                "uv",
                [{**key, "UVI": uvi} for key, uvi in zip(keys, [2, 8])],
            )

            admin_map = Path(tmp) / "admin_centroids.csv"
            pd.DataFrame([{"nx": 60, "ny": 127, "admin_name": "서울특별시 종로구"}]).to_csv(
                admin_map, index=False
            )

            silver = build_silver_frame(self.spark, str(root), RUN_DT, str(admin_map))
            latest = build_latest(silver)
            daily = build_daily(silver)

            self.assertEqual(silver.count(), 2)
            self.assertEqual(latest.count(), 1)
            daily_row = daily.collect()[0]
            self.assertEqual(daily_row["obs_cnt"], 2)
            self.assertEqual(daily_row["max_time"], "0900")
            self.assertEqual(validate_spark_frame("silver_risk_enriched", silver).issues, [])
            self.assertEqual(validate_spark_frame("gold_risk_latest", latest).issues, [])
            self.assertEqual(validate_spark_frame("gold_risk_daily", daily).issues, [])
            self.assertEqual(validate_export_sources(latest, daily, RUN_DT), (1, 1))
            with self.assertRaisesRegex(ValueError, "Gold export is stale"):
                validate_export_sources(latest, daily, "2025010210")



if __name__ == "__main__":
    unittest.main()
