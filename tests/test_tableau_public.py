from pathlib import Path
import tempfile
import unittest

import pandas as pd

from weather_risk_assessment.scripts.generate_tableau_sample import generate_sample
from weather_risk_assessment.scripts.prepare_tableau_public import prepare_tableau_csv


class TableauPublicExportTests(unittest.TestCase):
    def test_generates_repeatable_multi_period_sample(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            centroids_path = root / "centroids.csv"
            output_path = root / "sample.csv"
            pd.DataFrame(
                [
                    {"admin_name": "서울특별시 종로구", "lat": 37.57, "lon": 126.98},
                    {"admin_name": "서울특별시 중구", "lat": 37.56, "lon": 126.99},
                ]
            ).to_csv(centroids_path, index=False)

            generate_sample(centroids_path, output_path, periods=3)

            sample = pd.read_csv(output_path)
            self.assertEqual(len(sample), 6)
            self.assertEqual(set(sample["data_status"]), {"SYNTHETIC_DEMO"})
            self.assertTrue(sample["risk_score"].between(0, 1).all())

    def test_prepares_public_csv_and_expands_shared_grid_regions(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            risk_path = root / "risk.parquet"
            centroids_path = root / "centroids.csv"
            output_path = root / "tableau.csv"
            pd.DataFrame(
                [
                    {
                        "admin_names": "서울특별시 종로구|서울특별시 중구",
                        "fcst_ts": "2025-01-02 08:00:00",
                        "nx": 60,
                        "ny": 127,
                        "R_total": 0.7,
                        "R_rain": 0.8,
                        "R_heat": 0.2,
                        "R_wind": 0.4,
                        "R_uv": 0.1,
                        "R_typhoon": 0.0,
                        "risk_level": "HIGH",
                    }
                ]
            ).to_parquet(risk_path, index=False)
            pd.DataFrame(
                [
                    {
                        "admin_name": "서울특별시 종로구",
                        "nx": 60,
                        "ny": 127,
                        "lat": 37.57,
                        "lon": 126.98,
                    },
                    {
                        "admin_name": "서울특별시 중구",
                        "nx": 60,
                        "ny": 127,
                        "lat": 37.56,
                        "lon": 126.99,
                    },
                    {
                        "admin_name": "서울특별시 중구",
                        "nx": 98,
                        "ny": 76,
                        "lat": 35.10,
                        "lon": 129.03,
                    },
                ]
            ).to_csv(centroids_path, index=False)

            result = prepare_tableau_csv(risk_path, centroids_path, output_path)

            self.assertEqual(result, output_path)
            exported = pd.read_csv(output_path)
            self.assertEqual(len(exported), 2)
            self.assertEqual(set(exported["latitude"]), {37.57, 37.56})
            self.assertEqual(
                list(exported.columns),
                [
                    "admin_name",
                    "forecast_time",
                    "risk_score",
                    "rain_risk",
                    "heat_risk",
                    "wind_risk",
                    "uv_risk",
                    "typhoon_risk",
                    "risk_level",
                    "latitude",
                    "longitude",
                ],
            )

    def test_rejects_missing_risk_columns(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            risk_path = root / "risk.parquet"
            centroids_path = root / "centroids.csv"
            pd.DataFrame([{"admin_names": "서울특별시 종로구"}]).to_parquet(
                risk_path, index=False
            )
            pd.DataFrame(
                [
                    {
                        "admin_name": "서울특별시 종로구",
                        "nx": 60,
                        "ny": 127,
                        "lat": 37.57,
                        "lon": 126.98,
                    }
                ]
            ).to_csv(centroids_path, index=False)

            with self.assertRaisesRegex(ValueError, "Missing columns in risk data"):
                prepare_tableau_csv(risk_path, centroids_path, root / "out.csv")


if __name__ == "__main__":
    unittest.main()
