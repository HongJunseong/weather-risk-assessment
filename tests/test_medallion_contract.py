import unittest

import pandas as pd

from weather_risk_assessment.contracts.medallion import validate_frame


RISK_VALUES = {
    "R_rain": 0.2,
    "R_heat": 0.3,
    "R_wind": 0.4,
    "R_uv": 0.5,
    "R_typhoon": 0.1,
    "R_total": 0.5,
}


def silver_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "nx": 60,
                "ny": 127,
                "fcstDate": "20250102",
                "fcstTime": "0800",
                "dt": "2025010207",
                "fcst_ts": "2025-01-02 08:00:00",
                "admin_names": "서울특별시 종로구",
                **RISK_VALUES,
            }
        ]
    )


def latest_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "admin_names": "서울특별시 종로구",
                "fcst_ts": "2025-01-02 08:00:00",
                "dt": "2025010207",
                "risk_level": "MED",
                **RISK_VALUES,
            }
        ]
    )


def daily_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": "2025-01-02",
                "admin_names": "서울특별시 종로구",
                "r_total_avg": 0.4,
                "r_total_max": 0.7,
                "obs_cnt": 3,
                "max_time": "1400",
            }
        ]
    )


class MedallionContractTests(unittest.TestCase):
    def test_valid_silver_frame_passes(self):
        self.assertEqual(validate_frame("silver_risk_enriched", silver_frame()).issues, [])

    def test_silver_requires_output_schema(self):
        frame = silver_frame().drop(columns=["R_total"])
        report = validate_frame("silver_risk_enriched", frame)
        self.assertEqual([issue.code for issue in report.issues], ["missing_columns"])

    def test_silver_rejects_duplicate_forecast_keys(self):
        frame = pd.concat([silver_frame(), silver_frame()], ignore_index=True)
        report = validate_frame("silver_risk_enriched", frame)
        self.assertIn("duplicate_keys", [issue.code for issue in report.issues])

    def test_silver_rejects_out_of_range_risk(self):
        frame = silver_frame()
        frame.loc[0, "R_wind"] = 1.1
        report = validate_frame("silver_risk_enriched", frame)
        self.assertEqual([issue.code for issue in report.issues], ["invalid_score"])

    def test_silver_rejects_invalid_partition_and_forecast_time(self):
        frame = silver_frame()
        frame.loc[0, "dt"] = "2025139907"
        frame.loc[0, "fcstTime"] = "2561"
        report = validate_frame("silver_risk_enriched", frame)
        self.assertEqual(
            {issue.code for issue in report.issues},
            {"invalid_forecast_time", "invalid_run_dt"},
        )

    def test_valid_latest_frame_passes(self):
        self.assertEqual(validate_frame("gold_risk_latest", latest_frame()).issues, [])

    def test_latest_rejects_duplicate_region_and_wrong_level(self):
        frame = pd.concat([latest_frame(), latest_frame()], ignore_index=True)
        frame["risk_level"] = "LOW"
        report = validate_frame("gold_risk_latest", frame)
        self.assertEqual(
            {issue.code for issue in report.issues},
            {"duplicate_keys", "risk_level_mismatch"},
        )

    def test_valid_daily_frame_passes(self):
        self.assertEqual(validate_frame("gold_risk_daily", daily_frame()).issues, [])

    def test_daily_rejects_invalid_aggregate(self):
        frame = daily_frame()
        frame.loc[0, "r_total_avg"] = 0.8
        frame.loc[0, "r_total_max"] = 0.7
        frame.loc[0, "obs_cnt"] = 0
        frame.loc[0, "max_time"] = "2460"
        report = validate_frame("gold_risk_daily", frame)
        self.assertEqual(
            {issue.code for issue in report.issues},
            {"invalid_period", "invalid_obs_count", "invalid_aggregate"},
        )


if __name__ == "__main__":
    unittest.main()
