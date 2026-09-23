import unittest

import pandas as pd

from weather_risk_assessment.risk.config import compute_r_total
from weather_risk_assessment.risk.rain_risk import _coerce_mm_series
from weather_risk_assessment.risk.uv_risk import compute_uv_risk
from weather_risk_assessment.utils.latlon_to_grid import latlon_to_grid


class RiskTests(unittest.TestCase):
    def test_total_baseline_and_peak(self):
        frame = pd.DataFrame([
            [0, 0, 0, 0, 0], [1, 1, 1, 1, 1], [1, 0, 0, 0, 0],
        ], columns=['R_rain', 'R_heat', 'R_wind', 'R_uv', 'R_typhoon'])
        result = compute_r_total(frame)
        for actual, expected in zip(result, [0, 1, 0.784]):
            self.assertAlmostEqual(actual, expected)

    def test_missing_indicator_remains_missing(self):
        frame = pd.DataFrame({'R_rain': [None], 'R_heat': [0.5], 'R_wind': [0.5],
                              'R_uv': [0.5], 'R_typhoon': [0.5]}, dtype=float)
        self.assertTrue(pd.isna(compute_r_total(frame).iloc[0]))

    def test_rain_units_and_missing_values(self):
        values = pd.Series(['강수없음', '1mm 미만', '5.0mm', None, '-2mm'])
        self.assertEqual(_coerce_mm_series(values).tolist(), [0, 0.5, 5, 0, 0])

    def test_uv_missing_reading_uses_weather_fallback(self):
        frame = pd.DataFrame({
            "UVI": [None, 6.0],
            "SKY": [1, 1],
            "TMP": [30.0, 30.0],
            "fcstTime": ["1200", "1200"],
        })
        result = compute_uv_risk(frame)
        fallback = compute_uv_risk(frame.drop(columns="UVI"))
        direct = compute_uv_risk(pd.DataFrame({"UVI": [6.0]}))

        self.assertAlmostEqual(result.iloc[0], fallback.iloc[0])
        self.assertGreater(result.iloc[0], 0)
        self.assertAlmostEqual(result.iloc[1], direct.iloc[0])

    def test_seoul_grid(self):
        self.assertEqual(latlon_to_grid(37.5665, 126.9780), (60, 127))
