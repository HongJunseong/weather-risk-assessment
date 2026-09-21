import unittest

from weather_risk_assessment.collectors.short_forecast import _latest_issue_time
from weather_risk_assessment.collectors.typhoon_forecast import _derive_target_times
from weather_risk_assessment.collectors.ultra_nowcast_shortfcst import (
    _iter_fcst_candidates,
    _iter_ncst_candidates,
)
from weather_risk_assessment.utils.run_time import normalize_run_dt, resolve_run_time


class RunTimeTests(unittest.TestCase):
    def test_valid_run_dt_is_kst_and_hour_aligned(self):
        run_time = resolve_run_time("2024022907")
        self.assertEqual(run_time.format("YYYY-MM-DD HH:mm Z"), "2024-02-29 07:00 +09:00")
        self.assertEqual(normalize_run_dt("2024022907"), "2024022907")

    def test_invalid_run_dt_is_rejected(self):
        for value in ("2024-02-29 07", "2024023007", "202402290730"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                resolve_run_time(value)

    def test_ultra_candidates_are_derived_from_run_time(self):
        reference = resolve_run_time("2025010203")
        self.assertEqual(
            list(_iter_ncst_candidates(reference, tries=2)),
            [("20250102", "0200"), ("20250102", "0100")],
        )
        self.assertEqual(
            list(_iter_fcst_candidates(reference, tries=2)),
            [("20250102", "0230"), ("20250102", "0130")],
        )

    def test_short_forecast_uses_previous_published_issue(self):
        self.assertEqual(_latest_issue_time(resolve_run_time("2025010208")), ("20250102", "0500"))

    def test_typhoon_fallback_targets_end_at_run_time(self):
        targets = _derive_target_times(None, resolve_run_time("2025010208"))
        self.assertEqual(len(targets), 6)
        self.assertEqual(targets[0].strftime("%Y%m%d%H"), "2025010203")
        self.assertEqual(targets[-1].strftime("%Y%m%d%H"), "2025010208")
        self.assertIsNone(targets.tz)
