import json
from pathlib import Path
import unittest

import pandas as pd

from weather_risk_assessment.collectors.short_forecast import (
    _parse_json_safely,
    _postprocess_wide,
)
from weather_risk_assessment.collectors.typhoon_forecast import _parse_track_items
from weather_risk_assessment.collectors.ultra_nowcast_shortfcst import (
    _parse_response,
    normalize,
)
from weather_risk_assessment.collectors.uv_forecast import _parse_h_offsets


FIXTURES = Path(__file__).parent / "fixtures" / "kma"


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload
        self.text = json.dumps(payload, ensure_ascii=False)
        self.status_code = 200
        self.headers = {"content-type": "application/json"}

    def json(self):
        return self._payload

    def raise_for_status(self):
        return None


def load_fixture(name):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


class CollectorFixtureTests(unittest.TestCase):
    def test_ultra_response_normalizes_to_one_forecast_row(self):
        response = FakeResponse(load_fixture("ultra_shortfcst_response.json"))
        wide = normalize(pd.DataFrame(_parse_response(response)))
        self.assertEqual(len(wide), 1)
        self.assertEqual(wide.loc[0, "fcstTime"], "0800")
        self.assertEqual(float(wide.loc[0, "T1H"]), 2.4)
        self.assertEqual(wide.loc[0, "RN1"], "강수없음")

    def test_short_forecast_parses_precipitation_text(self):
        response = FakeResponse(load_fixture("short_forecast_response.json"))
        payload = _parse_json_safely(response)
        raw = pd.DataFrame(payload["response"]["body"]["items"]["item"])
        wide = _postprocess_wide(raw, admin_code=11110)
        self.assertEqual(len(wide), 1)
        self.assertEqual(wide.iloc[0]["PCP"], 0.5)
        self.assertEqual(wide.iloc[0]["admin_code"], 11110)

    def test_typhoon_items_have_hourly_track_inputs(self):
        track = _parse_track_items(load_fixture("typhoon_forecast_items.json"))
        self.assertEqual(len(track), 2)
        self.assertEqual(track.iloc[-1]["wind"], 30)
        self.assertEqual(track.iloc[0]["time"].strftime("%Y%m%d%H%M"), "202501020600")

    def test_uv_offsets_are_numeric(self):
        offsets = _parse_h_offsets(load_fixture("uv_forecast_item.json"))
        self.assertEqual(offsets, {0: 0.0, 3: 1.0, 6: 2.0, 9: 3.0})
