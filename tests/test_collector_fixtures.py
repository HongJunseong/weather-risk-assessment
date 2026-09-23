import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd
import pendulum

from weather_risk_assessment.collectors import short_forecast, uv_forecast
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

    def test_kma_empty_items_are_a_valid_empty_response(self):
        response = FakeResponse(load_fixture("empty_items_response.json"))
        self.assertEqual(_parse_response(response), [])

    def test_kma_no_data_response_is_distinguishable(self):
        response = FakeResponse(load_fixture("no_data_response.json"))
        with self.assertRaisesRegex(RuntimeError, "NO_DATA"):
            _parse_response(response)

    def test_kma_authentication_error_is_not_treated_as_empty(self):
        response = FakeResponse(load_fixture("api_error_response.json"))
        with self.assertRaisesRegex(RuntimeError, "SERVICE KEY"):
            _parse_response(response)

    def test_short_forecast_parses_precipitation_text(self):
        response = FakeResponse(load_fixture("short_forecast_response.json"))
        payload = _parse_json_safely(response)
        raw = pd.DataFrame(payload["response"]["body"]["items"]["item"])
        wide = _postprocess_wide(raw, admin_code=11110)
        self.assertEqual(len(wide), 1)
        self.assertEqual(wide.iloc[0]["PCP"], 0.5)
        self.assertEqual(wide.iloc[0]["admin_code"], 11110)

    def test_short_forecast_accepts_templated_string_output_path(self):
        items = load_fixture("short_forecast_response.json")["response"]["body"]["items"]["item"]
        targets = pd.DataFrame({"fcstDate": ["20250102"], "fcstTime": ["0800"]})
        with tempfile.TemporaryDirectory() as tmp:
            call_list = Path(tmp) / "call_list.csv"
            call_list.write_text("nx,ny,admin_code\n60,127,11110\n", encoding="utf-8")
            output = Path(tmp) / "dt=2025010208" / "short_fcst.parquet"
            with (
                patch.object(short_forecast, "API_KEY", "test-key"),
                patch.object(short_forecast, "_fetch_vilage_singlepage", return_value=pd.DataFrame(items)),
                patch.object(
                    short_forecast,
                    "_target_times_df",
                    return_value=(targets, pendulum.datetime(2025, 1, 2, 8, tz="Asia/Seoul")),
                ),
            ):
                result = short_forecast.collect_short_fcst(
                    call_list_csv=call_list, out_path=str(output), run_dt="2025010208"
                )
            self.assertEqual(result, str(output))
            self.assertTrue(output.is_file())

    def test_typhoon_items_have_hourly_track_inputs(self):
        track = _parse_track_items(load_fixture("typhoon_forecast_items.json"))
        self.assertEqual(len(track), 2)
        self.assertEqual(track.iloc[-1]["wind"], 30)
        self.assertEqual(track.iloc[0]["time"].strftime("%Y%m%d%H%M"), "202501020600")

    def test_uv_offsets_are_numeric(self):
        offsets = _parse_h_offsets(load_fixture("uv_forecast_item.json"))
        self.assertEqual(offsets, {0: 0.0, 3: 1.0, 6: 2.0, 9: 3.0})

    def test_uv_v5_request_returns_items(self):
        item = load_fixture("uv_forecast_item.json")
        response = FakeResponse({
            "response": {
                "header": {"resultCode": "00"},
                "body": {"items": {"item": [item]}},
            }
        })
        with (
            patch.object(uv_forecast, "API_KEY", "test-key"),
            patch.object(uv_forecast.session, "get", return_value=response) as get,
        ):
            self.assertEqual(uv_forecast._http("1100000000", "2025010206"), [item])
        self.assertTrue(get.call_args.args[0].endswith("/LivingWthrIdxServiceV5/getUVIdxV5"))

    def test_uv_api_error_is_not_empty_data(self):
        payload = {"OpenAPI_ServiceResponse": {
            "cmmMsgHeader": {"errMsg": "NO_OPENAPI_SERVICE_ERROR"}
        }}
        with self.assertRaisesRegex(RuntimeError, "NO_OPENAPI_SERVICE_ERROR"):
            uv_forecast._items_from_text(json.dumps(payload))
        self.assertEqual(uv_forecast._items_from_text(json.dumps({
            "response": {"header": {"resultCode": "03"}, "body": {"items": {}}}
        })), [])
        self.assertEqual(uv_forecast._items_from_text(json.dumps({
            "response": {"header": {"resultCode": "99", "resultMsg": "검색결과가 없습니다. [2811000000]"}}
        })), [])
        with self.assertRaisesRegex(RuntimeError, "KMA UV API 99"):
            uv_forecast._items_from_text(json.dumps({
                "response": {"header": {"resultCode": "99", "resultMsg": "UNKNOWN_SYSTEM_ERROR"}}
            }))

    def test_uv_find_base_extracts_base_date_from_item(self):
        item = {
            "areaNo": "1100000000",
            "date": "2026092312",
            "h0": "7",
            "h3": "4",
        }
        response = FakeResponse({
            "response": {
                "header": {"resultCode": "00"},
                "body": {"items": {"item": [item]}},
            }
        })
        anchor = pendulum.datetime(2026, 9, 23, 13, 0, tz="Asia/Seoul")
        with (
            patch.object(uv_forecast, "API_KEY", "test-key"),
            patch.object(uv_forecast.session, "get", return_value=response),
        ):
            base_dt, rel = uv_forecast._find_base("1100000000", anchor, back_hours=12)
        self.assertIsNotNone(base_dt)
        self.assertEqual(base_dt.strftime("%Y%m%d%H"), "2026092312")
        self.assertEqual(rel, {0: 7.0, 3: 4.0})

    def test_uv_area_code_alias_mapping(self):
        # 2026년 행정구역 개편 코드 매핑 검증
        self.assertEqual(uv_forecast.AREA_CODE_ALIAS["2911000000"], "1221000000")  # 광주 동구
        self.assertEqual(uv_forecast.AREA_CODE_ALIAS["4611000000"], "1211000000")  # 전남 목포시
        self.assertEqual(uv_forecast.AREA_CODE_ALIAS["2811000000"], "2812500000")  # 인천 제물포구
