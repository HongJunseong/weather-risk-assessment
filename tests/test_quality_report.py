import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from weather_risk_assessment.scripts.generate_quality_report import (
    collect_ingestion_metrics,
    collect_risk_metrics,
    format_markdown_report,
    generate_report,
)


class QualityReportTests(unittest.TestCase):
    def test_collect_ingestion_metrics_with_sample_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            sink_dir = Path(tmp)
            # Create uv.parquet with 247 rows
            uv_df = pd.DataFrame({"admin_code": [str(i) for i in range(247)], "UVI": [5.0] * 247})
            uv_df.to_parquet(sink_dir / "uv.parquet", index=False)

            # Create short_fcst.parquet with 100 unique locations
            short_df = pd.DataFrame({"nx": list(range(100)), "ny": list(range(100)), "TMP": [20.0] * 100})
            short_df.to_parquet(sink_dir / "short_fcst.parquet", index=False)

            metrics = collect_ingestion_metrics(sink_dir)

            self.assertTrue(metrics["uv"]["exists"])
            self.assertEqual(metrics["uv"]["rows"], 247)
            self.assertEqual(metrics["uv"]["unique_locations"], 247)
            self.assertEqual(metrics["uv"]["coverage_pct"], 100.0)
            self.assertEqual(metrics["uv"]["missing_pct"], 0.0)

            self.assertTrue(metrics["short_fcst"]["exists"])
            self.assertEqual(metrics["short_fcst"]["unique_locations"], 100)
            self.assertAlmostEqual(metrics["short_fcst"]["coverage_pct"], 40.49, places=1)
            self.assertAlmostEqual(metrics["short_fcst"]["missing_pct"], 59.51, places=1)

            # Non-existent dataset
            self.assertFalse(metrics["ultra_nowcast"]["exists"])
            self.assertEqual(metrics["ultra_nowcast"]["rows"], 0)
            self.assertEqual(metrics["ultra_nowcast"]["missing_pct"], 100.0)

    def test_collect_risk_metrics_calculates_levels_and_highest(self):
        with tempfile.TemporaryDirectory() as tmp:
            risk_file = Path(tmp) / "risk_latest.parquet"
            df = pd.DataFrame([
                {"admin_names": "서울 종로구", "R_total": 0.85, "risk_level": "VERY_HIGH", "fcst_ts": "2026-09-23 18:00"},
                {"admin_names": "부산 해운대구", "R_total": 0.65, "risk_level": "HIGH", "fcst_ts": "2026-09-23 18:00"},
                {"admin_names": "대구 수성구", "R_total": 0.30, "risk_level": "NORMAL", "fcst_ts": "2026-09-23 18:00"},
            ])
            df.to_parquet(risk_file, index=False)

            metrics = collect_risk_metrics(risk_file)
            self.assertEqual(metrics["status"], "VALID")
            self.assertEqual(metrics["total_rows"], 3)
            self.assertEqual(metrics["very_high_count"], 1)
            self.assertEqual(metrics["high_count"], 1)
            self.assertEqual(metrics["normal_count"], 1)
            self.assertEqual(metrics["highest_risk"]["region"], "서울 종로구")
            self.assertEqual(metrics["highest_risk"]["r_total"], 0.85)

    def test_generate_report_writes_json_and_markdown(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            sink_dir = root / "sink"
            sink_dir.mkdir()
            out_json = root / "metrics" / "quality_report.json"
            out_md = root / "docs" / "report.md"

            report = generate_report(
                run_dt="2026092318",
                sink_dir=sink_dir,
                latest_path=None,
                out_json=out_json,
                out_md=out_md,
            )

            self.assertTrue(out_json.exists())
            self.assertTrue(out_md.exists())

            saved_data = json.loads(out_json.read_text(encoding="utf-8"))
            self.assertEqual(saved_data["run_dt"], "2026092318")
            self.assertIn("ingestion", saved_data)
            self.assertIn("risk_summary", saved_data)

            md_text = out_md.read_text(encoding="utf-8")
            self.assertIn("파이프라인 실행 및 데이터 품질 리포트", md_text)
            self.assertIn("2026092318", md_text)


if __name__ == "__main__":
    unittest.main()
