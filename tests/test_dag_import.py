import importlib.util
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


AIRFLOW_AVAILABLE = importlib.util.find_spec("airflow") is not None
ROOT = Path(__file__).resolve().parents[1]


@unittest.skipUnless(AIRFLOW_AVAILABLE, "Airflow is installed in the DAG CI job")
class DagImportTests(unittest.TestCase):
    def test_weather_dag_loads_without_import_errors(self):
        with tempfile.TemporaryDirectory() as airflow_home, patch.dict(
            os.environ,
            {
                "AIRFLOW_HOME": airflow_home,
                "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
            },
        ):
            from airflow.models import DagBag

            dag_bag = DagBag(dag_folder=str(ROOT / "dags"), include_examples=False)
            self.assertEqual(dag_bag.import_errors, {})

            dag = dag_bag.dags.get("weather_risk_assessment")
            self.assertIsNotNone(dag)
            self.assertEqual(dag.schedule_interval, "10 * * * *")
            self.assertFalse(dag.catchup)
            self.assertEqual(dag.max_active_runs, 1)
            self.assertTrue(all(task.retries == 2 for task in dag.tasks))
            self.assertTrue(
                all(task.retry_delay.total_seconds() == 300 for task in dag.tasks)
            )
            self.assertEqual(
                set(dag.task_ids),
                {
                    "make_admin_centroids_from_shp",
                    "make_unique_admin_list",
                    "collect_kma_legacy",
                    "collect_short_fcst",
                    "collect_typhoon_forecast_wide",
                    "collect_uv_wide",
                    "upload_bronze_to_s3",
                    "build_silver_risk_enriched",
                    "build_gold_risk_latest",
                    "build_gold_risk_daily",
                    "export_gold_parquet",
                    "send_high_risk_alerts_to_kafka",
                },
            )


if __name__ == "__main__":
    unittest.main()
