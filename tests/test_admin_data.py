import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


class AdminDataTests(unittest.TestCase):
    def test_module_entrypoint_writes_to_configured_data_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp)
            pd.DataFrame({
                'admin_code': ['11110', '11111', '26110'],
                'nx': [60, 60, 98], 'ny': [127, 127, 76],
                'lat': [37.57, 37.58, 35.10], 'lon': [126.98, 126.99, 129.03],
            }).to_csv(data / 'admin_centroids.csv', index=False)
            command = [
                sys.executable, '-m', 'weather_risk_assessment.scripts.make_admin_list',
            ]
            environment = {
                **os.environ,
                'WEATHER_DATA_DIR': tmp,
                'PYTHONPATH': str(ROOT),
            }
            subprocess.run(command, env=environment, cwd=tmp, check=True,
                           capture_output=True, text=True)
            calls = pd.read_csv(data / 'unique_admin_centroids.csv')
            grid = pd.read_parquet(data / 'grid_latlon.parquet')
            self.assertEqual(list(zip(calls.nx, calls.ny)), [(60, 127), (98, 76)])
            self.assertEqual(list(zip(grid.nx, grid.ny)), [(60, 127), (98, 76)])

            outputs = [data / 'unique_admin_centroids.csv', data / 'grid_latlon.parquet']
            modified = [path.stat().st_mtime_ns for path in outputs]
            second = subprocess.run(command, env=environment, cwd=tmp, check=True,
                                    capture_output=True, text=True)
            self.assertEqual([path.stat().st_mtime_ns for path in outputs], modified)
            self.assertIn('reused:', second.stdout)
