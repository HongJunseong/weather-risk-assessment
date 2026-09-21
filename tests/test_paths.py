import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class PathTests(unittest.TestCase):
    def paths(self, cwd, **overrides):
        env = {k: v for k, v in os.environ.items()
               if k not in {'WEATHER_DATA_DIR', 'DRE_SINK_DIR'}}
        env.update(overrides, PYTHONPATH=str(ROOT))
        result = subprocess.check_output([
            sys.executable, '-c',
            'import json; from weather_risk_assessment.paths import DATA_ROOT, SINK_DIR; '
            'print(json.dumps([str(DATA_ROOT), str(SINK_DIR)]))',
        ], cwd=cwd, env=env, text=True)
        return json.loads(result)

    def test_default_independent_of_working_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(self.paths(tmp), [str(ROOT / 'data'), str(ROOT / 'data/live')])

    def test_override_does_not_create_directories_on_import(self):
        with tempfile.TemporaryDirectory() as tmp:
            data, sink = Path(tmp) / 'custom', Path(tmp) / 'sink'
            self.assertEqual(self.paths(tmp, WEATHER_DATA_DIR=str(data), DRE_SINK_DIR=str(sink)),
                             [str(data), str(sink)])
            self.assertFalse(data.exists())
            self.assertFalse(sink.exists())

    def test_blank_env_uses_defaults(self):
        self.assertEqual(self.paths(ROOT, WEATHER_DATA_DIR='', DRE_SINK_DIR=''),
                         [str(ROOT / 'data'), str(ROOT / 'data/live')])
