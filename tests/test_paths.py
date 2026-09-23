import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from weather_risk_assessment.paths import outputs_are_current

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

    def test_outputs_are_reused_only_while_newer_than_sources(self):
        with tempfile.TemporaryDirectory() as tmp:
            source, output = Path(tmp) / 'source', Path(tmp) / 'output'
            source.write_text('source')
            output.write_text('output')
            os.utime(source, ns=(1, 1))
            os.utime(output, ns=(2, 2))

            self.assertTrue(outputs_are_current([output], [source]))
            os.utime(source, ns=(3, 3))
            self.assertFalse(outputs_are_current([output], [source]))
