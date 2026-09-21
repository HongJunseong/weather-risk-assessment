"""Shared local paths. Configure environment before importing pipeline modules."""
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = Path(os.environ.get("WEATHER_DATA_DIR") or PROJECT_ROOT / "data").expanduser().resolve()
SINK_DIR = Path(os.environ.get("DRE_SINK_DIR") or DATA_ROOT / "live").expanduser().resolve()
