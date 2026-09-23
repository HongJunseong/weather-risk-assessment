"""Shared local paths. Configure environment before importing pipeline modules."""
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = Path(os.environ.get("WEATHER_DATA_DIR") or PROJECT_ROOT / "data").expanduser().resolve()
SINK_DIR = Path(os.environ.get("DRE_SINK_DIR") or DATA_ROOT / "live").expanduser().resolve()


def outputs_are_current(outputs, sources) -> bool:
    outputs = tuple(map(Path, outputs))
    sources = tuple(map(Path, sources))
    return (
        bool(outputs and sources)
        and all(path.is_file() and path.stat().st_size > 0 for path in outputs)
        and all(path.is_file() for path in sources)
        and min(path.stat().st_mtime_ns for path in outputs)
        >= max(path.stat().st_mtime_ns for path in sources)
    )
