"""Pipeline run-time contract shared by Airflow tasks and command-line jobs."""

from __future__ import annotations

import re

import pendulum


KST = pendulum.timezone("Asia/Seoul")
RUN_DT_PATTERN = re.compile(r"^\d{10}$")


def resolve_run_time(run_dt: str | None = None) -> pendulum.DateTime:
    """Return an hour-aligned KST timestamp for a ``YYYYMMDDHH`` run id.

    ``None`` or an empty value is supported for direct CLI use and resolves to
    the current KST hour. Scheduled Airflow tasks should always pass ``run_dt``.
    """
    value = (run_dt or "").strip()
    if not value:
        return pendulum.now(KST).start_of("hour")
    if not RUN_DT_PATTERN.fullmatch(value):
        raise ValueError(f"run_dt must use YYYYMMDDHH, got {run_dt!r}")
    try:
        return pendulum.from_format(value, "YYYYMMDDHH", tz=KST)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"run_dt is not a valid KST hour: {run_dt!r}") from exc


def normalize_run_dt(run_dt: str | None = None) -> str:
    """Resolve and serialize a run id in its canonical ``YYYYMMDDHH`` form."""
    return resolve_run_time(run_dt).format("YYYYMMDDHH")
