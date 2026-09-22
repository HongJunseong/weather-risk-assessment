"""Common validation for KMA JSON response envelopes."""

from __future__ import annotations

from typing import Any


def extract_kma_items(payload: dict[str, Any]) -> tuple[list[dict[str, Any]], Any]:
    response = payload.get("response")
    if not isinstance(response, dict):
        raise RuntimeError("KMA malformed response: response object is missing")

    header = response.get("header") or {}
    if header.get("resultCode") != "00":
        raise RuntimeError(f"KMA API error: {header}")

    body = response.get("body") or {}
    container = body.get("items")
    if not container:
        return [], body.get("totalCount")
    if not isinstance(container, dict):
        raise RuntimeError("KMA malformed response: items must be an object")

    items = container.get("item") or []
    if isinstance(items, dict):
        items = [items]
    if not isinstance(items, list):
        raise RuntimeError("KMA malformed response: item must be a list")
    return items, body.get("totalCount")
