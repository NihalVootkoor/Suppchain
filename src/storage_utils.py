"""Storage decoding utilities."""

from __future__ import annotations

import json
from typing import Any


def row_to_dict(row: dict[str, Any]) -> dict[str, Any]:
    """Convert an enriched event row to a dict with decoded lists."""

    data = dict(row)
    list_keys = (
        "oem_entities",
        "supplier_entities",
        "component_entities",
        "mitigation_actions",
    )
    for key in list_keys:
        value = data.get(key)
        if isinstance(value, str):
            data[key] = json.loads(value) if value else []
        elif value is None:
            data[key] = []
    return data
