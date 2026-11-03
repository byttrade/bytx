"""OKX: applied leverage from the Set leverage response.

Docs «Set leverage» (POST /api/v5/account/set-leverage): ``data[]`` with the ``lever`` field
(String) — leverage after applying.
"""
from __future__ import annotations

from typing import Any, Optional


def applied_leverage(response: Any) -> Optional[float]:
    """The smallest ``data[].lever`` of the response (several records — size only by the smallest)."""
    if not isinstance(response, dict):
        return None
    data = response.get('data')
    if data is None and isinstance(response.get('info'), dict):
        data = response['info'].get('data')
    values = []
    for row in data if isinstance(data, list) else []:
        try:
            values.append(float(row['lever']))
        except (KeyError, TypeError, ValueError):
            continue
    return min(values) if values else None
