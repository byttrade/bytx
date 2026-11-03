"""Shared by ``exchanges.<exchange>.tiers``: parses ccxt leverage tiers into :class:`LeverageTier`."""
from __future__ import annotations

from typing import Any, Optional

from bytx.types import LeverageTier


def num(value: Any) -> Optional[float]:
    try:
        return None if value in (None, '') else float(value)
    except (TypeError, ValueError):
        return None


def from_ccxt(rows: list, source: str = 'exchange') -> list[LeverageTier]:
    """ccxt leverage tiers structure (minNotional/maxNotional/maintenanceMarginRate/maxLeverage)."""
    out = []
    for i, r in enumerate(rows or []):
        tier = r.get('tier')
        out.append(LeverageTier(
            tier=int(tier) if tier is not None else i + 1, max_leverage=num(r.get('maxLeverage')),
            maintenance_margin_rate=num(r.get('maintenanceMarginRate')),
            min_notional=num(r.get('minNotional')), max_notional=num(r.get('maxNotional')),
            source=source, raw=r.get('info') if isinstance(r.get('info'), dict) else r))
    return sorted(out, key=lambda t: t.tier)
