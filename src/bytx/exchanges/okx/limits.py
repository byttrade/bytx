"""OKX: amount step/minimum from /public/instruments.

Docs «Get instruments» (https://www.okx.com/docs-v5/en/#public-data-rest-api-get-instruments):
``lotSz`` — step, ``minSz`` — minimum (in contracts for SWAP). OKX has no minimum
notional — ``min_cost`` is not set.
"""
from __future__ import annotations

from typing import Any, Optional


def lot_rules(market: dict) -> tuple[Optional[float], Optional[float]]:
    """(step, minimum) amount: ``lotSz`` / ``minSz``."""
    info = market.get('info') or {}
    precision = market.get('precision') or {}
    limits = (market.get('limits') or {}).get('amount') or {}
    if info:
        return float(info.get('lotSz', precision.get('amount', 0.01))), float(info.get('minSz', limits.get('min', 0.01)))
    return float(precision.get('amount', 0.01)), float(limits.get('min', 0.01))


def min_cost(market: dict) -> Optional[float]:
    return None


def _price(value: Any) -> Optional[float]:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if out > 0 else None


async def price_band(ex: Any, symbol: str) -> Optional[tuple[Optional[float], Optional[float]]]:
    """Order price band now: (highest buy price, lowest sell price) or None.

    Docs «Get limit price» (https://www.okx.com/docs-v5/en/#public-data-rest-api-get-limit-price):
    GET /api/v5/public/price-limit ``instId`` — «Retrieve the highest buy limit and lowest sell limit of
    the instrument»; ``buyLmt`` / ``sellLmt`` («Return "" when enabled is false»), ``enabled`` — «Whether
    price limit is effective». An order beyond the band is rejected entirely: 51006 «Order price is not within the
    price limit (max buy price: {param0}, min sell price: {param1})» (demo 2026-11-04: ±0.5% for BTC-USDT-SWAP).
    One request without retries (public, 20 requests / 2 s per IP)."""
    m = ex.ccxt.market(symbol)
    resp = await ex._call(ex.ccxt.public_get_public_price_limit, {'instId': m['id']}, safe=False)
    rows = resp.get('data') if isinstance(resp, dict) else None
    row = next((r for r in rows or [] if isinstance(r, dict) and r.get('instId') == m['id']), None)
    if row is None or str(row.get('enabled')).lower() == 'false':
        return None
    buy, sell = _price(row.get('buyLmt')), _price(row.get('sellLmt'))
    return (buy, sell) if buy is not None or sell is not None else None
