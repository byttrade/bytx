"""Shared by ``exchanges/<exchange>/history.py``: market-history limits per docs.

Each exchange describes how many records one candle / funding / public-trade request returns,
how deep history goes, and which request parameters make the ``[since, until]`` window
explicit (ccxt sometimes sends something other than what the docs require; fixed here).

The :mod:`bytx.data` loader reads these tables; ccxt is not the source of truth.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Optional

DAY_MS = 86_400_000
HOUR_MS = 3_600_000


@dataclass(frozen=True)
class PageLimit:
    """How many records one request returns (``page``) and how deep history goes (``depth_ms``,
    ``depth_rows`` — in records, e.g. the last 5000 HL candles). ``verified`` — the number is taken from the docs
    text at ``url``; False — the docs don't say, the value is conservative (see ``note``)."""

    page: int
    url: str
    verified: bool
    note: str = ''
    depth_ms: Optional[int] = None
    depth_rows: Optional[int] = None


@dataclass(frozen=True)
class TradesHistory:
    """Public trade history. ``supported=False`` — per docs REST returns only recent trades
    (``note`` — how many and where to get the archive); otherwise ``page`` per request, ``window_ms`` — the largest
    ``[start, end]`` window of one request (Binance aggTrades — under an hour), ``depth_ms`` — depth."""

    supported: bool
    url: str
    verified: bool
    note: str = ''
    page: int = 0
    window_ms: Optional[int] = None
    depth_ms: Optional[int] = None


#: ccxt ``fetch_funding_rate_history`` params for the ``[since, until]`` window (ms, both bounds inclusive)
FundingParams = Callable[[int, int], dict]
#: trade page: (ccxt, market, since, until, state) → raw ccxt trades (ascending by time)
TradesPage = Callable[..., Any]


def until_param(_since: int, until: int) -> dict:
    """The window is set by the unified ccxt ``until`` (exchanges where ccxt maps it to the docs parameter)."""
    return {'until': until}


__all__ = ['DAY_MS', 'HOUR_MS', 'FundingParams', 'PageLimit', 'TradesHistory', 'TradesPage', 'until_param']
