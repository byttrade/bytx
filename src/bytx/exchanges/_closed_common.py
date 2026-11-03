"""Shared by ``exchanges/<exchange>/position_history.py`` of newer exchanges: closed position."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional


def fnum(value: Any) -> Optional[float]:
    try:
        return None if value in (None, '') else float(value)
    except (TypeError, ValueError):
        return None


@dataclass(slots=True)
class ClosedPosition:
    """Closed position from exchange history. ``size`` — in base (Bitget) or contracts × contractSize (Gate);
    ``realized_pnl`` — price PnL (as the exchange reports it), ``net_pnl`` — with fees and funding, if the exchange
    reports it; ``fee`` — fees with the exchange's sign (negative — paid). Whatever the exchange omits is None."""

    exchange: str
    symbol: str
    side: str
    size: Optional[float]
    entry_price: Optional[float]
    exit_price: Optional[float]
    realized_pnl: Optional[float]
    net_pnl: Optional[float] = None
    fee: Optional[float] = None
    funding: Optional[float] = None
    open_ts: Optional[int] = None
    close_ts: Optional[int] = None
    id: Optional[str] = None
    raw: dict = field(default_factory=dict, repr=False)


__all__ = ['ClosedPosition', 'fnum']
