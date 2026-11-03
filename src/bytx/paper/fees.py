"""Paper-mode fees: each exchange's default tier, per its documentation.

Tables live in ``bytx.exchanges.<exchange>.fees`` (source URL, ``verified`` — whether the
value was checked against the doc text). ccxt (``Market.maker_fee``) is not used as a source. Custom tier —
``PaperConfig(fees=PaperFees(maker=..., taker=...))`` or ``fees=(maker, taker)``.
"""
from __future__ import annotations

import importlib
from dataclasses import dataclass
from typing import Optional, Union

from bytx.errors import ConfigError

# ccxt id → bytx.exchanges package
_PACKAGES = {'binance': 'binance', 'bybit': 'bybit', 'okx': 'okx', 'bingx': 'bingx',
             'hyperliquid': 'hyperliquid', 'kucoin': 'kucoin', 'kucoinfutures': 'kucoin',
             'bitget': 'bitget', 'gate': 'gate', 'mexc': 'mexc', 'htx': 'htx'}


@dataclass(frozen=True, slots=True)
class PaperFees:
    """Fee rates (fractions: 0.0005 = 0.05%). Negative maker — rebate."""

    maker: float
    taker: float
    source: Optional[str] = None
    verified: bool = True


FeesArg = Union[PaperFees, tuple[float, float], None]


def default_fees(exchange_id: str, market_type: str = 'swap') -> PaperFees:
    """Default-tier fee of an exchange (``exchange_id`` — ccxt id or SDK name)."""
    package = _PACKAGES.get(exchange_id)
    if package is None:
        raise ConfigError(f'paper: no fee table for {exchange_id!r} — set PaperConfig(fees=...)')
    table = importlib.import_module(f'bytx.exchanges.{package}.fees').PAPER_FEES
    kind = 'spot' if market_type == 'spot' else 'swap'
    maker, taker, source, verified = table[kind]
    return PaperFees(maker, taker, source, verified)


def resolve_fees(fees: FeesArg, exchange_id: str, market_type: str) -> PaperFees:
    if fees is None:
        return default_fees(exchange_id, market_type)
    if isinstance(fees, PaperFees):
        return fees
    maker, taker = fees
    return PaperFees(float(maker), float(taker), 'user', True)


__all__ = ['PaperFees', 'default_fees', 'resolve_fees']
