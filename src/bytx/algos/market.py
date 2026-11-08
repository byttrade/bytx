"""Market with a slippage cap: limit IOC at "reference ± cap".

A plain market order on a thin book walks through any number of levels. Here the price is capped:
``buy`` no higher than ``reference × (1 + max_slippage_bps / 10⁴)``, ``sell`` no lower than
``reference × (1 − …)``; the price is rounded to the tick **inside** the cap (buy down, sell up). If the
exchange's price band is narrower than the cap (OKX price-limit: an order beyond it is rejected entirely, 51006),
the price goes inside the band.
The order is a limit with ``time_in_force='IOC'``: fills immediately within the price, the exchange
cancels the rest (all 6 SDK exchanges support IOC: Binance/BingX/KuCoin ``timeInForce=IOC``, Bybit ``IOC``,
OKX ``ordType=ioc``, Hyperliquid ``tif: Ioc`` — ``tests/ground_truth/*/place_order``/``order``).

Reference price (``reference``): 'best' is the best opposite price (ask for buy) at start,
'mid' is the midpoint. Book without extra requests (:func:`~bytx.algos.base.current_book`).
Before sending, the book is checked: if there is no opposite liquidity within the cap, the order is not
sent. ``retries``: how many times to repeat IOC for the remainder (with a ``poll`` pause) from the **same**
reference price: the cap does not drift with the market. A remainder below the exchange's minimum order (step,
``min_amount``, minimum notional ``min_cost`` at the IOC price; for reduce-only — without the notional) is not
retried — ``extra['below_min']``; the whole ``size`` below the minimum — ``OrderTooSmall`` ('failed').
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, Optional

from bytx.algos.base import EPS, TERMINAL_ORDER, ExecAlgo, bps, check_positive
from bytx.errors import ConfigError
from bytx.types import OrderBook, Side

if TYPE_CHECKING:  # pragma: no cover
    from bytx.exchange import Exchange


def cap_price(ref: float, side: Side, max_slippage_bps: float) -> float:
    """Cap price before rounding."""
    k = max_slippage_bps / 1e4
    return ref * (1 + k) if side == 'buy' else ref * (1 - k)


def available_within(book: OrderBook, side: Side, limit: float) -> float:
    """Opposite liquidity (base) at a price no worse than ``limit``."""
    if side == 'buy':
        return sum(q for p, q in book.asks if p <= limit + EPS)
    return sum(q for p, q in book.bids if p >= limit - EPS)


class CappedMarket(ExecAlgo):
    """Market with a slippage cap (see module). ``size`` in base."""

    kind = 'market'

    def __init__(self, ex: 'Exchange', symbol: str, side: Side, *, size: float, max_slippage_bps: float = 50.0,
                 reference: Literal['best', 'mid'] = 'best', retries: int = 0, reduce_only: bool = False,
                 **kwargs: Any) -> None:
        kwargs.setdefault('role', 'close' if reduce_only else 'entry')
        super().__init__(ex, symbol, side, **kwargs)
        check_positive('size', size, ex)
        check_positive('max_slippage_bps', max_slippage_bps, ex)
        if reference not in ('best', 'mid'):
            raise ConfigError(f"reference: 'best' or 'mid', not {reference!r}", exchange=ex.name)
        if retries < 0:
            raise ConfigError(f'retries must be >= 0, not {retries!r}', exchange=ex.name)
        self.size = float(size)
        self.max_slippage_bps = float(max_slippage_bps)
        self.reference = reference
        self.retries = int(retries)
        self.reduce_only = reduce_only

    async def _run(self) -> None:
        side: Side = self.side  # type: ignore[assignment]
        self.target = self._round_size(self.size)
        book = await self._book()
        best = book.best_ask if side == 'buy' else book.best_bid
        ref = book.mid if self.reference == 'mid' else (best[0] if best else None)
        if ref is None:
            self._stop('no_liquidity')
            return
        limit = await self._cap_limit(ref, self.max_slippage_bps)
        self.extra.update(reference_price=ref, limit_price=limit, attempts=0)
        min_size = max(self._min_size(limit, reduce_only=self.reduce_only), EPS)    # notional — at the IOC price
        self._check_target(min_size, reduce_only=self.reduce_only)
        attempt = 0
        while True:
            await self._gate()
            if not await self._resolve_unknown():
                await self._sleep(self.poll)
                continue
            left = self._round_size(self.target - self.filled)
            if left < min_size:
                self._below_min(left)
                self._stop('filled')
                break
            if attempt > self.retries:
                self._stop('partial' if self.filled > EPS else 'no_liquidity')
                break
            if attempt:
                await self._sleep(self.poll)
                book = await self._book()
            attempt += 1
            self.extra['attempts'] = attempt
            if available_within(book, side, limit) <= EPS:
                continue                          # no opposite orders within the cap
            order = await self._place(side=side, size=left, price=limit, type='limit', time_in_force='IOC',
                                      reduce_only=self.reduce_only, tag='ioc')
            while order is not None and order.status not in TERMINAL_ORDER:
                await self._sleep(self.poll)      # IOC closes immediately; response without status: re-read
                order = await self._refresh(order)
        avg = self.average
        self.extra['slippage_bps'] = (bps(avg, ref) if side == 'buy' else -bps(avg, ref)) if avg else None


__all__ = ['CappedMarket', 'available_within', 'cap_price']
