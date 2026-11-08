"""Chase: a limit order chasing the best price on its side.

The order (post-only by default) rests at the best price on its side (buy at best bid, sell at
best ask; price rounded passively to the tick). When the best price moves, the order is **canceled and
re-placed** at the new one: cancel → confirmed final status → new order for the remainder. While
the canceled order's status is unclear (network), no new one is placed. ``amend`` is not used: on some exchanges
a price change drops post-only or is unsupported for this order type; cancel+place
works the same on all 6 exchanges and on paper.

Exchanges handle a post-only order that would cross the book differently: rejection on placement
(Binance USDⓈ-M GTX: error, Hyperliquid ``Alo``: order rejected) or acceptance with immediate
cancellation (Bybit ``PostOnly``, OKX ``post_only``, KuCoin ``postOnly``). Both cases are treated here as
a "bounce": the order did not fill, the book is re-read, the order is re-placed (counts toward ``max_moves``).

A remainder below the exchange's minimum order (step, ``min_amount``, minimum notional ``min_cost`` at the limit
price; for reduce-only — without the notional) can't be placed: the working limit order is canceled, result
``filled``, the remainder — ``extra['below_min']``. The whole ``size`` below the minimum — ``OrderTooSmall`` ('failed').

Limits: ``max_moves``: re-placements (after the first placement); ``max_deviation_bps``:
how far the best price may move against us from the start price; ``timeout``: seconds by the
algorithm's clock; ``min_interval``: at most one re-placement per that many seconds (saves exchange
rate limits). When exhausted, ``on_exhausted``: 'stop' (cancel the order, result ``done`` with
reason 'max_moves' / 'deviation' / 'timeout') or 'market' (remainder as a market with a
``max_slippage_bps`` cap, :class:`~bytx.algos.market.CappedMarket` logic).
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, Optional

from bytx.algos.base import EPS, TERMINAL_ORDER, ExecAlgo, bps, check_positive
from bytx.algos.market import available_within
from bytx.errors import ConfigError, InvalidOrder, OrderTooSmall, PositionAlreadyClosed
from bytx.types import Order, Side

if TYPE_CHECKING:  # pragma: no cover
    from bytx.exchange import Exchange


class Chase(ExecAlgo):
    """Chase (see module). ``size`` in base."""

    kind = 'chase'

    def __init__(self, ex: 'Exchange', symbol: str, side: Side, *, size: float, max_moves: int = 20,
                 max_deviation_bps: Optional[float] = None, min_interval: float = 1.0,
                 timeout: Optional[float] = None, post_only: bool = True,
                 on_exhausted: Literal['stop', 'market'] = 'stop', max_slippage_bps: float = 50.0,
                 reduce_only: bool = False, **kwargs: Any) -> None:
        kwargs.setdefault('role', 'close' if reduce_only else 'entry')
        kwargs.setdefault('poll', 0.5)
        super().__init__(ex, symbol, side, **kwargs)
        check_positive('size', size, ex)
        check_positive('max_deviation_bps', max_deviation_bps, ex, allow_none=True)
        check_positive('timeout', timeout, ex, allow_none=True)
        check_positive('max_slippage_bps', max_slippage_bps, ex)
        if max_moves < 0:
            raise ConfigError(f'max_moves must be >= 0, not {max_moves!r}', exchange=ex.name)
        if min_interval < 0:
            raise ConfigError(f'min_interval must be >= 0, not {min_interval!r}', exchange=ex.name)
        if on_exhausted not in ('stop', 'market'):
            raise ConfigError(f"on_exhausted: 'stop' or 'market', not {on_exhausted!r}", exchange=ex.name)
        self.size = float(size)
        self.max_moves = int(max_moves)
        self.max_deviation_bps = max_deviation_bps
        self.min_interval = float(min_interval)
        self.timeout = timeout
        self.post_only = post_only
        self.on_exhausted = on_exhausted
        self.max_slippage_bps = float(max_slippage_bps)
        self.reduce_only = reduce_only
        self.working: Optional[Order] = None
        self.moves = 0
        self.bounces = 0

    def _passive(self) -> Side:
        return 'buy' if self.side == 'buy' else 'sell'

    async def _target(self) -> Optional[float]:
        book = await self._book()
        lvl = book.best_bid if self.side == 'buy' else book.best_ask
        return self._round_price(lvl[0], self._passive()) if lvl else None

    def _min_at(self, price: Optional[float]) -> float:
        """Minimum order at the limit price (step, min_amount, min_cost; for reduce-only — without the notional)."""
        return max(self._min_size(price, reduce_only=self.reduce_only), EPS)

    async def _run(self) -> None:
        self.target = self._round_size(self.size)
        start_price = await self._target()
        min_size = self._min_at(start_price)
        self._check_target(min_size, reduce_only=self.reduce_only)
        self.extra.update(start_price=start_price, moves=0, bounces=0)
        last_move = self.clock.now_ms()
        deadline = self.started_ms + int(self.timeout * 1000) if self.timeout else None
        placed = False
        while True:
            await self._gate()
            if not await self._resolve_unknown():
                await self._sleep(self.poll)
                continue
            if self.working is not None:
                self.working = await self._refresh(self.working)
                self._settled()
            left = self._round_size(self.target - self.filled)
            if left < min_size:
                await self._finish_below_min(left)
                return
            now = self.clock.now_ms()
            price = await self._target()
            if price is not None:
                min_size = self._min_at(price)
                if left < min_size:          # remainder below the minimum notional at the new price
                    await self._finish_below_min(left)
                    return
            exhausted = None
            if deadline is not None and now >= deadline + self.paused_ms:
                exhausted = 'timeout'
            elif price is not None and start_price and self.max_deviation_bps is not None:
                away = bps(price, start_price) if self.side == 'buy' else -bps(price, start_price)
                if away > self.max_deviation_bps:
                    exhausted = 'deviation'
            if exhausted is not None:
                await self._exhaust(exhausted)
                return
            if price is None:
                await self._sleep(self.poll)
                continue
            if self.working is None:
                if placed:
                    if self.moves >= self.max_moves:
                        await self._exhaust('max_moves')
                        return
                    self.moves += 1
                placed = True
                await self._place_at(price, left)
                last_move = now
            elif self.working.price != price and now - last_move >= self.min_interval * 1000:
                if self.moves >= self.max_moves:
                    await self._exhaust('max_moves')
                    return
                done = await self._cancel_child(self.working)
                if done is None:
                    await self._sleep(self.poll)   # status unclear: do not place a new one
                    continue
                self.working = None
                self.moves += 1
                left = self._round_size(self.target - self.filled)   # may have filled before the cancel
                if left >= min_size:
                    await self._place_at(price, left)
                else:
                    await self._finish_below_min(left)
                    return
                last_move = now
            self.extra.update(moves=self.moves, bounces=self.bounces)
            await self._sleep(self.poll)

    async def _finish_below_min(self, left: float) -> None:
        """Remainder below the minimum order: cancel the working limit order (partially filled), result 'filled'
        (the remainder can't be placed — ``extra['below_min']``)."""
        if self.working is not None and self.working.status not in TERMINAL_ORDER:
            if await self._cancel_child(self.working) is None:
                await self._await_final(self.working)       # no final state — 'cancel_unconfirmed', no second order
            self.working = None
            left = self._round_size((self.target or 0.0) - self.filled)
        self._below_min(left)
        self.extra.update(moves=self.moves, bounces=self.bounces)
        self._stop('filled')

    def _settled(self) -> None:
        """Working order finished: not fully filled and not canceled by us means a "bounce"."""
        w = self.working
        if w is None or w.status not in TERMINAL_ORDER:
            return
        if w.status != 'closed' and float(w.filled or 0) < float(w.size or 0) - EPS:
            self.bounces += 1
        self.working = None

    async def _place_at(self, price: float, size: float) -> None:
        try:
            self.working = await self._place(side=self.side, size=size, price=price,  # type: ignore[arg-type]
                                             type='limit', post_only=self.post_only,
                                             reduce_only=self.reduce_only, tag='chase')
        except (OrderTooSmall, PositionAlreadyClosed):
            raise
        except InvalidOrder as e:
            if not self.post_only:
                raise
            self.bounces += 1                  # post-only would have crossed the book: re-read and retry
            self.extra['last_bounce'] = str(e)
            self.working = None
        else:
            self._settled()

    async def _exhaust(self, why: str) -> None:
        if self.working is not None:
            if await self._cancel_child(self.working) is None:
                await self._await_final(self.working)       # no final state — 'cancel_unconfirmed', no IOC on top
            self.working = None
        self.extra.update(moves=self.moves, bounces=self.bounces, exhausted=why)
        left = self._round_size((self.target or 0.0) - self.filled)
        book = await self._book() if self.on_exhausted == 'market' else None
        best = (book.best_ask if self.side == 'buy' else book.best_bid) if book is not None else None
        limit = None
        if best is not None:
            limit = await self._cap_limit(best[0], self.max_slippage_bps)
        if book is not None and limit is not None and left >= self._min_at(limit):
            if available_within(book, self.side, limit) > EPS:  # type: ignore[arg-type]
                order = await self._place(side=self.side, size=left, price=limit,  # type: ignore[arg-type]
                                          type='limit', time_in_force='IOC', reduce_only=self.reduce_only,
                                          tag='market')
                while order is not None and order.status not in TERMINAL_ORDER:
                    await self._sleep(self.poll)
                    order = await self._refresh(order)
            left = self._round_size((self.target or 0.0) - self.filled)
            if left < self._min_at(limit):
                self._below_min(left)
                self._stop('filled')
                return
        self._stop(why)


__all__ = ['Chase']
