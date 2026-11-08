"""Iceberg: a large fixed-price limit order in visible slices.

Only the visible slice ``visible`` (base) rests in the book at ``price``; once filled, the next one follows
until ``size`` is reached. ``visible_jitter`` (0…0.9): each slice size ×(1 ± jitter), so that a
repeating size does not give the iceberg away; ``seed`` for reproducibility (without it, replay derives from ``PaperConfig.seed``). Client-side emulation, without
exchange-native icebergs (they have their own rules and not all exchanges have them): same on 6 exchanges and paper.

A slice is not smaller than the exchange's minimum order (step, ``min_amount``, minimum notional ``min_cost`` at
``price``; for reduce-only — without the notional): a smaller ``visible`` — the minimum allowed slice rests in the
book; a tail below the minimum that would remain after a slice goes into that slice (``extra['visible_min']``).
The whole ``size`` below the minimum or not divisible into slices smaller than itself — ``OrderTooSmall`` ('failed'):
the iceberg never silently places the whole size.

If the visible slice was canceled by someone else (manually, by the exchange: post-only, STP, liquidation), the algorithm ends
(``reason='child_canceled'``): blindly re-placing over someone else's decision is unsafe. ``timeout``: seconds by
the algorithm's clock, after which the working slice is canceled (``reason='timeout'``).
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

from bytx.algos.base import algo_rng, EPS, TERMINAL_ORDER, ExecAlgo, check_positive
from bytx.errors import ConfigError, OrderTooSmall
from bytx.types import Order, Side

if TYPE_CHECKING:  # pragma: no cover
    from bytx.exchange import Exchange


class Iceberg(ExecAlgo):
    """Iceberg (see module). ``size`` and ``visible`` in base."""

    kind = 'iceberg'

    def __init__(self, ex: 'Exchange', symbol: str, side: Side, *, size: float, price: float, visible: float,
                 visible_jitter: float = 0.0, seed: Optional[int] = None, post_only: bool = False,
                 reduce_only: bool = False, timeout: Optional[float] = None, **kwargs: Any) -> None:
        kwargs.setdefault('role', 'close' if reduce_only else 'entry')
        super().__init__(ex, symbol, side, **kwargs)
        check_positive('size', size, ex)
        check_positive('price', price, ex)
        check_positive('visible', visible, ex)
        check_positive('timeout', timeout, ex, allow_none=True)
        if not 0.0 <= visible_jitter <= 0.9:
            raise ConfigError(f'visible_jitter: 0…0.9, not {visible_jitter!r}', exchange=ex.name)
        if visible > size:
            raise ConfigError(f'visible ({visible}) is greater than size ({size})', exchange=ex.name)
        self.size = float(size)
        self.price = float(price)
        self.visible = float(visible)
        self.visible_jitter = float(visible_jitter)
        self.post_only = post_only
        self.reduce_only = reduce_only
        self.timeout = timeout
        self._rng = algo_rng(ex, seed)
        self.working: Optional[Order] = None

    def _next_visible(self, left: float) -> float:
        j = self.visible_jitter
        part = self.visible * (1.0 + (self._rng.random() * 2 - 1) * j) if j else self.visible
        return min(part, left)

    async def _run(self) -> None:
        self.target = self._round_size(self.size)
        side: Side = self.side  # type: ignore[assignment]
        price = self._round_price(self.price, side)
        min_size = max(self._min_size(price, reduce_only=self.reduce_only), EPS)
        self._check_target(min_size, reduce_only=self.reduce_only)
        target = self.target or 0.0
        first = self._tail_merged(min(max(self._round_size(self.visible), min_size), target), target, min_size)
        if first >= target - EPS and self.visible < target - EPS:
            # the exchange's minimum order doesn't allow splitting the size: the very first slice is the whole size
            raise OrderTooSmall(f'iceberg: minimum order {min_size:g} at price {price:g} does not allow splitting '
                                f'{target:g} into slices ≤ visible {self.visible:g} — the whole size would be in the '
                                f'book', exchange=self.ex.name)
        if min_size > self.visible + EPS:
            self.extra['visible_min'] = min_size
        deadline = self.started_ms + int(self.timeout * 1000) if self.timeout else None
        self.extra.update(price=price, parts=0)
        while True:
            await self._gate()
            if not await self._resolve_unknown():
                await self._sleep(self.poll)
                continue
            if self.working is not None:
                self.working = await self._refresh(self.working)
                if self.working.status in TERMINAL_ORDER:
                    ended, self.working = self.working, None
                    if ended.status != 'closed' and float(ended.filled or 0) < float(ended.size or 0) - EPS:
                        self._stop('child_canceled')
                        return
            left = self._round_size(self.target - self.filled)
            if left < min_size:
                self._below_min(left)
                self._stop('filled')
                return
            if deadline is not None and self.clock.now_ms() >= deadline + self.paused_ms:
                if self.working is not None:
                    if await self._cancel_child(self.working) is None:
                        # no final state within max_errors poll pauses — 'cancel_unconfirmed' (it used to wait forever)
                        await self._await_final(self.working)
                    self.working = None
                self._stop('timeout')
                return
            if self.working is None:
                # the visible slice is not below the minimum order; a tail below the minimum — into this slice
                part = max(self._round_size(self._next_visible(left)), min_size)
                part = self._tail_merged(min(part, left), left, min_size)
                self.working = await self._place(side=side, size=part, price=price, type='limit',
                                                 post_only=self.post_only, reduce_only=self.reduce_only, tag='part')
                self.extra['parts'] = self.extra['parts'] + 1
                if self.working is not None and self.working.status in TERMINAL_ORDER:
                    continue                      # filled immediately (crossed the book): next one
            await self._sleep(self.poll)


__all__ = ['Iceberg']
