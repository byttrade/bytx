"""TWAP: size in slices at intervals over a given time.

``slices`` slices over ``duration`` seconds: the first immediately, then every ``duration / slices``.
``randomize`` (0…0.9): randomness against predictability: the time of each slice (except
the first) shifts within ±``randomize``/2 of the interval, slice weight ×(1 ± ``randomize``)
(sum is exactly ``size``); ``seed`` for reproducibility (without it, replay derives from ``PaperConfig.seed``).

A slice executes as ``slice_type``:

- 'market': a market order; with ``max_slippage_bps``, a limit IOC no worse than "best opposite ±
  cap" (as in :mod:`bytx.algos.market`);
- 'limit': post-only at the best price on its side; before the next slice the unfilled part is canceled
  and rolled into it. The remainder after the last slice (one interval later) goes as market (with the cap,
  if set).

Each slice catches up on **schedule lag**: the target for slice i is the cumulative share of size
minus what is already filled (underfill from earlier slices is not lost, no overfill). A slice below
the exchange's minimum size accumulates into the next. Pause shifts the remaining schedule by the pause duration.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, Optional

from bytx.algos.base import algo_rng, EPS, TERMINAL_ORDER, ExecAlgo, check_positive
from bytx.algos.market import available_within, cap_price
from bytx.errors import ConfigError, InvalidOrder
from bytx.types import Order, Side

if TYPE_CHECKING:  # pragma: no cover
    from bytx.exchange import Exchange


class TWAP(ExecAlgo):
    """TWAP (see module). ``size`` in base, ``duration`` in seconds."""

    kind = 'twap'

    def __init__(self, ex: 'Exchange', symbol: str, side: Side, *, size: float, duration: float, slices: int,
                 randomize: float = 0.0, seed: Optional[int] = None,
                 slice_type: Literal['market', 'limit'] = 'market', max_slippage_bps: Optional[float] = None,
                 reduce_only: bool = False, **kwargs: Any) -> None:
        kwargs.setdefault('role', 'close' if reduce_only else 'entry')
        super().__init__(ex, symbol, side, **kwargs)
        check_positive('size', size, ex)
        check_positive('duration', duration, ex)
        check_positive('max_slippage_bps', max_slippage_bps, ex, allow_none=True)
        if not isinstance(slices, int) or slices < 1:
            raise ConfigError(f'slices: integer >= 1, not {slices!r}', exchange=ex.name)
        if not 0.0 <= randomize <= 0.9:
            raise ConfigError(f'randomize: 0…0.9, not {randomize!r}', exchange=ex.name)
        if slice_type not in ('market', 'limit'):
            raise ConfigError(f"slice_type: 'market' or 'limit', not {slice_type!r}", exchange=ex.name)
        self.size = float(size)
        self.duration = float(duration)
        self.slices = slices
        self.randomize = float(randomize)
        self.slice_type = slice_type
        self.max_slippage_bps = max_slippage_bps
        self.reduce_only = reduce_only
        rng = algo_rng(ex, seed)
        interval = self.duration * 1000 / slices
        r = self.randomize
        self.offsets_ms = [0] + [int(round(i * interval + (rng.random() - 0.5) * r * interval))
                                 for i in range(1, slices)]
        weights = [1.0 + (rng.random() * 2 - 1) * r for _ in range(slices)]
        total = sum(weights)
        acc = 0.0
        self.cum_share: list[float] = []
        for w in weights:
            acc += w
            self.cum_share.append(acc / total)
        self.cum_share[-1] = 1.0
        self._interval_ms = interval
        self._resting: Optional[Order] = None

    async def _run(self) -> None:
        self.target = self._round_size(self.size)
        min_size = max(self._min_size(), EPS)
        start = self.clock.now_ms()
        self.extra.update(slices_done=0, schedule_ms=list(self.offsets_ms))
        for i in range(self.slices):
            await self._wait_until(start + self.offsets_ms[i])
            await self._close_resting()
            want = self._round_size(self.target * self.cum_share[i] - self.filled)
            if want >= min_size:
                await self._slice(want, last=False)
            self.extra['slices_done'] = i + 1
        if self.slice_type == 'limit':
            await self._wait_until(start + int(self.slices * self._interval_ms))
            await self._close_resting()
            left = self._round_size(self.target - self.filled)
            if left >= min_size:
                await self._slice(left, last=True)
        left = self._round_size(self.target - self.filled)
        self._stop('filled' if left < min_size else 'partial')

    async def _wait_until(self, t_ms: int) -> None:
        while True:
            await self._gate()
            if not await self._resolve_unknown():
                await self._sleep(self.poll)
                continue
            now = self.clock.now_ms()
            due = t_ms + self.paused_ms
            if now >= due:
                return
            await self._sleep((due - now) / 1000)

    async def _close_resting(self) -> None:
        """An unfilled limit slice is canceled before the next one (the remainder rolls into it)."""
        while self._resting is not None:
            done = await self._cancel_child(self._resting)
            if done is not None:
                self._resting = None
                return
            await self._sleep(self.poll)     # status unclear: do not place a new slice
            await self._gate()

    async def _slice(self, size: float, *, last: bool) -> None:
        side: Side = self.side  # type: ignore[assignment]
        if self.slice_type == 'limit' and not last:
            book = await self._book()
            lvl = book.best_bid if side == 'buy' else book.best_ask
            if lvl is None:
                self.extra['skipped'] = self.extra.get('skipped', 0) + 1
                return
            price = self._round_price(lvl[0], side)
            try:
                order = await self._place(side=side, size=size, price=price, type='limit', post_only=True,
                                          reduce_only=self.reduce_only, tag='slice')
            except InvalidOrder as e:            # post-only bounce: the slice rolls into the next one
                self.extra['bounces'] = self.extra.get('bounces', 0) + 1
                self.extra['last_bounce'] = str(e)
                return
            if order is not None and order.status not in TERMINAL_ORDER:
                self._resting = order
            return
        if self.max_slippage_bps is None:
            order = await self._place(side=side, size=size, type='market', reduce_only=self.reduce_only, tag='slice')
        else:
            book = await self._book()
            best = book.best_ask if side == 'buy' else book.best_bid
            limit = self._round_price(cap_price(best[0], side, self.max_slippage_bps), side) if best else 0.0
            if best is None or available_within(book, side, limit) <= EPS:
                self.extra['skipped'] = self.extra.get('skipped', 0) + 1
                return
            order = await self._place(side=side, size=size, price=limit, type='limit', time_in_force='IOC',
                                      reduce_only=self.reduce_only, tag='slice')
        while order is not None and order.status not in TERMINAL_ORDER:
            await self._sleep(self.poll)
            order = await self._refresh(order)


__all__ = ['TWAP']
