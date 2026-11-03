"""Client-side trailing stop for a position.

The algorithm watches the price (``price_source``: 'mid' is the book midpoint, 'mark' / 'last' from the ticker) and
keeps the stop level at ``distance`` (price) or ``distance_pct`` (% of the extreme) from the best price since
activation: long trails down from the high, short trails up from the low. The level only improves.
When the price reaches the level, the position is closed with a reduce-only market (journal role ``trailing``; size is
``size`` or the whole position). ``activation_price``: trailing starts once the price reaches it.

``exchange_stop=True``: a safeguard in case the process dies: the level is mirrored by a real SL on the
exchange (:meth:`Exchange.place_tp_sl`) that follows the level at most every ``min_interval`` seconds and
by at least ``min_move``. The move is **make-before-break**: first
the new SL, then the old one is canceled; if the new one fails, the old one stays; if the old one fails to cancel,
it is retried on subsequent steps (``extra['stale_stops']``). Bybit: a whole-position SL (trading-stop, Full)
is modified in place by the exchange, so :meth:`Exchange.modify_tp_sl` is used there instead of place+cancel (otherwise
the live stop would be removed; ``bytx.exchanges.bybit.tp_sl.SL_IN_PLACE``).

End: 'trailing' (client-side level hit), 'exchange_stop' (exchange SL triggered),
'position_closed' (position closed some other way). Pause freezes trailing (the exchange SL stays).
``cancel()`` cancels the exchange SL; an algorithm failure or task cancellation does not (protection stays).
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, Optional

from bytx.algos.base import EPS, TERMINAL_ORDER, ExecAlgo, check_positive, current_book
from bytx.errors import BytxError, ConfigError, NetworkError, OrderNotFound, PositionAlreadyClosed
from bytx.exchanges import tp_sl_in_place
from bytx.types import PositionSide, Side

if TYPE_CHECKING:  # pragma: no cover
    from bytx.exchange import Exchange
    from bytx.protection import TpSlOrder
    from bytx.trackers import PositionTracker


class TrailingStop(ExecAlgo):
    """Trailing stop (see module). ``position_side`` is 'long' / 'short'."""

    kind = 'trailing'

    def __init__(self, ex: 'Exchange', symbol: str, position_side: PositionSide, *,
                 distance: Optional[float] = None, distance_pct: Optional[float] = None,
                 activation_price: Optional[float] = None, size: Optional[float] = None,
                 price_source: Literal['mid', 'mark', 'last'] = 'mid', exchange_stop: bool = False,
                 min_move: Optional[float] = None, min_interval: float = 5.0,
                 position_tracker: Optional['PositionTracker'] = None, **kwargs: Any) -> None:
        if position_side not in ('long', 'short'):
            raise ConfigError(f"position_side: 'long' or 'short', not {position_side!r}", exchange=ex.name)
        kwargs.setdefault('role', 'trailing')
        super().__init__(ex, symbol, 'sell' if position_side == 'long' else 'buy', **kwargs)
        if (distance is None) == (distance_pct is None):
            raise ConfigError('exactly one of distance / distance_pct is required', exchange=ex.name)
        check_positive('distance', distance, ex, allow_none=True)
        check_positive('distance_pct', distance_pct, ex, allow_none=True)
        check_positive('activation_price', activation_price, ex, allow_none=True)
        check_positive('size', size, ex, allow_none=True)
        check_positive('min_move', min_move, ex, allow_none=True)
        if distance_pct is not None and distance_pct >= 100:
            raise ConfigError(f'distance_pct must be < 100, not {distance_pct!r}', exchange=ex.name)
        if price_source not in ('mid', 'mark', 'last'):
            raise ConfigError(f"price_source: 'mid' / 'mark' / 'last', not {price_source!r}", exchange=ex.name)
        if min_interval < 0:
            raise ConfigError(f'min_interval must be >= 0, not {min_interval!r}', exchange=ex.name)
        self.position_side = position_side
        self.distance = distance
        self.distance_pct = distance_pct
        self.activation_price = activation_price
        self.size = size
        self.price_source = price_source
        self.exchange_stop = exchange_stop
        self.min_move = min_move
        self.min_interval = float(min_interval)
        self.position_tracker = position_tracker
        self.active = activation_price is None
        self.extreme: Optional[float] = None
        self.level: Optional[float] = None
        self.stop: Optional['TpSlOrder'] = None
        self.stale: list[str] = []
        self._stop_moved_ms: Optional[int] = None

    def _counts(self, child: Any) -> bool:
        return child.tag in ('close', 'stop')

    # ─── price and position ─────────────────────────────────────────────────

    async def _price(self) -> Optional[float]:
        if self.price_source == 'mid':
            return (await current_book(self.ex, self.symbol)).mid
        streams = getattr(self.ex, '_streams', None)
        t = streams.last_ticker(self.symbol) if streams is not None else None
        if t is None:
            t = await self.ex.ticker(self.symbol)
        return (t.mark or t.last) if self.price_source == 'mark' else (t.last or t.mark)

    async def _position(self) -> Any:
        tr = self.position_tracker
        if tr is not None and tr.live:
            return tr.get(self.symbol, self.position_side)
        return await self.ex.position(self.symbol, self.position_side)

    def _level_for(self, extreme: float) -> float:
        d = self.distance if self.distance is not None else extreme * (self.distance_pct or 0.0) / 100
        raw = extreme - d if self.position_side == 'long' else extreme + d
        # round to the tick away from price (long down, short up): rounding never moves the stop closer
        return self._round_price(raw, 'buy' if self.position_side == 'long' else 'sell')

    def _better(self, new: float, old: Optional[float]) -> bool:
        if old is None:
            return True
        return new > old if self.position_side == 'long' else new < old

    # ─── loop ───────────────────────────────────────────────────────────────

    async def _run(self) -> None:
        self.extra.update(activated=self.active, moves=0)
        while True:
            await self._gate()
            try:
                pos = await self._position()
                price = await self._price()
            except NetworkError as e:
                self._error(e, 'price/position')
                await self._sleep(self.poll)
                continue
            self._ok()
            if pos is None:
                await self._finish_without_position()
                return
            if price is None:
                await self._sleep(self.poll)
                continue
            long = self.position_side == 'long'
            if not self.active:
                if self.activation_price is not None and (price >= self.activation_price if long
                                                          else price <= self.activation_price):
                    self.active = True
                    self.extra['activated'] = True
                    self.extra['activated_ms'] = self.clock.now_ms()
                else:
                    await self._sleep(self.poll)
                    continue
            if self.extreme is None or (price > self.extreme if long else price < self.extreme):
                self.extreme = price
                level = self._level_for(price)
                if self._better(level, self.level):
                    self.level = level
                    self.extra['stop_level'] = level
            assert self.level is not None
            if price <= self.level if long else price >= self.level:
                await self._close(pos)
                return
            if self.exchange_stop:
                await self._sync_exchange_stop()
            await self._sleep(self.poll)

    async def _finish_without_position(self) -> None:
        reason = 'position_closed'
        if self.stop is not None:
            try:
                got = await self.ex.tp_sl_order(self.symbol, self.stop.id)
                self._update(got)
                if got.state == 'triggered':
                    reason = 'exchange_stop'
                    self.stop = None
            except (OrderNotFound, NetworkError):
                pass
        await self._remove_stops()
        self._stop(reason)

    async def _close(self, pos: Any) -> None:
        size = min(self.size, float(pos.size)) if self.size is not None else float(pos.size)
        self.target = self._round_size(size)
        self.extra['triggered_price'] = self.level
        try:
            order = await self._place(side=self.side, size=self.target, type='market',  # type: ignore[arg-type]
                                      reduce_only=True, tag='close')
        except PositionAlreadyClosed:
            order = None
        while order is not None and order.status not in TERMINAL_ORDER:
            await self._sleep(self.poll)
            order = await self._refresh(order)
        if self._unknown:
            await self._resolve_unknown()
        await self._remove_stops()
        self._stop('trailing')

    # ─── exchange SL ────────────────────────────────────────────────────────

    async def _sync_exchange_stop(self) -> None:
        """Make-before-break: new SL → record → cancel the old one (Bybit Full: in-place replacement)."""
        if self.stale:
            await self._retry_stale()
        level = self.level
        if level is None:
            return
        cur = self.stop
        now = self.clock.now_ms()
        if cur is not None:
            step = self.min_move or 0.0
            old = float(cur.stop_loss or cur.trigger_price or 0.0)
            gain = (level - old) if self.position_side == 'long' else (old - level)
            if gain <= max(step, EPS):
                return
            if self._stop_moved_ms is not None and now - self._stop_moved_ms < self.min_interval * 1000:
                return
        size = self._round_size(self.size) if self.size is not None else None
        try:
            with self._bind(self.role):
                if cur is not None and size is None and tp_sl_in_place(self.ex.ccxt.id):
                    res = await self.ex.modify_tp_sl(self.symbol, cur.id, stop_loss=level)
                    if res.order is None:
                        raise RuntimeError(f'in-place SL move not confirmed: {res.old}/{res.new}')
                    new = res.order
                else:
                    new = await self.ex.place_tp_sl(self.symbol, 'stop_loss', level,
                                                    position_side=self.position_side, size=size)
        except (BytxError, RuntimeError) as e:     # leave the old SL alone
            self.extra['stop_errors'] = self.extra.get('stop_errors', 0) + 1
            self._error(e if isinstance(e, BytxError) else NetworkError(str(e)), 'move SL')
            return
        self._ok()
        self._add(new, self.role, None, 'stop')
        self.stop = new
        self._stop_moved_ms = now
        self.extra['moves'] = self.extra.get('moves', 0) + 1
        if cur is not None and cur.id != new.id and not (size is None and tp_sl_in_place(self.ex.ccxt.id)):
            self.stale.append(cur.id)
            await self._retry_stale()

    async def _retry_stale(self) -> None:
        if not self.stale:
            return
        ids = list(self.stale)
        try:
            await self.ex.cancel_tp_sl(self.symbol, order_ids=ids)
        except NetworkError as e:
            logger_debug(self, f'old SLs {ids} not canceled: {e!r}')
        left = []
        for i in ids:
            try:
                got = await self.ex.tp_sl_order(self.symbol, i)
            except OrderNotFound:
                continue
            except NetworkError:
                left.append(i)
                continue
            self._update(got)
            if got.state == 'open':
                left.append(i)
        self.stale = left
        self.extra['stale_stops'] = list(left)

    async def _remove_stops(self) -> None:
        if self.stop is not None and self.stop.id not in self.stale:
            self.stale.append(self.stop.id)
        self.stop = None
        for _ in range(self.max_errors):
            if not self.stale:
                return
            await self._retry_stale()

    async def _cleanup(self) -> None:
        for c in self._children:
            if c.tag == 'close' and c.order.status not in TERMINAL_ORDER:
                await self._cancel_child(c.order)
        if self._stop_reason is not None:       # cancel(): cancel SL; failure/task cancellation: keep it
            await self._remove_stops()


def logger_debug(algo: ExecAlgo, msg: str) -> None:
    from loguru import logger
    logger.debug(f'{algo.ex.name} {algo.id}: {msg}')


__all__ = ['TrailingStop']
