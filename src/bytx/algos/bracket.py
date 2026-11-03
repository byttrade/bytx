"""Bracket: entry + TP + SL, with OCO where the exchange does not link the legs itself.

1. **Entry**: market (no ``price``) or limit (``price``; ``entry_post_only``), journal role ``entry``.
   ``entry_timeout`` (s, by the algorithm's clock): the limit is canceled and the filled part is protected.
   Nothing filled: result ``done`` with reason 'entry_not_filled'.
2. **Protection**: :meth:`Exchange.set_tp_sl` on the filled size (``position_side`` and size come from
   the algorithm: the position is not read, foreign size is not protected), journal roles ``tp`` / ``sl``.
   If not all protection was placed (:class:`~bytx.errors.ProtectionNotPlaced` / error), by default
   (``on_protect_fail='close'``) placed legs are canceled, the filled entry is closed with a
   reduce-only market (role ``close``), result ``failed``; 'keep' leaves the position as is.
3. **OCO.** How the exchange links TP and SL is per its docs, see the ``OCO`` table in
   ``bytx.exchanges.<exchange>.tp_sl`` (:func:`bytx.exchanges.tp_sl_oco`): OKX places TP and SL as one
   algo order ('native_pair': only one leg can trigger); on Binance, Bybit (Partial legs), BingX,
   Hyperliquid (grouping "na"), KuCoin the legs are independent: **emulation**, the algorithm watches the legs
   (:meth:`Exchange.tp_sl_order`) and, once one triggers, cancels the other. Otherwise the remaining
   reduce-only leg would close the next position. ``oco='emulate'``: always watch and cancel.
   If the position closed some other way (manually, liquidation), the legs are canceled ('position_closed').

Result: ``reason`` 'take_profit' / 'stop_loss' / 'protection_triggered' (OKX pair: the exchange does not
say which leg) / 'position_closed' / 'protection_canceled' (legs canceled by someone else, position open);
``extra['oco']``: mode and docs link. ``filled`` / ``average`` refer to the entry.
Pause applies only until protection: the algorithm keeps watching the legs. ``cancel()`` after
entry cancels the legs (the position is left unprotected; ``close_on_cancel=True`` also closes it).
An algorithm failure or cancellation of its task (``task.cancel``, process shutdown) does **not** cancel the legs:
the position's protection stays on the exchange.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, Optional

from bytx.algos.base import EPS, TERMINAL_ORDER, ExecAlgo, _Child, check_positive
from bytx.errors import BytxError, ConfigError, NetworkError, OrderNotFound, PositionAlreadyClosed
from bytx.exchanges import tp_sl_oco
from bytx.types import Order, Side

if TYPE_CHECKING:  # pragma: no cover
    from bytx.exchange import Exchange
    from bytx.protection import TpSlOrder
    from bytx.trackers import PositionTracker

_LEG_DONE = frozenset({'triggered', 'canceled'})


class Bracket(ExecAlgo):
    """Bracket (see module). ``size`` in base; ``take_profit`` / ``stop_loss`` are trigger prices."""

    kind = 'bracket'

    def __init__(self, ex: 'Exchange', symbol: str, side: Side, *, size: float, take_profit: float,
                 stop_loss: float, price: Optional[float] = None, entry_timeout: Optional[float] = None,
                 entry_post_only: bool = False, oco: Literal['auto', 'emulate'] = 'auto',
                 trigger_by: Optional[str] = None, on_protect_fail: Literal['close', 'keep'] = 'close',
                 close_on_cancel: bool = False, position_tracker: Optional['PositionTracker'] = None,
                 **kwargs: Any) -> None:
        kwargs.setdefault('role', 'entry')
        kwargs.setdefault('poll', 1.0)
        super().__init__(ex, symbol, side, **kwargs)
        check_positive('size', size, ex)
        check_positive('take_profit', take_profit, ex)
        check_positive('stop_loss', stop_loss, ex)
        check_positive('price', price, ex, allow_none=True)
        check_positive('entry_timeout', entry_timeout, ex, allow_none=True)
        if oco not in ('auto', 'emulate'):
            raise ConfigError(f"oco: 'auto' or 'emulate', not {oco!r}", exchange=ex.name)
        if on_protect_fail not in ('close', 'keep'):
            raise ConfigError(f"on_protect_fail: 'close' or 'keep', not {on_protect_fail!r}", exchange=ex.name)
        lo, hi = (stop_loss, take_profit) if side == 'buy' else (take_profit, stop_loss)
        if not lo < hi or (price is not None and not lo < price < hi):
            raise ConfigError(f'bracket {side}: requires {"SL < price < TP" if side == "buy" else "TP < price < SL"} '
                              f'(TP {take_profit}, SL {stop_loss}, price {price})', exchange=ex.name)
        self.size = float(size)
        self.take_profit = float(take_profit)
        self.stop_loss = float(stop_loss)
        self.price = price
        self.entry_timeout = entry_timeout
        self.entry_post_only = entry_post_only
        self.oco = oco
        self.trigger_by = trigger_by
        self.on_protect_fail = on_protect_fail
        self.close_on_cancel = close_on_cancel
        self.position_tracker = position_tracker
        self.position_side = 'long' if side == 'buy' else 'short'
        self.legs: list['TpSlOrder'] = []
        self.native = False

    def _counts(self, child: _Child) -> bool:
        return child.tag == 'entry'

    # ─── stages ─────────────────────────────────────────────────────────────

    async def _run(self) -> None:
        mode, url, verified, note = tp_sl_oco(self.ex.ccxt.id)
        self.extra['oco'] = {'mode': mode, 'source': url, 'verified': verified, 'note': note}
        self.target = self._round_size(self.size)
        if not await self._enter():
            return
        filled = self._round_size(self.filled)
        if filled < max(self._min_size(), EPS):
            self._stop('entry_not_filled')
            return
        await self._protect(filled, mode)
        await self._watch()

    async def _enter(self) -> bool:
        side: Side = self.side  # type: ignore[assignment]
        await self._gate()
        entry = await self._place(side=side, size=self.target or 0.0, price=self.price,
                                  post_only=self.entry_post_only, tag='entry')
        deadline = self.clock.now_ms() + int(self.entry_timeout * 1000) if self.entry_timeout else None
        while True:
            if entry is None:
                if not await self._resolve_unknown():
                    await self._sleep(self.poll)
                    continue
                entry = next((c.order for c in self._children if c.tag == 'entry'), None)
                if entry is None:
                    self._stop('entry_not_filled')       # reconciled: order absent
                    return False
            if entry.status in TERMINAL_ORDER:
                return True
            if deadline is not None and self.clock.now_ms() >= deadline + self.paused_ms:
                done = await self._cancel_child(entry)
                if done is not None:
                    return True
            await self._sleep(self.poll)
            await self._gate()
            entry = await self._refresh(entry)

    async def _protect(self, filled: float, mode: str) -> None:
        tp, sl = self.take_profit, self.stop_loss
        try:
            with self._bind(None):
                legs = await self.ex.set_tp_sl(self.symbol, take_profit=tp, stop_loss=sl,
                                               position_side=self.position_side, size=filled,
                                               trigger_by=self.trigger_by)  # type: ignore[arg-type]
        except BytxError as e:
            placed = list((e.details or {}).get('placed') or []) if isinstance(e.details, dict) else []
            self._legs(placed)
            self.extra['protect_error'] = repr(e)
            if self.on_protect_fail == 'close':
                await self._cancel_legs()
                await self._close_position(filled)
            self.reason = 'protection_failed'
            raise
        self._legs(legs)
        self.native = (self.oco == 'auto' and mode == 'native_pair' and len(legs) == 1
                       and legs[0].kind == 'tp_sl')
        self.extra['oco']['emulated'] = not self.native

    def _legs(self, legs: list['TpSlOrder']) -> None:
        for leg in legs:
            tag = {'take_profit': 'tp', 'stop_loss': 'sl'}.get(leg.kind, 'tp_sl')
            self.legs.append(leg)
            self._add(leg, tag if tag != 'tp_sl' else 'tp', 0, tag)

    async def _watch(self) -> None:
        while True:
            self._check_stop()                  # pause does not stop leg watching, only cancel does
            fresh = []
            for leg in self.legs:
                try:
                    got = await self.ex.tp_sl_order(self.symbol, leg.id)
                except OrderNotFound:      # not on the exchange: canceled (or filled without a trace)
                    got = leg
                    got.state = 'canceled'
                except NetworkError as e:
                    self._error(e, f'read TP/SL {leg.id}')
                    got = leg
                else:
                    self._ok()
                    self._update(got)
                fresh.append(got)
            self.legs = fresh
            fired = [x for x in fresh if x.state == 'triggered']
            if fired:
                winner = fired[0]
                if not self.native:
                    await self._cancel_legs(exclude=winner.id)
                self.extra['triggered'] = winner.id
                self._stop({'take_profit': 'take_profit', 'stop_loss': 'stop_loss'}.get(winner.kind,
                                                                                        'protection_triggered'))
                return
            open_legs = [x for x in fresh if x.state not in _LEG_DONE]
            if not await self._position_open():
                await self._cancel_legs()
                self._stop('position_closed')
                return
            if not open_legs:
                self._stop('protection_canceled')
                return
            await self._sleep(self.poll)

    async def _position_open(self) -> bool:
        tr = self.position_tracker
        if tr is not None and tr.live:
            return tr.get(self.symbol, self.position_side) is not None
        try:
            pos = await self.ex.position(self.symbol, self.position_side)  # type: ignore[arg-type]
        except NetworkError as e:
            self._error(e, 'read position')
            return True
        self._ok()
        return pos is not None

    # ─── cancellation ───────────────────────────────────────────────────────

    async def _cancel_legs(self, exclude: Optional[str] = None) -> None:
        ids = [x.id for x in self.legs if x.id != exclude and x.state not in _LEG_DONE]
        if not ids:
            return
        for _ in range(self.max_errors):
            try:
                await self.ex.cancel_tp_sl(self.symbol, order_ids=ids)
            except NetworkError as e:
                self._error(e, 'cancel TP/SL')
            left = []
            for i in ids:
                try:
                    got = await self.ex.tp_sl_order(self.symbol, i)
                except OrderNotFound:
                    self._set_leg_state(i, 'canceled')
                    continue
                except NetworkError:
                    left.append(i)
                    continue
                self._update(got)
                self.legs = [got if x.id == i else x for x in self.legs]
                if got.state not in _LEG_DONE:
                    left.append(i)
            if not left:
                break
            ids = left
        else:
            raise RuntimeError(f'bracket: TP/SL {ids} not canceled; cancel them manually')

    def _set_leg_state(self, leg_id: str, state: str) -> None:
        for x in self.legs:
            if x.id == leg_id:
                x.state = state

    async def _close_position(self, size: float) -> None:
        close_side: Side = 'sell' if self.side == 'buy' else 'buy'
        try:
            await self._place(side=close_side, size=size, type='market', reduce_only=True, role='close',
                              tag='close')
        except PositionAlreadyClosed:
            pass

    async def _cleanup(self) -> None:
        for c in self._children:
            if c.tag == 'entry' and c.order.status not in TERMINAL_ORDER:
                await self._cancel_child(c.order)
        if self._stop_reason is None:
            return      # failure or task cancellation: the position's protection stays on the exchange
        await self._cancel_legs()
        if self.close_on_cancel:
            filled = self._round_size(self.filled)
            if filled > EPS and await self._position_open():
                await self._close_position(filled)

    @property
    def entry(self) -> Optional[Order]:
        return next((c.order for c in self._children if c.tag == 'entry'), None)


__all__ = ['Bracket']
