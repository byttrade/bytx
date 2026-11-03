"""Execution algorithms on top of :class:`~bytx.Exchange`.

    algo = ex.algos.chase('BTC/USDT:USDT', 'buy', size=0.5, max_moves=20)   # limit order chasing the best price
    algo.pause(); algo.resume()
    result = await algo                     # AlgoResult: status, reason, filled, average, children
    await ex.algos.twap('ETH/USDT:USDT', 'sell', size=10, duration=600, slices=20, randomize=0.3)
    await ex.algos.market('BTC/USDT:USDT', 'buy', size=0.2, max_slippage_bps=20)
    await ex.algos.bracket('BTC/USDT:USDT', 'buy', size=0.1, take_profit=72_000, stop_loss=66_000)
    ex.algos.trailing_stop('BTC/USDT:USDT', 'long', distance_pct=1.5)

Each ``ex.algos.*`` method starts the algorithm as a background task right away and returns its object
(:class:`~bytx.algos.base.ExecAlgo`): ``await algo`` for the result, ``algo.status``, ``algo.filled``,
``algo.children``, ``pause()`` / ``resume()`` / ``await cancel()``. Argument errors raise
:class:`~bytx.errors.ConfigError` immediately (no network). Works the same on a live exchange, paper and
replay (on paper, by the ``ex.clock`` clock). Algorithms stay off the ``place_order`` hot path:
until ``ex.algos`` is accessed, the module is not even imported.

Child orders are regular SDK orders: bot tag ``client_id_prefix``, broker tag, ``pre_submit``
hook, RiskGuard; with a position journal, each order gets a role (``role=``; bracket uses
``entry`` / ``tp`` / ``sl``, trailing uses ``trailing``) and ``position_id=``.
"""
from __future__ import annotations

import weakref
from typing import TYPE_CHECKING, Any, Optional

from bytx.algos.base import AlgoResult, AlgoStatus, ExecAlgo, current_book
from bytx.algos.bracket import Bracket
from bytx.algos.chase import Chase
from bytx.algos.iceberg import Iceberg
from bytx.algos.market import CappedMarket
from bytx.algos.trailing import TrailingStop
from bytx.algos.twap import TWAP
from bytx.types import PositionSide, Side

if TYPE_CHECKING:  # pragma: no cover
    from bytx.exchange import Exchange


class Algos:
    """Algorithm factory for one Exchange (``ex.algos``) and registry of running ones."""

    def __init__(self, ex: 'Exchange') -> None:
        self.ex = ex
        self._running: 'weakref.WeakSet[ExecAlgo]' = weakref.WeakSet()
        self._strong: set[ExecAlgo] = set()     # keeps live algorithms from being GC'd until done

    def _start(self, algo: ExecAlgo) -> Any:
        self._running.add(algo)
        self._strong.add(algo)
        algo.start()
        assert algo._task is not None
        algo._task.add_done_callback(lambda _t: self._strong.discard(algo))
        return algo

    @property
    def active(self) -> list[ExecAlgo]:
        """Unfinished algorithms of this Exchange."""
        return [a for a in self._running if not a.done]

    async def cancel_all(self, reason: str = 'user') -> list[AlgoResult]:
        """Cancel all unfinished algorithms (their working orders are canceled)."""
        return [await a.cancel(reason) for a in self.active]

    def market(self, symbol: str, side: Side, *, size: float, max_slippage_bps: float = 50.0,
               reference: str = 'best', retries: int = 0, reduce_only: bool = False, **kwargs: Any) -> CappedMarket:
        """Market with a slippage cap: limit IOC no worse than ``reference ± max_slippage_bps``.
        See :mod:`bytx.algos.market`."""
        return self._start(CappedMarket(self.ex, symbol, side, size=size, max_slippage_bps=max_slippage_bps,
                                        reference=reference, retries=retries,  # type: ignore[arg-type]
                                        reduce_only=reduce_only, **kwargs))

    def chase(self, symbol: str, side: Side, *, size: float, max_moves: int = 20,
              max_deviation_bps: Optional[float] = None, min_interval: float = 1.0, timeout: Optional[float] = None,
              post_only: bool = True, on_exhausted: str = 'stop', max_slippage_bps: float = 50.0,
              reduce_only: bool = False, **kwargs: Any) -> Chase:
        """Limit order at the best price on its side: re-placed until filled.
        See :mod:`bytx.algos.chase`."""
        return self._start(Chase(self.ex, symbol, side, size=size, max_moves=max_moves,
                                 max_deviation_bps=max_deviation_bps, min_interval=min_interval, timeout=timeout,
                                 post_only=post_only, on_exhausted=on_exhausted,  # type: ignore[arg-type]
                                 max_slippage_bps=max_slippage_bps, reduce_only=reduce_only, **kwargs))

    def twap(self, symbol: str, side: Side, *, size: float, duration: float, slices: int,
             randomize: float = 0.0, seed: Optional[int] = None, slice_type: str = 'market',
             max_slippage_bps: Optional[float] = None, reduce_only: bool = False, **kwargs: Any) -> TWAP:
        """Size in slices at equal (± ``randomize``) intervals over ``duration`` seconds.
        See :mod:`bytx.algos.twap`."""
        return self._start(TWAP(self.ex, symbol, side, size=size, duration=duration, slices=slices,
                                randomize=randomize, seed=seed, slice_type=slice_type,  # type: ignore[arg-type]
                                max_slippage_bps=max_slippage_bps, reduce_only=reduce_only, **kwargs))

    def iceberg(self, symbol: str, side: Side, *, size: float, price: float, visible: float,
                visible_jitter: float = 0.0, seed: Optional[int] = None, post_only: bool = False,
                reduce_only: bool = False, **kwargs: Any) -> Iceberg:
        """Limit order at ``price`` in visible slices of ``visible``. See :mod:`bytx.algos.iceberg`."""
        return self._start(Iceberg(self.ex, symbol, side, size=size, price=price, visible=visible,
                                   visible_jitter=visible_jitter, seed=seed, post_only=post_only,
                                   reduce_only=reduce_only, **kwargs))

    def bracket(self, symbol: str, side: Side, *, size: float, take_profit: float, stop_loss: float,
                price: Optional[float] = None, entry_timeout: Optional[float] = None, oco: str = 'auto',
                trigger_by: Optional[str] = None, on_protect_fail: str = 'close', close_on_cancel: bool = False,
                **kwargs: Any) -> Bracket:
        """Entry + TP + SL; OCO is emulated where the exchange does not cancel the sibling itself.
        See :mod:`bytx.algos.bracket`."""
        return self._start(Bracket(self.ex, symbol, side, size=size, take_profit=take_profit, stop_loss=stop_loss,
                                   price=price, entry_timeout=entry_timeout, oco=oco,  # type: ignore[arg-type]
                                   trigger_by=trigger_by, on_protect_fail=on_protect_fail,  # type: ignore[arg-type]
                                   close_on_cancel=close_on_cancel, **kwargs))

    def trailing_stop(self, symbol: str, position_side: PositionSide, *, distance: Optional[float] = None,
                      distance_pct: Optional[float] = None, activation_price: Optional[float] = None,
                      size: Optional[float] = None, price_source: str = 'mid', exchange_stop: bool = False,
                      min_move: Optional[float] = None, min_interval: float = 5.0,
                      **kwargs: Any) -> TrailingStop:
        """Client-side trailing stop for a position. See :mod:`bytx.algos.trailing`."""
        return self._start(TrailingStop(self.ex, symbol, position_side, distance=distance,
                                        distance_pct=distance_pct, activation_price=activation_price, size=size,
                                        price_source=price_source, exchange_stop=exchange_stop,  # type: ignore[arg-type]
                                        min_move=min_move, min_interval=min_interval, **kwargs))


__all__ = ['Algos', 'AlgoResult', 'AlgoStatus', 'Bracket', 'CappedMarket', 'Chase', 'ExecAlgo', 'Iceberg', 'TWAP',
           'TrailingStop', 'current_book']
