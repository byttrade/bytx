"""Risk-based position sizing: ``ex.risk_size`` and ``ex.open_position``.

Calculation (:func:`plan_position`, no network):

- entry price ``E``: market — the best opposite price (ask for long, bid for short) ± ``slippage_bps``;
    limit (``price=``, sent as IOC) — the price itself, it cannot fill worse;
- stop exit ``S``: the trigger on the price step ∓ ``slippage_bps`` (the stop executes as market);
- loss per 1 base coin = ``|E − S| + fee × (E + S)`` — taker fee on entry and exit;
- contracts = ``risk_usdt / (loss × contractSize)``, rounded down to the exchange amount step — actual
    risk ≤ ``risk_usdt``;
- minimum: ``min amount`` and ``min_cost`` (notional at the entry price, as in ``place_order``), rounded up to the step.
    If the minimum size risks more than ``risk_usdt`` — :class:`~bytx.errors.MinSizeExceedsRisk`.

Leverage does not affect risk (the stop loss is the same), but it defines margin (``notional / leverage``) and
the liquidation price: in isolated a stop beyond the liquidation estimate is rejected (the position would be liquidated before the stop),
margin above free collateral — :class:`~bytx.errors.InsufficientFunds` (``check_margin``).

The fee (``fee_rate``) defaults to the exchange's default-tier taker rate from its docs
(``bytx.exchanges.<exchange>.fees``; for paper — the paper account rate). A custom tier — ``fee_rate=``.

The module is imported on the first ``risk_size`` / ``open_position`` call: the
``place_order`` / WS hot path is unchanged.
"""
from __future__ import annotations

import contextlib
import dataclasses
from dataclasses import dataclass, field
from decimal import ROUND_UP, Decimal
from typing import TYPE_CHECKING, Any, Callable, Literal, Optional

from bytx.errors import (
    BytxError,
    ConfigError,
    InsufficientFunds,
    InvalidOrder,
    MinSizeExceedsRisk,
    NotSupported,
    OrderStatusUnknown,
    OrderTooSmall,
    PositionAlreadyClosed,
    PositionOpenError,
    ProtectionNotPlaced,
)
from bytx.sizing import snap_down
from bytx.types import Order, PositionSide, Side

if TYPE_CHECKING:  # pragma: no cover
    from bytx.exchange import Exchange
    from bytx.protection import TpSlOrder

StopMode = Literal['auto', 'separate']
_SIDES: dict[str, PositionSide] = {'long': 'long', 'short': 'short', 'buy': 'long', 'sell': 'short'}


@dataclass(frozen=True, slots=True)
class RiskPlan:
    """Risk-based sizing (before sending). Amounts: ``contracts`` — exchange units, ``size`` — base.

    ``risk`` — the loss if the stop triggers at this size (after rounding, with entry and exit fees
    and slippage), ≤ ``risk_target``; ``fees`` — its fee part. ``entry_price`` /
    ``stop_exit_price`` — expected prices with slippage, ``stop_price`` — the trigger on the price step.
    ``min_contracts`` / ``min_risk`` — the exchange minimum size and its risk. ``margin`` and
    ``liquidation_price`` (isolated estimate: ``E × (1 ∓ 1/leverage ± mmr)``) — None without leverage.
    """

    symbol: str
    side: PositionSide
    risk_target: float
    risk: float
    risk_per_contract: float
    fees: float
    contracts: float
    size: float
    notional: float
    entry_price: float
    stop_price: float
    stop_exit_price: float
    fee_rate: float
    fee_source: str
    slippage_bps: float
    min_contracts: float
    min_risk: float
    leverage: Optional[float] = None
    margin: Optional[float] = None
    liquidation_price: Optional[float] = None
    take_profit: Optional[float] = None
    warnings: tuple[str, ...] = ()

    @property
    def order_side(self) -> Side:
        return 'buy' if self.side == 'long' else 'sell'

    @property
    def risk_used(self) -> float:
        """Share of the target risk taken by the size after rounding (0..1)."""
        return self.risk / self.risk_target if self.risk_target else 0.0


@dataclass(slots=True)
class OpenPositionResult:
    """Result of :meth:`Exchange.open_position`.

    ``status``: 'opened' — the position is open and protected by a stop; 'not_filled' — the limit IOC did not fill,
    no position. ``stop_mode``: 'attached' — the SL went in the same request as the entry (per the exchange docs,
    :func:`bytx.exchanges.attached_sl`; ``stop`` / ``take_profit`` are then None — they are part of the entry
    order; on a partial entry fill the SL is verified on the exchange and present in ``stop``; if it was not placed, the stop is placed
    separately, mode 'separate', with a warning in ``warnings``), 'separate' — SL as a separate reduce-only order right after the entry (``stop``; for an OKX pair
    ``stop is take_profit`` — a single algo order). ``filled_contracts`` / ``filled_size`` / ``average`` —
    the entry fill; ``risk`` — the actual risk (average entry price, filled size), ``plan`` — the plan
    before sending. ``warnings`` — e.g. TP not placed (the stop is in place).
    """

    status: Literal['opened', 'not_filled']
    plan: RiskPlan
    entry: Order
    stop_mode: Literal['attached', 'separate']
    stop: Optional['TpSlOrder']
    take_profit: Optional['TpSlOrder']
    filled_contracts: float
    filled_size: float
    average: Optional[float]
    risk: float
    warnings: list[str] = field(default_factory=list)


# ─── calculation without network ──────────────────────────────────────────

def _ceil_step(value: float, step: Optional[float]) -> float:
    if not step or value <= 0:
        return value
    d, s = Decimal(str(value)), Decimal(str(step))
    return float((d / s).to_integral_value(rounding=ROUND_UP) * s)


def _floor_step(value: float, step: Optional[float]) -> float:
    if not step:
        return value
    return float(snap_down(Decimal(str(value)), Decimal(str(step))))


def plan_position(*, symbol: str, side: str, risk_usdt: float, entry: float, stop: float,
                  contract_size: float = 1.0, amount_step: Optional[float] = None,
                  min_amount: Optional[float] = None, min_cost: Optional[float] = None,
                  fee_rate: float = 0.0, fee_source: str = 'user', slippage_bps: float = 0.0,
                  entry_is_limit: bool = False, leverage: Optional[float] = None,
                  maintenance_margin_rate: Optional[float] = None, take_profit: Optional[float] = None,
                  round_down: Optional[Callable[[float], float]] = None,
                  exchange: Optional[str] = None) -> RiskPlan:
    """Risk-based size from the stop — a pure function (see the module). ``entry`` — the reference entry price
    (best opposite or the limit price), ``stop`` — the stop trigger already on the price step. ``round_down`` —
    rounds contracts down per exchange rules (default — by ``amount_step``).

    A stop on the wrong side of the entry — :class:`InvalidOrder`; if the minimum size risks more than
    ``risk_usdt`` — :class:`MinSizeExceedsRisk` (``details``: ``min_contracts``, ``min_risk``,
    ``risk_usdt``, ``risk_per_contract``)."""
    pos_side = _SIDES.get(side)
    if pos_side is None:
        raise ConfigError(f"side: 'long' / 'short' (or 'buy' / 'sell'), not {side!r}", exchange=exchange)
    for name, value in (('risk_usdt', risk_usdt), ('entry', entry), ('stop', stop), ('contract_size', contract_size)):
        if value is None or not value > 0:
            raise ConfigError(f'{name} must be > 0, not {value!r}', exchange=exchange)
    if fee_rate < 0 or slippage_bps < 0:
        raise ConfigError(f'fee_rate and slippage_bps must be ≥ 0 (fee_rate={fee_rate}, slippage_bps={slippage_bps})',
                          exchange=exchange)
    long = pos_side == 'long'
    slip = slippage_bps / 10_000.0
    entry_eff = entry if entry_is_limit else entry * (1 + slip if long else 1 - slip)
    stop_eff = stop * (1 - slip if long else 1 + slip)
    if (long and not stop < entry) or (not long and not stop > entry):
        raise InvalidOrder(f'stop {stop} must be {"below" if long else "above"} the entry price {entry} ({symbol} '
                           f'{pos_side})', exchange=exchange,
                           details={'symbol': symbol, 'side': pos_side, 'stop': stop, 'entry': entry})
    if take_profit is not None and ((long and not take_profit > entry) or (not long and not take_profit < entry)):
        raise InvalidOrder(f'take_profit {take_profit} must be {"above" if long else "below"} the entry price {entry} '
                           f'({symbol} {pos_side})', exchange=exchange,
                           details={'symbol': symbol, 'side': pos_side, 'take_profit': take_profit, 'entry': entry})
    per_base = abs(entry_eff - stop_eff) + fee_rate * (entry_eff + stop_eff)
    per_contract = per_base * contract_size

    def down(c: float) -> float:
        if round_down is not None:
            try:
                return round_down(c)
            except OrderTooSmall:
                return 0.0
        return _floor_step(c, amount_step)

    min_c = max(min_amount or 0.0, amount_step or 0.0)
    if min_cost:
        min_c = max(min_c, _ceil_step(min_cost / (contract_size * entry), amount_step))
    min_c = _ceil_step(min_c, amount_step) if min_c > 0 else min_c
    min_risk = min_c * per_contract
    contracts = down(risk_usdt / per_contract)
    if contracts <= 0 or contracts < min_c * (1 - 1e-9):
        raise MinSizeExceedsRisk(
            f'minimum size {min_c} contracts risks {min_risk:.4f} > risk_usdt {risk_usdt} '
            f'({symbol} {pos_side}: stop {stop}, entry ≈{entry_eff:.8g})', exchange=exchange,
            details={'symbol': symbol, 'side': pos_side, 'risk_usdt': risk_usdt, 'min_contracts': min_c,
                     'min_risk': min_risk, 'risk_per_contract': per_contract})
    size = contracts * contract_size
    notional = size * entry_eff
    warnings: list[str] = []
    margin = liq = None
    if leverage:
        margin = notional / leverage
        mmr = maintenance_margin_rate or 0.0
        liq = entry_eff * (1 - 1 / leverage + mmr) if long else entry_eff * (1 + 1 / leverage - mmr)
    return RiskPlan(symbol=symbol, side=pos_side, risk_target=float(risk_usdt), risk=contracts * per_contract,
                    risk_per_contract=per_contract, fees=size * fee_rate * (entry_eff + stop_eff),
                    contracts=contracts, size=size, notional=notional, entry_price=entry_eff, stop_price=stop,
                    stop_exit_price=stop_eff, fee_rate=fee_rate, fee_source=fee_source,
                    slippage_bps=slippage_bps, min_contracts=min_c, min_risk=min_risk, leverage=leverage,
                    margin=margin, liquidation_price=liq, take_profit=take_profit, warnings=tuple(warnings))


def realized_risk(plan: RiskPlan, contracts: float, contract_size: float, average: Optional[float]) -> float:
    """Actual risk after the fill: filled size, average entry price (if absent — the expected one from the plan)."""
    e = average or plan.entry_price
    s = plan.stop_exit_price
    return contracts * contract_size * (abs(e - s) + plan.fee_rate * (e + s))


# ─── Exchange: calculation with exchange data ─────────────────────────────

def _fee_rate(ex: 'Exchange', fee_rate: Optional[float]) -> tuple[float, str]:
    if fee_rate is not None:
        return float(fee_rate), 'user'
    paper_fees = getattr(ex, 'fees', None)
    if getattr(ex, 'is_paper', False) and paper_fees is not None:
        return float(paper_fees.taker), 'paper'
    from bytx.paper.fees import default_fees
    try:
        fees = default_fees(ex.ccxt.id, ex.market_type)
    except ConfigError:
        return 0.0, 'unknown'
    return float(fees.taker), fees.source or 'docs'


async def _plan(ex: 'Exchange', symbol: str, side: str, *, risk_usdt: float, stop: float,
                leverage: Optional[float], price: Optional[float], take_profit: Optional[float],
                fee_rate: Optional[float], slippage_bps: float,
                maintenance_margin_rate: Optional[float] = None) -> RiskPlan:
    from bytx import convert
    from bytx.markets import to_market
    pos_side = _SIDES.get(side)
    if pos_side is None:
        raise ConfigError(f"side: 'long' / 'short' (or 'buy' / 'sell'), not {side!r}", exchange=ex.name)
    await ex.connect()
    s = ex.symbol(symbol)
    m = to_market(ex.ccxt.market(s), ex.ccxt.id)
    if not m.is_contract:
        raise NotSupported(f'open_position / risk_size — derivatives only (reduce-only stop), not {s}',
                           exchange=ex.name)
    if not stop or stop <= 0:
        raise ConfigError(f'stop must be > 0, not {stop!r}', exchange=ex.name)
    order_side: Side = 'buy' if pos_side == 'long' else 'sell'
    entry = (convert.price_to_precision(ex.ccxt, m, price, order_side) if price is not None
             else await ex._reference_price(s, order_side))
    rate, source = _fee_rate(ex, fee_rate)
    lev = leverage if leverage is not None else ex._leverage.get(s)

    def round_down(c: float) -> float:
        return convert.contracts_to_precision(ex.ccxt, m, c)

    return plan_position(symbol=s, side=pos_side, risk_usdt=risk_usdt, entry=entry,
                         stop=ex.round_price(s, stop), contract_size=m.contract_size or 1.0,
                         amount_step=m.amount_step, min_amount=m.min_amount, min_cost=m.min_cost,
                         fee_rate=rate, fee_source=source, slippage_bps=slippage_bps,
                         entry_is_limit=price is not None, leverage=lev,
                         maintenance_margin_rate=maintenance_margin_rate,
                         take_profit=ex.round_price(s, take_profit) if take_profit is not None else None,
                         round_down=round_down, exchange=ex.name)


async def risk_size(ex: 'Exchange', symbol: str, side: str, *, risk_usdt: float, stop: float,
                    leverage: Optional[float] = None, price: Optional[float] = None,
                    take_profit: Optional[float] = None, fee_rate: Optional[float] = None,
                    slippage_bps: float = 0.0) -> RiskPlan:
    return await _plan(ex, symbol, side, risk_usdt=risk_usdt, stop=stop, leverage=leverage, price=price,
                       take_profit=take_profit, fee_rate=fee_rate, slippage_bps=slippage_bps)


def _bind(ex: 'Exchange', position_id: Optional[str], role: Optional[str]) -> Any:
    journal = ex.journal
    if journal is None:
        return contextlib.nullcontext()
    return journal.bind(position_id=position_id, role=role)


async def _leverage(ex: 'Exchange', s: str, pos_side: PositionSide, leverage: Optional[float],
                    warnings: list[str]) -> Optional[float]:
    if leverage is not None:
        if not leverage > 0:
            raise ConfigError(f'leverage must be > 0, not {leverage!r}', exchange=ex.name)
        if ex._leverage.get(s) == leverage:
            return float(leverage)
        applied = await ex.set_leverage(s, leverage, allow_open_position=True)  # no position — already checked
        if applied is not None and applied < leverage:
            warnings.append(f'leverage {leverage} capped by the exchange to {applied} (tier)')
        return float(applied if applied is not None else leverage)
    known = ex._leverage.get(s)
    if known:
        return float(known)
    try:
        got = await ex.leverage(s, pos_side)
    except BytxError as e:
        warnings.append(f'leverage not read ({e.message or e}): margin and liquidation not checked')
        return None
    if not got:
        warnings.append('the exchange does not report leverage: margin and liquidation not checked')
    return float(got) if got else None


async def _entry_fill(ex: 'Exchange', s: str, order: Order, pos_side: PositionSide) -> tuple[Order, float]:
    """Filled entry contracts: order response → re-read order → position (no pauses)."""
    if order.status in ('closed', 'canceled', 'expired', 'rejected') and order.filled_contracts is not None:
        return order, float(order.filled_contracts)
    try:
        order = await ex.order(s, order.id)
    except BytxError:
        pass
    if order.status in ('closed', 'canceled', 'expired', 'rejected') and order.filled_contracts is not None:
        return order, float(order.filled_contracts)
    pos = await ex.position(s, pos_side)
    return order, float(pos.contracts) if pos is not None else float(order.filled_contracts or 0.0)


async def open_position(ex: 'Exchange', symbol: str, side: str, *, risk_usdt: float, stop: float,
                        leverage: Optional[float] = None, take_profit: Optional[float] = None,
                        price: Optional[float] = None, fee_rate: Optional[float] = None,
                        slippage_bps: float = 0.0, stop_mode: StopMode = 'auto',
                        trigger_by: Optional[str] = None, on_stop_fail: Literal['close', 'keep'] = 'close',
                        check_margin: bool = True, client_id: Optional[str] = None,
                        position_id: Optional[str] = None) -> OpenPositionResult:
    from bytx.exchanges import attached_sl
    from bytx.protection import find_leg
    pos_side = _SIDES.get(side)
    if pos_side is None:
        raise ConfigError(f"side: 'long' / 'short' (or 'buy' / 'sell'), not {side!r}", exchange=ex.name)
    if stop_mode not in ('auto', 'separate'):
        raise ConfigError(f"stop_mode: 'auto' or 'separate', not {stop_mode!r}", exchange=ex.name)
    if on_stop_fail not in ('close', 'keep'):
        raise ConfigError(f"on_stop_fail: 'close' or 'keep', not {on_stop_fail!r}", exchange=ex.name)
    await ex.connect()
    s = ex.symbol(symbol)
    if not ex.ccxt.market(s).get('contract'):
        raise NotSupported(f'open_position — derivatives only (reduce-only stop), not {s}', exchange=ex.name)
    # only a new position is opened: the position SL (Bybit Full) and the risk are computed for it alone;
    # in one-way an opposite position would be reduced rather than opened
    hedge = await ex.is_hedge_mode(s)
    busy = [p for p in await ex.positions([s]) if not hedge or p.side == pos_side]
    if busy:
        raise PositionOpenError(f'{s} already has an open position {busy[0].side} {busy[0].contracts}: open_position '
                                f'opens a new one (to add to a position use place_order)', exchange=ex.name,
                                details={'symbol': s, 'positions': busy})
    warnings: list[str] = []
    lev = await _leverage(ex, s, pos_side, leverage, warnings)
    mode = ex.margin_mode_of(s)
    mmr = None
    if lev and mode == 'isolated':
        try:
            mmr = await ex.maintenance_margin_rate(s)
        except BytxError:
            warnings.append('maintenance margin rate not read: liquidation estimated without it')
    plan = await _plan(ex, s, pos_side, risk_usdt=risk_usdt, stop=stop, leverage=lev, price=price,
                       take_profit=take_profit, fee_rate=fee_rate, slippage_bps=slippage_bps,
                       maintenance_margin_rate=mmr)
    if plan.liquidation_price is not None and mode == 'isolated':
        beyond = (plan.stop_price <= plan.liquidation_price if pos_side == 'long'
                  else plan.stop_price >= plan.liquidation_price)
        if beyond:
            raise InvalidOrder(f'stop {plan.stop_price} is beyond the liquidation estimate {plan.liquidation_price:.8g} '
                               f'(isolated, leverage {lev}): the position would be liquidated before the stop — lower the leverage',
                               exchange=ex.name, details={'plan': plan})
    if check_margin and plan.margin is not None:
        free = await ex.free_collateral(s)
        need = plan.margin + plan.notional * plan.fee_rate
        if need > free:
            raise InsufficientFunds(f'margin required {need:.4f} (notional {plan.notional:.4f} / leverage {lev} + fee), '
                                    f'free {free:.4f} ({s})', exchange=ex.name,
                                    details={'plan': plan, 'free': free, 'required': need})
    if fee_rate is None and plan.fee_source == 'unknown':
        warnings.append('exchange fee unknown — risk computed without it (pass fee_rate=)')
    attach = (stop_mode == 'auto' and price is None and trigger_by is None and attached_sl(ex.ccxt.id)[0])
    cs = plan.size / plan.contracts if plan.contracts else 1.0
    unknown_fill: Optional[float] = None
    store = ex.intents
    # client_id of a separate stop — upfront, into the plan (sent to the exchange as is; where it is not sent — None)
    stop_cid = None if attach or store is None else _stop_client_id(ex, plan, position_id)
    # protection plan (both modes) goes into the entry intent BEFORE sending: if the process dies between entry and stop,
    # recover() reconciles exchange TP/SL (the attached one may have been placed) and places the missing one
    plan_cm = (contextlib.nullcontext() if store is None else
               store.protection(_plan_record(plan, pos_side, trigger_by, attach, stop_cid)))
    try:
        with _bind(ex, position_id, 'entry'), plan_cm:
            entry = await ex.place_order(s, plan.order_side, contracts=plan.contracts, price=price,
                                         time_in_force='IOC' if price is not None else None, client_id=client_id,
                                         stop_loss=plan.stop_price if attach else None,
                                         take_profit=plan.take_profit if attach else None)
    except OrderStatusUnknown as e:
        # entry outcome unknown: no position — the error as is; a position exists — the entry went through, protect with a separate
        # stop (the attached one may have been placed with the entry — then there are two stops, both reduce-only)
        pos = await ex.position(s, pos_side)
        if pos is None:
            raise
        entry = _unknown_entry(e, s, plan, price, pos)
        unknown_fill = float(pos.contracts)
        if attach:
            # the attached SL may have been placed with the entry — check on the exchange (OKX too: the fill
            # is unknown), if present — do not place a second one
            found = await _attached_legs(ex, s, pos_side, trust_docs=False)
            stop_leg = find_leg(ex, found, 'stop_loss', pos_side, plan.stop_price)
            if stop_leg is not None:
                warnings.append(f'entry outcome unknown ({e.message or e}), position found — the attached '
                                f'stop is on the exchange')
                if store is not None and entry.client_id:
                    store._on_protected(ex, entry.client_id)
                risk = realized_risk(plan, unknown_fill, cs, entry.average)
                return OpenPositionResult('opened', plan, entry, 'attached', stop_leg,
                                          find_leg(ex, found, 'take_profit', pos_side, plan.take_profit),
                                          unknown_fill, unknown_fill * cs, entry.average, risk, warnings)
        warnings.append(f'entry outcome unknown ({e.message or e}), position found — stop placed separately')
        if attach and store is not None and entry.client_id:
            stop_cid = _stop_client_id(ex, plan, position_id)
            store._set_plan(ex, entry.client_id, _plan_record(plan, pos_side, trigger_by, False, stop_cid))
        attach = False
    if unknown_fill is not None:
        filled = unknown_fill
    else:
        entry, filled = await _entry_fill(ex, s, entry, pos_side)
    if filled <= 0:
        if store is not None and entry.client_id:
            store._on_protected(ex, entry.client_id)   # no position — nothing to protect
        return OpenPositionResult('not_filled', plan, entry, 'attached' if attach else 'separate', None, None,
                                  0.0, 0.0, entry.average, 0.0, warnings)
    risk = realized_risk(plan, filled, cs, entry.average)
    protect_plan = plan
    if attach and filled < plan.contracts - 1e-12:
        # partial fill: the attached SL may not have been placed (OKX attachAlgoOrds — only after
        # the entry is fully filled, ATTACHED_SL) — check; if absent — a separate stop, as in the separate path
        found = await _attached_legs(ex, s, pos_side)
        stop_leg = find_leg(ex, found, 'stop_loss', pos_side, plan.stop_price)
        if stop_leg is not None:
            tp_leg = find_leg(ex, found, 'take_profit', pos_side, plan.take_profit)
            if store is not None and entry.client_id:
                store._on_protected(ex, entry.client_id)
            return OpenPositionResult('opened', plan, entry, 'attached', stop_leg, tp_leg, filled, filled * cs,
                                      entry.average, risk, warnings)
        tp_only = find_leg(ex, found, 'take_profit', pos_side, plan.take_profit) is not None
        warnings.append(f'entry partially filled ({filled} of {plan.contracts}): the attached SL was not placed — '
                        f'stop placed separately for the filled size')
        attach = False
        if tp_only:
            protect_plan = _without_tp(plan)
        if store is not None and entry.client_id:
            stop_cid = _stop_client_id(ex, protect_plan, position_id)
            store._set_plan(ex, entry.client_id, _plan_record(protect_plan, pos_side, trigger_by, False, stop_cid))
    if attach:
        if store is not None and entry.client_id:
            store._on_protected(ex, entry.client_id)   # the SL went in the entry request (per the exchange docs)
        return OpenPositionResult('opened', plan, entry, 'attached', None, None, filled, filled * cs,
                                  entry.average, risk, warnings)
    stop_order, tp_order = await _protect(ex, s, pos_side, protect_plan, entry, filled, trigger_by, on_stop_fail,
                                          position_id, warnings, stop_cid)
    return OpenPositionResult('opened', plan, entry, 'separate', stop_order, tp_order, filled, filled * cs,
                              entry.average, risk, warnings)


async def _attached_legs(ex: 'Exchange', s: str, pos_side: PositionSide, *,
                         trust_docs: bool = True) -> list['TpSlOrder']:
    """TP/SL attached to the entry on the exchange. OKX — per the docs they are absent on a partial fill (no request,
    ``trust_docs``; fill unknown — a request); if the exchange did not respond — assume absent (an extra
    reduce-only stop is safer than a position without a stop)."""
    if trust_docs and ex.ccxt.id == 'okx':
        return []
    try:
        return list(await ex.tp_sl_orders(s, pos_side))
    except BytxError:
        return []


def _plan_record(plan: RiskPlan, pos_side: str, trigger_by: Optional[str], attach: bool,
                 stop_cid: Optional[str]) -> dict:
    """Stop plan for the entry intent (:class:`~bytx.recovery.Intent` ``tp_sl``)."""
    return {'stop_loss': plan.stop_price, 'take_profit': plan.take_profit, 'trigger_by': trigger_by,
            'mode': 'attached' if attach else 'separate', 'side': pos_side, 'stop_client_id': stop_cid}


def _stop_client_id(ex: 'Exchange', plan: RiskPlan, position_id: Optional[str]) -> Optional[str]:
    """client_id of a separate stop upfront — only where it is sent to the exchange as is and does not change the
    order kind: Bybit trading-stop does not accept an id, BingX — only for LIMIT/MARKET (Place order), OKX with an id
    places TP and SL separately (not as a pair)."""
    if ex.ccxt.id in ('bybit', 'bingx') or (ex.ccxt.id == 'okx' and plan.take_profit is not None):
        return None
    with _bind(ex, position_id, None):
        return ex._new_client_id('sl')


def _without_tp(plan: RiskPlan) -> RiskPlan:
    """Plan without TP: an attached TP is already on the exchange — only the SL is placed separately."""
    return dataclasses.replace(plan, take_profit=None)


def _unknown_entry(e: OrderStatusUnknown, s: str, plan: RiskPlan, price: Optional[float], pos: Any) -> Order:
    """Entry order with an unknown outcome, rebuilt from the found position (status None)."""
    return Order(id=e.exchange_order_id or '', client_id=e.client_order_id, symbol=s, side=plan.order_side,
                 type='limit' if price is not None else 'market', status=None, price=price,
                 average=pos.entry_price, size=pos.size, filled=pos.size, remaining=0.0, reduce_only=False,
                 post_only=False, time_in_force='IOC' if price is not None else None, trigger_price=None,
                 fee=None, fee_currency=None, timestamp=None, contracts=pos.contracts,
                 filled_contracts=pos.contracts, remaining_contracts=0.0)


async def _protect(ex: 'Exchange', s: str, pos_side: PositionSide, plan: RiskPlan, entry: Order, filled: float,
                   trigger_by: Optional[str], on_stop_fail: str, position_id: Optional[str],
                   warnings: list[str], stop_cid: Optional[str] = None
                   ) -> tuple[Optional['TpSlOrder'], Optional['TpSlOrder']]:
    """SL (and TP) as separate reduce-only orders for the filled size; if the SL is not placed — rollback."""
    placed: list['TpSlOrder'] = []
    error: Optional[BaseException] = None
    store, cid = ex.intents, entry.client_id
    try:
        with _bind(ex, position_id, None), (store.parent(cid) if store is not None and cid
                                             else contextlib.nullcontext()):
            placed = await ex.set_tp_sl(s, stop_loss=plan.stop_price, take_profit=plan.take_profit,
                                        position_side=pos_side, contracts=filled,
                                        trigger_by=trigger_by,  # type: ignore[arg-type]
                                        stop_loss_client_id=stop_cid)
    except BytxError as e:
        error = e
        got = e.details.get('placed') if isinstance(e.details, dict) else None
        placed = list(got or [])
    stop_order = next((o for o in placed if o.kind in ('stop_loss', 'tp_sl')), None)
    tp_order = next((o for o in placed if o.kind in ('take_profit', 'tp_sl')), None)
    if stop_order is not None:
        if error is not None:
            warnings.append(f'TP not placed: {error}')
        if store is not None and cid:
            store._on_protected(ex, cid)
        return stop_order, tp_order
    assert error is not None
    cause = error.__cause__ if isinstance(error, ProtectionNotPlaced) and error.__cause__ else error
    details: dict = {'order': entry, 'placed': placed, 'plan': plan, 'filled_contracts': filled,
                     'rolled_back': False, 'close': None, 'close_error': None}
    if on_stop_fail == 'close':
        try:
            if placed:
                await ex.cancel_tp_sl(s, order_ids=[o.id for o in placed])
            with _bind(ex, position_id, 'close'):
                details['close'] = await ex.place_order(s, 'sell' if pos_side == 'long' else 'buy',
                                                        contracts=filled, reduce_only=True)
            details['rolled_back'] = True
        except PositionAlreadyClosed:
            details['rolled_back'] = True
        except BytxError as e:
            details['close_error'] = e
    if details['rolled_back'] and store is not None and cid:
        store._on_protected(ex, cid)   # no position — nothing to protect
    elif store is not None and cid:
        store._retire_plan(ex, cid, 'protection_failed')   # the failure is reported to the caller
    what = ('position closed (rolled back)' if details['rolled_back']
            else 'position LEFT without a stop' + (f': rollback failed ({details["close_error"]})'
                                                   if details['close_error'] else ''))
    raise ProtectionNotPlaced(f'entry {entry.id} filled ({filled} contracts), stop not placed: {cause}; {what}',
                              exchange=ex.name, code=getattr(cause, 'code', None), details=details) from cause


__all__ = ['RiskPlan', 'OpenPositionResult', 'plan_position', 'realized_risk', 'risk_size', 'open_position']
