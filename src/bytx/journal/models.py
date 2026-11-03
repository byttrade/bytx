"""Position journal models.

:class:`JournalEvent` — a single position event (append-only), :class:`PositionRecord` — a summary
built from events. Serialization — :meth:`JournalEvent.to_dict` / :meth:`JournalEvent.from_dict`
(a store document, e.g. of the ``position_journal`` collection): schema version ``_v``, unknown
keys are ignored on read (new fields are only ever added).
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field, fields
from typing import Any, Iterable, Optional

from bytx.errors import ConfigError

JOURNAL_SCHEMA_VERSION = 1

# + 'triggered' (TP/SL fired: the exchange turned the trigger into a market order)
# + 'intervention' (intervention in the position — kind in ``kind``, see INTERVENTION_KINDS)
# + 'tp_sl_set' — we placed a position TP/SL without a client_id (Bybit trading-stop, Gate price_orders):
#   role tp/sl, position side, ``trigger_price``; the exchange's conditional order without cid is matched to it
#   by symbol, side, role and trigger (otherwise it would look foreign). Not an order — absent from the order summary.
EVENTS = frozenset({
    'intent', 'placed', 'replaced', 'triggered', 'filled', 'partially_filled', 'canceled', 'rejected',
    'expired', 'position_opened', 'increased', 'reduced', 'closed', 'liquidated', 'adl', 'funding',
    'fee', 'drift', 'intervention', 'tp_sl_set',
})
# Intervention kind (``JournalEvent.kind`` of an ``intervention`` event):
# foreign_order — an order/fill of foreign origin in the position (manual, exchange UI,
# third-party software, another platform bot, unknown); liquidation / adl / settlement — the exchange closed
# the position itself; external_cancel / external_amend — our order canceled / amended not by us;
# position_drift — the exchange size diverged from the sum of our fills.
INTERVENTION_KINDS = frozenset({'foreign_order', 'liquidation', 'adl', 'settlement', 'external_cancel',
                                'external_amend', 'position_drift'})
# Internal ``drift`` event kinds: exchange history is incomplete (the position is marked ``incomplete``)
DRIFT_KINDS = frozenset({'position_drift', 'history_incomplete'})
ORDER_EVENTS = frozenset({'intent', 'placed', 'replaced', 'triggered', 'filled', 'partially_filled',
                          'canceled', 'rejected', 'expired'})
POSITION_EVENTS = frozenset({'position_opened', 'increased', 'reduced', 'closed', 'liquidated', 'adl'})
# + 'unknown' — no source marker, no false confidence
ORIGINS = frozenset({
    'own', 'platform', 'bot_legacy', 'manual', 'exchange_ui', 'foreign_api', 'liquidation', 'adl',
    'funding', 'settlement', 'unknown',
})
# + 'legacy_dca' (old bot cid: tag + hex — the role cannot be derived from it)
ROLES = frozenset({'entry', 'dca', 'tp', 'sl', 'tp_reduce', 'trailing', 'close', 'liquidation',
                   'legacy_dca'})
CONFIDENCE = frozenset({'exact', 'heuristic'})


@dataclass(slots=True)
class JournalEvent:
    """Position journal event.

    The store assigns ``seq`` (monotonic within ``(bot_id, position_id)``); ``key`` is the
    idempotency key: a repeated event (WS, REST reconciliation, restart) does not duplicate the record.
    Default ``(bot_id, exchange_order_id, event, trade_id)``, see :meth:`default_key`.
    Amounts (``amount``, ``filled``, ``contracts_after``) are in base coin, as in :class:`~bytx.types.Order`.
    ``kind`` — ``intervention`` / ``drift`` kind; ``order_type`` (limit, market,
    stop_market, …), ``trigger_price``, ``reduce_only`` — for reports and mapping into application formats.
    """

    position_id: str
    event: str
    origin: str = 'unknown'
    ts: int = 0
    seq: Optional[int] = None
    bot_id: Optional[str] = None
    role: Optional[str] = None
    level_id: Optional[int] = None
    exchange: Optional[str] = None
    symbol: Optional[str] = None
    position_side: Optional[str] = None
    exchange_order_id: Optional[str] = None
    client_order_id: Optional[str] = None
    trade_id: Optional[str] = None
    replaces: Optional[str] = None
    side: Optional[str] = None
    price: Optional[float] = None
    amount: Optional[float] = None
    filled: Optional[float] = None
    contracts_after: Optional[float] = None
    fee: Optional[float] = None
    fee_currency: Optional[str] = None
    confidence: Optional[str] = None
    note: Optional[str] = None
    kind: Optional[str] = None
    order_type: Optional[str] = None
    trigger_price: Optional[float] = None
    reduce_only: Optional[bool] = None
    key: Optional[str] = None
    raw: dict = field(default_factory=dict, repr=False)
    v: int = JOURNAL_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if not self.position_id:
            raise ConfigError('JournalEvent: position_id is required', details={'event': self.event})
        if self.event not in EVENTS:
            raise ConfigError(f'JournalEvent.event: one of {sorted(EVENTS)}, not {self.event!r}')
        if self.origin not in ORIGINS:
            raise ConfigError(f'JournalEvent.origin: one of {sorted(ORIGINS)}, not {self.origin!r}')
        if self.role is not None and self.role not in ROLES:
            raise ConfigError(f'JournalEvent.role: one of {sorted(ROLES)} or None, not {self.role!r}')
        if self.confidence is not None and self.confidence not in CONFIDENCE:
            raise ConfigError(f'JournalEvent.confidence: {sorted(CONFIDENCE)} or None, not {self.confidence!r}')
        if self.event == 'intervention' and self.kind not in INTERVENTION_KINDS:
            raise ConfigError(f'JournalEvent.kind for intervention: one of {sorted(INTERVENTION_KINDS)}, '
                              f'not {self.kind!r}')
        if self.key is None:
            self.key = self.default_key()

    def default_key(self) -> str:
        """Default idempotency key.

        ``bot_id | reference | event | discriminator``. Reference — the exchange order id, otherwise
        ``cid:<client_id>`` (``intent`` before the exchange response), otherwise ``pos:<position_id>``.
        Discriminator — ``trade_id``; without it: for ``partially_filled`` — the cumulative ``filled``
        (each new fill is a new event, a repeat of the same one is not); for ``replaced``,
        ``funding``, ``fee``, ``drift`` and position events — ``ts`` and the size after; for
        ``intervention`` — the kind (one intervention per order and kind; for ``position_drift`` — also
        ``ts`` and the size after).
        """
        if self.exchange_order_id:
            ref = f'oid:{self.exchange_order_id}'
        elif self.client_order_id:
            ref = f'cid:{self.client_order_id}'
        else:
            ref = f'pos:{self.position_id}'
        disc = self.trade_id or ''
        if not disc:
            if self.event == 'partially_filled':
                disc = f'filled={self.filled}'
            elif self.event == 'replaced':
                disc = f'ts={self.ts}:replaces={self.replaces}'
            elif self.event in POSITION_EVENTS or self.event in ('funding', 'fee', 'drift'):
                disc = f'ts={self.ts}:after={self.contracts_after}:amount={self.amount}'
                if self.event == 'drift' and self.kind:
                    disc = f'kind={self.kind}:{disc}'
            elif self.event == 'tp_sl_set':
                disc = f'role={self.role}:trigger={self.trigger_price}:ts={self.ts}'
            elif self.event == 'intervention':
                disc = f'kind={self.kind}'
                if self.kind == 'position_drift' or ref.startswith('pos:'):
                    disc += f':ts={self.ts}:after={self.contracts_after}'
        return f'{self.bot_id or ""}|{ref}|{self.event}|{disc}'

    # ─── serialization ──────────────────────────────────────────────────────

    def to_dict(self, *, with_raw: bool = True) -> dict:
        """Document for the store (Mongo ``position_journal``): ``_v`` instead of ``v``."""
        out: dict = {'_v': self.v}
        for f in fields(self):
            if f.name == 'v' or (f.name == 'raw' and not with_raw):
                continue
            out[f.name] = getattr(self, f.name)
        return out

    @classmethod
    def from_dict(cls, data: dict) -> 'JournalEvent':
        """From a store document; unknown keys (Mongo ``_id``, fields of future versions) are skipped."""
        known = {f.name for f in fields(cls)}
        kwargs = {k: v for k, v in data.items() if k in known and k != 'v'}
        kwargs['v'] = int(data.get('_v', data.get('v', JOURNAL_SCHEMA_VERSION)))
        return cls(**kwargs)

    def to_json(self, *, with_raw: bool = False) -> str:
        return json.dumps(self.to_dict(with_raw=with_raw), ensure_ascii=False, sort_keys=True, default=str)


JournalEntry = JournalEvent   # alias

_FINAL = {'filled': 'filled', 'canceled': 'canceled', 'rejected': 'rejected', 'expired': 'expired',
          'triggered': 'triggered'}
_PLATFORM_ORIGINS = frozenset({'own', 'platform', 'bot_legacy', 'funding', 'settlement'})


# Order outcome for the position history ("what fired, what was canceled, what is open")
OUTCOMES = {'filled': 'executed', 'triggered': 'executed', 'canceled': 'canceled', 'expired': 'canceled',
            'replaced': 'canceled', 'rejected': 'rejected', 'open': 'open', 'intent': 'open'}


@dataclass(slots=True)
class OrderSummary:
    """A position order in the summary: role, origin and last known status.

    ``status`` — detailed journal status (``intent``, ``open``, ``filled``, ``triggered``,
    ``canceled``, ``expired``, ``rejected``, ``replaced``); :attr:`outcome` — the outcome for history:
    ``executed`` (fired), ``canceled`` (canceled, expired or replaced), ``open`` (open),
    ``rejected`` (not accepted by the exchange). ``canceled_by`` — who canceled: ``own`` (cancel via the SDK),
    the intervention origin (``external_cancel``), None — unknown.
    """

    exchange_order_id: Optional[str]
    client_order_id: Optional[str]
    role: Optional[str]
    level_id: Optional[int]
    origin: str
    status: str
    filled: Optional[float] = None
    replaced_by: Optional[str] = None
    side: Optional[str] = None
    price: Optional[float] = None
    amount: Optional[float] = None
    order_type: Optional[str] = None
    trigger_price: Optional[float] = None
    placed_at: Optional[int] = None
    finished_at: Optional[int] = None
    canceled_by: Optional[str] = None

    @property
    def outcome(self) -> str:
        return OUTCOMES.get(self.status, 'open')


def fee_events(events: Iterable['JournalEvent']) -> list['JournalEvent']:
    """Events whose fee counts toward the total, without double counting.

    An order fee is written live in ``filled`` (``Order.fee``, without ``trade_id``), while
    history recovery writes trades (``trade_id``) with their own fees: for an order that
    has trades with a fee, only those count (like the size in :func:`own_size`);
    otherwise — fees of the order's events without ``trade_id``. Events without an order — as is.
    """
    events = list(events)
    traded: set[str] = set()
    for e in events:
        ref = _order_ref(e)
        if ref is not None and e.trade_id and e.fee is not None:
            traded.add(ref)
    out = []
    for e in events:
        if not e.fee:
            continue
        ref = _order_ref(e)
        if ref is not None and ref in traded and not e.trade_id:
            continue
        out.append(e)
    return out


def _order_ref(e: 'JournalEvent') -> Optional[str]:
    if e.exchange_order_id:
        return f'oid:{e.exchange_order_id}'
    if e.client_order_id:
        return f'cid:{e.client_order_id}'
    return None


@dataclass(slots=True)
class PositionRecord:
    """Position summary from journal events.

    ``orders`` — orders with roles and final status; ``fees`` / ``funding`` — event sums;
    ``contracts`` — size after the last position event; ``interventions`` — events of
    foreign origin (manual, foreign, unknown; liquidation, ADL) and
    ``intervention`` events; ``incomplete`` — the journal is incomplete (an old ``bot_legacy`` position or
    incomplete exchange history — a ``drift`` of kind ``history_incomplete``).
    """

    position_id: str
    bot_id: Optional[str] = None
    symbol: Optional[str] = None
    position_side: Optional[str] = None
    opened_at: Optional[int] = None
    closed_at: Optional[int] = None
    contracts: Optional[float] = None
    fees: float = 0.0
    funding: float = 0.0
    orders: list[OrderSummary] = field(default_factory=list)
    interventions: list[JournalEvent] = field(default_factory=list)
    incomplete: bool = False
    events: int = 0

    @property
    def is_closed(self) -> bool:
        return self.closed_at is not None

    @property
    def intervention_events(self) -> list[JournalEvent]:
        """Only ``intervention`` events (one per order and kind)."""
        return [e for e in self.interventions if e.event == 'intervention']

    def orders_by_outcome(self) -> dict[str, list[OrderSummary]]:
        """Position orders by outcome: ``executed`` / ``canceled`` / ``open`` / ``rejected``."""
        out: dict[str, list[OrderSummary]] = {'executed': [], 'canceled': [], 'open': [], 'rejected': []}
        for o in self.orders:
            out.setdefault(o.outcome, []).append(o)
        return out

    @classmethod
    def from_events(cls, events: list[JournalEvent]) -> 'PositionRecord':
        if not events:
            raise ConfigError('PositionRecord: no events')
        events = sorted(events, key=lambda e: (e.seq if e.seq is not None else 0))
        first = events[0]
        rec = cls(position_id=first.position_id, bot_id=first.bot_id, events=len(events))
        by_ref: dict[str, OrderSummary] = {}
        cid_ref: dict[str, str] = {}
        canceled_by: dict[str, str] = {}
        rec.fees = sum(e.fee for e in fee_events(events) if e.fee)
        for e in events:
            rec.symbol = rec.symbol or e.symbol
            rec.position_side = rec.position_side or e.position_side
            if e.origin == 'bot_legacy' or (e.event == 'drift' and e.kind == 'history_incomplete'):
                rec.incomplete = True
            if e.event == 'funding' and e.amount is not None:
                rec.funding += e.amount
            if e.event in POSITION_EVENTS:
                if e.event == 'position_opened' and rec.opened_at is None:
                    rec.opened_at = e.ts
                if e.contracts_after is not None:
                    rec.contracts = e.contracts_after
                if e.event in ('closed', 'liquidated', 'adl') and not e.contracts_after:
                    rec.closed_at = e.ts
                elif e.event in ('position_opened', 'increased'):
                    rec.closed_at = None
            if e.event == 'drift' and e.kind == 'history_incomplete':
                continue
            if e.event in ('liquidated', 'adl', 'intervention') or e.origin not in _PLATFORM_ORIGINS:
                rec.interventions.append(e)
            if e.event == 'intervention' and e.kind == 'external_cancel':
                for ref in (e.exchange_order_id, e.client_order_id and f'cid:{e.client_order_id}'):
                    if ref:
                        canceled_by[ref] = e.origin
            if e.event not in ORDER_EVENTS:
                continue
            ref = e.exchange_order_id or (cid_ref.get(e.client_order_id) if e.client_order_id else None) \
                or (f'cid:{e.client_order_id}' if e.client_order_id else None)
            if ref is None:
                continue
            o = by_ref.get(ref)
            if o is None and e.exchange_order_id and e.client_order_id:
                # intent (by cid) → placed (by exchange id): the same record
                pending = by_ref.pop(f'cid:{e.client_order_id}', None)
                if pending is not None:
                    pending.exchange_order_id = e.exchange_order_id
                    o = by_ref[ref] = pending
            if o is None:
                o = by_ref[ref] = OrderSummary(e.exchange_order_id, e.client_order_id, e.role, e.level_id,
                                               e.origin, 'intent' if e.event == 'intent' else 'open')
                rec.orders.append(o)
            if e.client_order_id:
                o.client_order_id = o.client_order_id or e.client_order_id
                if e.exchange_order_id:
                    cid_ref[e.client_order_id] = e.exchange_order_id
            o.role = o.role or e.role
            o.level_id = o.level_id if o.level_id is not None else e.level_id
            o.side = o.side or e.side
            o.order_type = e.order_type or o.order_type
            if e.trigger_price is not None:
                o.trigger_price = e.trigger_price
            if e.event in ('intent', 'placed', 'replaced'):
                if e.price is not None:
                    o.price = e.price
                if e.amount is not None:
                    o.amount = e.amount
                if e.event != 'intent' and o.placed_at is None:
                    o.placed_at = e.ts
            elif o.amount is None and e.amount is not None:
                o.amount = e.amount
            if e.event in _FINAL:
                o.status = _FINAL[e.event]
                o.finished_at = e.ts
                if e.event == 'canceled' and e.note == 'own_cancel':
                    o.canceled_by = 'own'
            elif e.event in ('placed', 'partially_filled') and o.status in ('intent', 'open'):
                o.status = 'open'
            if e.filled is not None:
                o.filled = e.filled
            if e.event == 'replaced' and e.replaces:
                old = by_ref.get(e.replaces)
                if old is not None and old is not o:
                    old.replaced_by = e.exchange_order_id
                    if old.status in ('open', 'intent'):
                        old.status = 'replaced'
                        old.finished_at = e.ts
        for o in rec.orders:
            who = canceled_by.get(o.exchange_order_id or '') or canceled_by.get(f'cid:{o.client_order_id}')
            if who is not None and o.canceled_by is None:
                o.canceled_by = who
        return rec


__all__ = [
    'CONFIDENCE', 'DRIFT_KINDS', 'EVENTS', 'INTERVENTION_KINDS', 'JOURNAL_SCHEMA_VERSION', 'JournalEntry',
    'JournalEvent', 'ORDER_EVENTS', 'ORIGINS', 'OUTCOMES', 'OrderSummary', 'POSITION_EVENTS', 'PositionRecord',
    'ROLES', 'fee_events',
]
