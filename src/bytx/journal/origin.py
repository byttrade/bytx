"""Origin of an order and a fill: deterministic, from exchange API markers.

Decision order (the first matching rule wins):

1. ``known`` — the order is already in the journal (our ``intent`` / ``placed``): its origin, ``exact``;
2. an exchange marker (:mod:`bytx.exchanges.<exchange>.journal_origin`, per the official docs) —
      liquidation, ADL, settlement/delisting, exchange UI, exchange bots and strategies, funding;
      a "child order" of a conditional one (TP/SL triggered) — the parent's origin (its client_id from
      the exchange response or ``resolve_parent(id)``), otherwise ``unknown``;
3. client_id (the Binance broker prefix ``x-<ID>`` is stripped):

      - a next-generation bot tag or an HL cloid with a tag — ``own`` (role and level from the cid);
      - the old bot format (tag + hex) — ``bot_legacy``; old-format algo TP/SL without a tag
          ``t|s{level}{hex}`` — ``bot_legacy`` with ``confidence='heuristic'`` (whose bot is unknown);
      - a platform format (``platform_formats``) — ``platform`` (for a Hyperliquid cloid — ``heuristic``:
          a tag inside a 128-bit hex number is indistinguishable from random hex);
      - another cid: if the exchange generates a cid itself when none is passed (Binance) — ``unknown``
          (manual or software without a cid — indistinguishable); otherwise ``foreign_api`` (``heuristic``: the docs
          have no exchange UI marker, and the UI could set its own id);
4. no cid: ``manual`` with ``confidence='heuristic'`` (manual or software without a client_id); on an exchange
      that generates the cid itself (Binance) — ``unknown``.

For a fill (``kind='fill'``) — the exchange's fill markers (liquidation, ADL, funding),
then ``known`` (the order's origin) and the fill's cid if the exchange returns it; without them — ``unknown``
(a fill's origin = its order's origin, which the caller looks up).
"""
from __future__ import annotations

import importlib
import re
from dataclasses import dataclass
from types import ModuleType
from typing import Any, Callable, Iterable, Optional, Pattern, Union

from bytx.exchanges.hyperliquid.cloid import cloid_body
from bytx.journal.client_id import BOT_TAG_RE, parse_client_id

# SDK name / ccxt id → exchanges/<exchange> package
_PACKAGES = {'binance': 'binance', 'binanceusdm': 'binance', 'bybit': 'bybit', 'okx': 'okx', 'bingx': 'bingx',
             'kucoin': 'kucoin', 'kucoinfutures': 'kucoin', 'kucoin futures': 'kucoin',
             'hyperliquid': 'hyperliquid', 'bitget': 'bitget', 'gate': 'gate', 'gate.io': 'gate',
             'mexc': 'mexc', 'mexc global': 'mexc', 'htx': 'htx'}
_OWN_ISH = frozenset({'own', 'bot_legacy'})


@dataclass(frozen=True, slots=True)
class Signal:
    """Origin marker from exchange response fields (a rule of the exchange's ``journal_origin`` module).

    ``origin`` — a :data:`~bytx.journal.models.ORIGINS` class; None — the order itself has no class:
    it is a child order of a conditional one (``child=True``, parent — ``parent_id`` /
    ``parent_client_id``) or a cancel "by the user" (``cancel_signal``: us or a human).
    ``event`` — the position event for a fill (``liquidated`` / ``adl`` / ``funding``).
    ``expected`` — for a cancel: a regular exchange cancel (OCO sibling, reduce-only, position closed,
    our dead man's switch) — not an intervention.
    """

    origin: Optional[str]
    reason: str
    confidence: Optional[str] = 'exact'
    child: bool = False
    parent_id: Optional[str] = None
    parent_client_id: Optional[str] = None
    event: Optional[str] = None
    expected: bool = False


@dataclass(frozen=True, slots=True)
class OriginVerdict:
    """Classification result: ``origin``, confidence, reason (for reports and rule audits)."""

    origin: str
    confidence: Optional[str]
    reason: str
    role: Optional[str] = None
    level_id: Optional[int] = None
    generation: Optional[str] = None
    parent: Optional[str] = None
    event: Optional[str] = None

    @property
    def is_ours(self) -> bool:
        """Our bot (new or old cid generation)."""
        return self.origin in _OWN_ISH


def rules_for(exchange: Any) -> ModuleType:
    """Exchange rules module ``bytx.exchanges.<exchange>.journal_origin``.

    ``exchange`` — SDK name, ccxt id, :class:`~bytx.Exchange` or a ccxt instance.
    """
    name = _exchange_name(exchange)
    pkg = _PACKAGES.get(name)
    if pkg is None:
        from bytx.errors import ConfigError
        raise ConfigError(f'no origin rules defined for {name!r}', details={'exchange': name})
    return importlib.import_module(f'bytx.exchanges.{pkg}.journal_origin')


def _exchange_name(exchange: Any) -> str:
    if isinstance(exchange, str):
        return exchange.lower()
    for attr in ('name', 'id'):
        value = getattr(exchange, attr, None)
        if isinstance(value, str) and value.lower() in _PACKAGES:
            return value.lower()
    return str(getattr(exchange, 'id', exchange)).lower()


def _strip(cid: str, prefixes: Iterable[str]) -> str:
    low = cid.lower()
    for p in prefixes:
        if p and len(cid) > len(p) and low.startswith(p.lower()):
            return cid[len(p):]
    return cid


def classify_origin(exchange: Any, *, client_id: Optional[str] = None, info: Optional[dict] = None,
                    kind: str = 'order', tag: Optional[str] = None,
                    platform_formats: Iterable[Union[str, Pattern[str]]] = (),
                    broker_prefixes: Iterable[str] = (), known: Optional[str] = None,
                    resolve_parent: Optional[Callable[[str], Optional[str]]] = None) -> OriginVerdict:
    """Origin of an order (``kind='order'``) or a fill (``kind='fill'``), see the module.

    ``info`` — the raw exchange response (``Order.raw['info']`` / ``Fill.raw['info']``, a history row,
    a WS message); ``client_id`` — logical or physical cid (if not given, taken from
    ``info`` by the exchange keys); ``tag`` — the bot tag (default ``d`` + 12 hex);
    ``known`` — origin of an order already in the journal; ``resolve_parent(id)`` — origin
    of the parent conditional order by its id (journal), if the exchange returns only the parent id.
    """
    rules = rules_for(exchange)
    info = info if isinstance(info, dict) else {}
    if known:
        return OriginVerdict(known, 'exact', 'order is recorded in the journal')
    sig = rules.fill_signal(info) if kind == 'fill' else rules.order_signal(info)
    if sig is not None and sig.origin is not None and not sig.child:
        return OriginVerdict(sig.origin, sig.confidence, sig.reason, event=sig.event)
    if client_id is None:
        client_id = rules.client_id_of(info)
    prefixes = tuple(broker_prefixes)
    formats = tuple(re.compile(p) if isinstance(p, str) else p for p in platform_formats)
    if sig is not None and sig.child:
        parent = sig.parent_id or sig.parent_client_id
        if sig.parent_client_id:
            v = _by_client_id(rules, sig.parent_client_id, tag, formats, prefixes)
            if v is not None and v.origin != 'unknown':
                return OriginVerdict(v.origin, v.confidence, f'{sig.reason}; parent: {v.reason}', v.role,
                                     v.level_id, v.generation, parent)
        if sig.parent_id and resolve_parent is not None:
            got = resolve_parent(str(sig.parent_id))
            if got:
                return OriginVerdict(got, 'exact', f'{sig.reason}; parent {sig.parent_id} is in the journal',
                                     parent=parent)
        if not client_id:
            return OriginVerdict('unknown', None, f'{sig.reason}; parent not identified', parent=parent)
    if client_id:
        v = _by_client_id(rules, str(client_id), tag, formats, prefixes)
        if v is not None:
            return v
    if kind == 'fill':
        return OriginVerdict('unknown', None, 'fill has no marker of its own: origin follows the order')
    if sig is not None and sig.origin is None and sig.reason:
        hint = sig.reason
    else:
        hint = None
    if getattr(rules, 'AUTO_CLIENT_ID', False):
        return OriginVerdict('unknown', None, f'{rules.EXCHANGE}: the exchange generates client_id itself — '
                                              'no cid in the data, source cannot be determined')
    ui = rules.ui_signal(info) if hasattr(rules, 'ui_signal') else None
    if ui is not None:
        return OriginVerdict(ui.origin or 'exchange_ui', ui.confidence, ui.reason)
    reason = 'no client_id: manual or software without client_id'
    if getattr(rules, 'CLIENT_ID_REQUIRED', False):
        reason = (f'no client_id, while {rules.EXCHANGE} requires it in the API ({rules.CLIENT_ID_FIELD}): '
                  'most likely manual (exchange UI)')
    return OriginVerdict('manual', 'heuristic', reason if hint is None else f'{hint}; {reason}')


def _by_client_id(rules: ModuleType, cid: str, tag: Optional[str], formats: tuple, prefixes: tuple
                  ) -> Optional[OriginVerdict]:
    logical = _strip(cid, prefixes)
    want = tag.lower().removeprefix('0x') if tag else None
    info = parse_client_id(logical, tag=want if want and not BOT_TAG_RE.fullmatch(want) else None)
    if want and info.bot_tag == want:
        if info.generation in ('v2', 'hyperliquid', 'tagged'):
            return OriginVerdict('own', 'exact', f'bot tag in client_id ({info.generation})', info.role,
                                 info.level_id, info.generation)
        return OriginVerdict('bot_legacy', 'exact', 'old bot client_id format (tag + hex)', None, None,
                             info.generation)
    if any(p.match(logical) for p in formats):
        if cloid_body(logical) is not None:
            # HL cloid is a 128-bit hex number: a tag-based format inside is indistinguishable from random hex
            return OriginVerdict('platform', 'heuristic', 'platform client_id format (Hyperliquid cloid: '
                                                          'tag inside a hex number, the match is ambiguous)',
                                 generation=info.generation)
        return OriginVerdict('platform', 'exact', 'platform client_id format', generation=info.generation)
    if info.generation == 'legacy_algo':
        return OriginVerdict('bot_legacy', 'heuristic', 'old-format algo TP/SL without a bot tag',
                             info.role, info.level_id, info.generation)
    special = rules.special_client_id(logical) if hasattr(rules, 'special_client_id') else None
    if special is not None:
        return OriginVerdict(special.origin or 'unknown', special.confidence, special.reason, event=special.event)
    if getattr(rules, 'AUTO_CLIENT_ID', False):
        return OriginVerdict('unknown', None, f'{rules.EXCHANGE}: client_id is not ours, and the exchange generates it itself '
                                              'when not passed — manual or third-party software, indistinguishable')
    return OriginVerdict('foreign_api', 'heuristic', 'client_id is not ours (the docs have no exchange UI marker)',
                         generation=info.generation)


def position_side_of_fill(exchange: Any, info: Optional[dict]) -> Optional[str]:
    """Fill position side in hedge mode (``long`` / ``short``); None — one-way or not specified."""
    return rules_for(exchange).fill_position_side(info if isinstance(info, dict) else {})


__all__ = ['OriginVerdict', 'Signal', 'classify_origin', 'position_side_of_fill', 'rules_for']
