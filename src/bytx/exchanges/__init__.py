"""SDK exchange modules: one package per exchange (``bytx.exchanges.<exchange>``).

An exchange package holds everything specific to it: ccxt fixes (``ccxt_fixes``),
broker attribution, instance patches, markets, position history.

Fixes for ccxt 4.4.65 bugs on the ccxt instance, based on official exchange docs.

ccxt is not the source of truth: a mismatch with the docs is pinned by a test in
``tests/ground_truth``, and the fix lives in ``exchanges/<exchange>/ccxt_fixes.py`` — patched
on the instance only (ccxt classes are left alone, other instances are unaffected).
Each such module exports ``apply(exchange)``; repeated calls are a no-op.
"""
from __future__ import annotations

import importlib
from typing import Any, Optional

_MODULES = {
    'binance': 'binance',
    'bybit': 'bybit',
    'okx': 'okx',
    'kucoinfutures': 'kucoin',
    'kucoin': 'kucoin',
    'bingx': 'bingx',
    'hyperliquid': 'hyperliquid',
    'bitget': 'bitget',
    'gate': 'gate',
    'mexc': 'mexc',
    'htx': 'htx',
}
_APPLIED_ATTR = '_bytx_ccxt_fixes'


def apply_ccxt_fixes(exchange: Any) -> None:
    """Applies fixes for the instance's exchange (idempotent)."""
    module = _MODULES.get(getattr(exchange, 'id', ''))
    if module is None or getattr(exchange, _APPLIED_ATTR, False):
        return
    importlib.import_module(f'{__name__}.{module}.ccxt_fixes').apply(exchange)
    setattr(exchange, _APPLIED_ATTR, True)


def tp_sl_trigger_by(ccxt_id: str) -> str:
    """Default TP/SL ``trigger_by`` for the exchange:
    Binance and OKX — 'last', others — 'mark'. See ``exchanges/<exchange>/tp_sl.py``."""
    module = _MODULES.get(ccxt_id)
    if module is None:
        return 'mark'
    return str(importlib.import_module(f'{__name__}.{module}.tp_sl').DEFAULT_TRIGGER_BY)


def tp_sl_oco(ccxt_id: str) -> tuple[str, Optional[str], bool, str]:
    """How the exchange links TP and SL (bracket), per docs, ``exchanges/<exchange>/tp_sl.py`` ``OCO``:
    (mode, docs URL, verified, note). Mode 'native_pair' — TP and SL in one order (only one leg
    can fire), 'emulated' — legs are independent, the SDK cancels the sibling. Unknown exchange — 'emulated'."""
    module = _MODULES.get(ccxt_id)
    if module is None:
        return 'emulated', None, False, 'no data'
    mode, url, verified, note = importlib.import_module(f'{__name__}.{module}.tp_sl').OCO
    return str(mode), url, bool(verified), str(note)


def attached_sl(ccxt_id: str) -> tuple[bool, Optional[str], bool, str]:
    """Whether SL can be attached to the entry order (``open_position``), per the Place order docs,
    ``exchanges/<exchange>/tp_sl.py`` ``ATTACHED_SL``: (supported, docs URL, verified, note). True — entry and
    SL in one request (Bybit, OKX, BingX, KuCoin); False — SL as a separate order right after entry
    (Binance, Hyperliquid). Unknown exchange — False."""
    module = _MODULES.get(ccxt_id)
    if module is None:
        return False, None, False, 'no data'
    ok, url, verified, note = importlib.import_module(f'{__name__}.{module}.tp_sl').ATTACHED_SL
    return bool(ok), url, bool(verified), str(note)


async def price_band(ex: Any, symbol: str) -> Optional[tuple[Optional[float], Optional[float]]]:
    """The exchange's order price band now — (highest buy price, lowest sell price) per the docs
    (``exchanges/<exchange>/limits.py`` ``price_band``): the exchange rejects an order beyond the band entirely.
    OKX has it (GET /api/v5/public/price-limit, error 51006); others — None (no band in the SDK)."""
    module = _MODULES.get(getattr(getattr(ex, 'ccxt', None), 'id', '') or '')
    if module is None:
        return None
    fn = getattr(importlib.import_module(f'{__name__}.{module}.limits'), 'price_band', None)
    return await fn(ex, symbol) if fn is not None else None


def tp_sl_in_place(ccxt_id: str) -> bool:
    """The exchange replaces a full-position SL in place (a new placement replaces the previous one): Bybit trading-stop
    Full. Moving the stop then goes through :meth:`Exchange.modify_tp_sl`, not place + cancel previous."""
    module = _MODULES.get(ccxt_id)
    if module is None:
        return False
    return bool(getattr(importlib.import_module(f'{__name__}.{module}.tp_sl'), 'SL_IN_PLACE', False))


def tp_sl_modify(ccxt_id: str) -> tuple[Optional[tuple], tuple[bool, Optional[str], bool, str]]:
    """How to move a TP/SL without a window without a stop (:meth:`Exchange.modify_tp_sl`) — per the docs,
    ``exchanges/<exchange>/tp_sl.py``: (``MODIFY_IN_PLACE``, ``TWO_SL``).

    ``MODIFY_IN_PLACE`` — (docs URL, verified, note) of an in-place trigger amend (OKX amend-algos, Bybit
    trading-stop Full, Bitget modify-tpsl-order) or None. ``TWO_SL`` — (allowed, URL, verified, note): two
    TP/SL with a size on one position coexist → make-before-break (the new one, then removing the previous one);
    False — cancel, then the new one (a window without a stop — the request time). Unknown exchange — (None, (False, …)).
    """
    module = _MODULES.get(ccxt_id)
    if module is None:
        return None, (False, None, False, 'no data')
    mod = importlib.import_module(f'{__name__}.{module}.tp_sl')
    in_place = getattr(mod, 'MODIFY_IN_PLACE', None)
    ok, url, verified, note = getattr(mod, 'TWO_SL', (False, None, False, 'no data'))
    return (tuple(in_place) if in_place else None), (bool(ok), url, bool(verified), str(note))


# SDK order status → normalized TP/SL state when the exchange has no such field
_GENERIC_STATE = {'open': 'open', 'closed': 'triggered', 'canceled': 'canceled',
                  'expired': 'canceled', 'rejected': 'canceled'}


def tp_sl_state(ccxt_id: str, info: Any, status: Optional[str] = None) -> Optional[str]:
    """TP/SL state: 'open' — waiting for trigger, 'triggered' — fired,
    'canceled' — removed without firing (canceled, expired, rejected). From the exchange field in
    ``exchanges/<exchange>/tp_sl.py``, otherwise from the order ``status``; unknown — None."""
    module = _MODULES.get(ccxt_id)
    if module is not None and isinstance(info, dict):
        found = importlib.import_module(f'{__name__}.{module}.tp_sl').state(info)
        if found is not None:
            return str(found)
    return _GENERIC_STATE.get(str(status or ''))


def _id_or_none(value: Any) -> Optional[str]:
    text = str(value).strip() if value is not None else ''
    return None if text in ('', '0', '-1', 'None') else text


def tp_sl_fills_by_other_order(ccxt_id: str) -> bool:
    """The exchange fills a triggered TP/SL with another order and writes its id into the TP/SL record (``FILL_ORDER_ID``)."""
    module = _MODULES.get(ccxt_id)
    return module is not None and bool(
        getattr(importlib.import_module(f'{__name__}.{module}.tp_sl'), 'FILL_ORDER_ID', ()))


def tp_sl_fill_order_ids(ccxt_id: str, info: Any) -> tuple[str, ...]:
    """Ids of the orders that filled a triggered TP/SL, if the exchange fills it with a DIFFERENT order and reports
    them in the TP/SL record (``exchanges/<exchange>/tp_sl.py`` ``FILL_ORDER_ID``): Binance actualOrderId, OKX ordIdList
    (Get algo order details: «There will be multiple order IDs when there is TP/SL splitting order» — there may be
    several), Gate trade_id, MEXC placeOrderId, HTX relation_order_id. Empty — not triggered, the exchange fills under
    the same id, or the TP/SL record has no link. The first field with a non-empty value; order — as the exchange's."""
    module = _MODULES.get(ccxt_id)
    if module is None or not isinstance(info, dict):
        return ()
    fields = getattr(importlib.import_module(f'{__name__}.{module}.tp_sl'), 'FILL_ORDER_ID', ())
    for name in fields:
        value = info.get(name)
        found = [_id_or_none(v) for v in (value if isinstance(value, list) else [value])]
        ids = tuple(dict.fromkeys(i for i in found if i is not None))
        if ids:
            return ids
    return ()


def tp_sl_fill_order_id(ccxt_id: str, info: Any) -> Optional[str]:
    """The first of :func:`tp_sl_fill_order_ids` (the fill order of a triggered TP/SL) or None."""
    ids = tp_sl_fill_order_ids(ccxt_id, info)
    return ids[0] if ids else None


__all__ = ['apply_ccxt_fixes', 'tp_sl_trigger_by', 'tp_sl_state', 'tp_sl_fill_order_id', 'tp_sl_fill_order_ids',
           'tp_sl_fills_by_other_order']


def _matching(ccxt_id: str, name: str) -> tuple[str, Optional[str], bool, str]:
    module = _MODULES.get(ccxt_id)
    if module is None:
        return 'reject', None, False, 'no data'
    mode, url, verified, note = getattr(importlib.import_module(f'{__name__}.{module}.matching'), name)
    return str(mode), url, bool(verified), str(note)


def post_only_cross(ccxt_id: str) -> tuple[str, Optional[str], bool, str]:
    """What happens when post-only would cross the book, per exchange docs (``exchanges/<exchange>/matching.py``):
    (mode, URL, verified, note); 'reject' — the request is rejected (Binance -5022), 'cancel' — the order
    is created and then canceled (Bybit, OKX, Hyperliquid). Unknown exchange — 'reject'. Paper follows this."""
    return _matching(ccxt_id, 'POST_ONLY_CROSS')


def market_no_liquidity(ccxt_id: str) -> tuple[str, Optional[str], bool, str]:
    """Market order with no opposite liquidity, per exchange docs: 'cancel' (Bybit) or 'reject'."""
    return _matching(ccxt_id, 'MARKET_NO_LIQUIDITY')


def history_spec(ccxt_id: str) -> Any:
    """Exchange market-history limits per docs (:mod:`bytx.data`): module
    ``exchanges/<exchange>/history.py`` (CANDLES, FUNDING, TRADES, candle_params, funding_params,
    fetch_trades). Unknown exchange — None (the loader does not guess limits)."""
    module = _MODULES.get(ccxt_id)
    if module is None:
        return None
    return importlib.import_module(f'{__name__}.{module}.history')
