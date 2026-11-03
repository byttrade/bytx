"""ccxt 4.4.65 fixes for okx (see tests/ground_truth).

Docs: https://www.okx.com/docs-v5/en/ — section anchors are given for each patch.
Patches on the instance only; ``okx_patches.patch_okx_instance`` (safe parse_market)
is installed earlier — here we wrap the instance methods already in place.
"""
from __future__ import annotations

from typing import Any

from ccxt.async_support.base.ws.cache import ArrayCacheBySymbolById
from ccxt.base.errors import InvalidNonce
from loguru import logger

# Instruments → state: trading impossible (suspend/rebase/preopen/test/settling).
# live and post_only (post-only orders accepted) — the market is active.
_INACTIVE_STATES = frozenset({'suspend', 'rebase', 'preopen', 'test', 'settling'})

# Algo order types: for /trade/order-algo TP/SL are top-level fields, left alone.
_ALGO_ORD_TYPES = frozenset({'trigger', 'conditional', 'oco', 'move_order_stop', 'iceberg', 'twap'})

# TP/SL fields that in /trade/order (and batch-orders) live inside attachAlgoOrds[].
# attachAlgoClOrdId — own id of the attached algo: «It will be posted to algoClOrdId when placing the
# attached algo order» (Place order → attachAlgoOrds; the journal recognizes a triggered TP/SL by it).
_ATTACH_KEYS = ('tpTriggerPx', 'tpTriggerPxType', 'tpOrdPx', 'tpOrdKind',
                'slTriggerPx', 'slTriggerPxType', 'slOrdPx', 'attachAlgoClOrdId')

FILLS_HASH = 'fills::myTrades'


def _patch_parse_market(ex: Any) -> None:
    """GET /api/v5/public/instruments → state: suspend/preopen/… ⇒ active=False.

    ccxt 4.4.65 always sets active=True.
    """
    original = ex.parse_market

    def parse_market(market):
        result = original(market)
        if result is not None and isinstance(market, dict) \
                and ex.safe_string(market, 'state') in _INACTIVE_STATES:
            result['active'] = False
        return result

    ex.parse_market = parse_market


def _patch_parse_funding_rate(ex: Any) -> None:
    """GET /api/v5/public/funding-rate: timestamp = ts («Data return time»).

    The interval is nextFundingTime minus fundingTime (per the fundingRate description);
    ccxt 4.4.65 wrongly subtracts from nextFundingRate.
    """
    original = ex.parse_funding_rate

    def parse_funding_rate(contract, market=None):
        result = original(contract, market)
        ts = ex.safe_integer(contract, 'ts')
        if ts is not None and result.get('timestamp') is None:
            result['timestamp'] = ts
            result['datetime'] = ex.iso8601(ts)
        funding_time = ex.safe_string(contract, 'fundingTime')
        next_time = ex.safe_string(contract, 'nextFundingTime')
        if funding_time and next_time:
            result['interval'] = ex.parse_funding_interval(str(int(next_time) - int(funding_time)))
        return result

    ex.parse_funding_rate = parse_funding_rate


def _patch_parse_trade(ex: Any) -> None:
    """Get transaction details (fills / fills-history): trade time is fillTime.

    ts there is the record generation time («sort by fillTime, not ts»). The WS fills channel
    has no fillTime, ts is the trade time: the ccxt value stays.
    """
    original = ex.parse_trade

    def parse_trade(trade, market=None):
        result = original(trade, market)
        fill_time = ex.safe_integer(trade, 'fillTime') if isinstance(trade, dict) else None
        if fill_time is not None:
            result['timestamp'] = fill_time
            result['datetime'] = ex.iso8601(fill_time)
        return result

    ex.parse_trade = parse_trade


def _patch_create_order_request(ex: Any) -> None:
    """POST /trade/order and /trade/batch-orders: TP/SL only inside attachAlgoOrds[].

    ccxt 4.4.65 puts tpTriggerPx/slTriggerPx… at the top level; the request spec
    has no such fields (section Place order → attachAlgoOrds). clOrdId/tag (broker
    tag) stay at the top level. Used by both create_order and create_orders.
    """
    original = ex.create_order_request

    def create_order_request(symbol, type, side, amount, price=None, params={}):
        request = original(symbol, type, side, amount, price, params)
        if not isinstance(request, dict) or 'attachAlgoOrds' in request:
            return request
        if request.get('ordType') in _ALGO_ORD_TYPES or type in _ALGO_ORD_TYPES:
            return request
        if request.get('tpTriggerPx') is None and request.get('slTriggerPx') is None:
            return request
        algo = {k: request.pop(k) for k in _ATTACH_KEYS if k in request}
        request['attachAlgoOrds'] = [algo]
        return request

    ex.create_order_request = create_order_request


def _patch_order_book_checksum(ex: Any) -> None:
    """WS order book: integrity via seqId/prevSeqId, not checksum.

    OKX docs (Order book channel): «checksum (Deprecated). The field remains in the push for
    books, books-l2-tbt and books50-l2-tbt, but its value is fixed to 0 and must no longer be
    used for integrity verification. Please use seqId/prevSeqId». ccxt 4.4.65 checks crc32
    against this zero → every snapshot is rejected (the live OKX book didn't work at all), and
    the ccxt sequence check lives inside the same checksum branch.
    So the ccxt checksum is disabled and we check sequence gaps ourselves:
    an update's prevSeqId must equal the previous message's seqId (a sequence
    reset, seqId < prevSeqId, is allowed — docs, «sequence reset»).
    Gap → reject(InvalidNonce) and book reset: the subscriber rebuilds it from a snapshot.
    """
    if not hasattr(ex, 'handle_order_book_message'):
        return
    ex.options.setdefault('watchOrderBook', {})['checksum'] = False
    original = ex.handle_order_book_message

    def handle_order_book_message(client, message, orderbook, messageHash, market=None):
        prev = ex.safe_integer(message, 'prevSeqId')
        last = orderbook['nonce'] if hasattr(orderbook, '__getitem__') else None
        if prev is not None and prev != -1 and last is not None and prev != last:
            symbol = orderbook.get('symbol') if hasattr(orderbook, 'get') else None
            subs = getattr(client, 'subscriptions', None)
            if isinstance(subs, dict):
                subs.pop(messageHash, None)
            if symbol is not None:
                ex.orderbooks.pop(symbol, None)
            client.reject(InvalidNonce(f'{ex.id} {symbol}: order book sequence gap '
                                       f'(prevSeqId {prev} ≠ {last}) — book reset, resubscribe required'),
                          messageHash)
            return orderbook
        return original(client, message, orderbook, messageHash, market)

    ex.handle_order_book_message = handle_order_book_message


def _patch_ws_fills(ex: Any) -> None:
    """WS fills channel (Order book trading → Trade → WS / Fills channel, VIP6+).

    ccxt 4.4.65 doesn't handle it. Trades go into a separate cache
    (``_bytx_fills``, not mixed with myTrades from the orders channel) and are resolved
    on ``fills::myTrades`` and ``fills::myTrades::<symbol>``.
    """
    if not hasattr(ex, 'handle_message'):
        return
    original = ex.handle_message

    def handle_fills(client, message):
        data = ex.safe_list(message, 'data', [])
        if not data:
            return
        cache = getattr(ex, '_bytx_fills', None)
        if cache is None:
            cache = ArrayCacheBySymbolById(ex.safe_integer(ex.options, 'tradesLimit', 1000))
            ex._bytx_fills = cache
        symbols = {}
        for item in data:
            trade = ex.parse_trade(item)
            cache.append(trade)
            symbols[trade['symbol']] = True
        client.resolve(cache, FILLS_HASH)
        for symbol in symbols:
            client.resolve(cache, f'{FILLS_HASH}::{symbol}')

    def handle_message(client, message):
        if isinstance(message, dict) and ex.safe_string_2(message, 'event', 'op') is None:
            arg = ex.safe_dict(message, 'arg', {})
            if ex.safe_string(arg, 'channel') == 'fills':
                if ex.handle_error_message(client, message):
                    handle_fills(client, message)
                return
        return original(client, message)

    ex.handle_message = handle_message


def _patch_ws_notice(ex: Any) -> None:
    """WS: ``{"event":"notice","code":"64010",…}`` is a notice, not an error (WebSocket → Notification).

    ccxt 4.4.65 treats any message with code != '0' as an error and calls ``client.reject(e)`` without an id:
    all of the connection's waiters are rejected, including login. On demo after a reconnect OKX sends
    notice 64010 («WebSocket port 8443 will be retired soon») — the private orders stream didn't
    recover (demo 2026-10-27: a cancel never reached the tracker). The notice is logged
    once per code and goes no further; a connection close on notice is handled by the supervisor.
    """
    if not hasattr(ex, 'handle_message'):
        return
    original = ex.handle_message
    seen: set = set()

    def handle_message(client, message):
        if isinstance(message, dict) and message.get('event') == 'notice':
            code = str(message.get('code') or '')
            if code not in seen:
                seen.add(code)
                logger.warning(f'okx WS notice {code}: {message.get("msg")}')
            return None
        return original(client, message)

    ex.handle_message = handle_message


def _patch_ws_order_trade_fee(ex: Any) -> None:
    """WS orders channel → trade: fee sign as in REST (Order channel → fillFee).

    Docs: «fillFee — last filled fee amount or rebate amount… Negative number represents the user
    transaction fee charged by the platform. Positive number represents rebate». ccxt REST parsing
    (parse_trade) flips the sign, ``order_to_trade`` (WS) doesn't: the WS fill fee came out
    negative (demo 2026-10-29: fees −0.099 instead of +0.099 — profit was overstated).
    """
    if not hasattr(ex, 'order_to_trade'):
        return
    original = ex.order_to_trade

    def order_to_trade(order, market=None):
        trade = original(order, market)
        fee = trade.get('fee') if isinstance(trade, dict) else None
        if isinstance(fee, dict) and fee.get('cost') is not None:
            fee['cost'] = -fee['cost']
            fees = trade.get('fees')
            if isinstance(fees, list) and len(fees) == 1 and isinstance(fees[0], dict):
                fees[0]['cost'] = fee['cost']
        return trade

    ex.order_to_trade = order_to_trade


def apply(exchange: Any) -> None:
    """ccxt okx instance patches (repeated call is a no-op)."""
    if getattr(exchange, '_bytx_okx_fixes', False):
        return
    exchange._bytx_okx_fixes = True
    _patch_parse_market(exchange)
    _patch_parse_funding_rate(exchange)
    _patch_parse_trade(exchange)
    _patch_create_order_request(exchange)
    _patch_order_book_checksum(exchange)
    _patch_ws_fills(exchange)
    _patch_ws_notice(exchange)
    _patch_ws_order_trade_fee(exchange)
