r"""Transparent Binance broker prefix in the client order id (design option A).

Binance broker attribution is the ``x-<ID>`` prefix in ``newClientOrderId`` (regular
orders) and in ``clientAlgoId`` (Algo Order API). ccxt 4.4.65 adds the prefix only
when it autogenerates the cid; a caller-supplied cid is sent as is, and ccxt never
touches ``clientAlgoId``. Services almost always pass their own cid, so without this module
their orders go unattributed.

The module works on the ccxt instance transport (a wrapper around ``exchange.request``, which
ccxt methods, the implicit API and direct ``exchange.request(...)`` calls all go through):

- **Sending.** In private requests, ``newClientOrderId``, ``clientAlgoId``,
  ``origClientOrderId``, ``cancelOrigClientOrderId`` (and ``batchOrders`` /
  ``origClientOrderIdList`` elements) get our ID prefix (spot — for ``private`` /
  ``sapi*``, futures — for ``fapi*`` / ``dapi*`` / ``papi`` / ``eapi*``).
  The caller's logical cid («d…», «t1…», «twap-…») is unchanged.
- **Response parsing.** The EXACT known prefix is stripped from response cid fields (``clientOrderId``, ``clientAlgoId``,
  ``origClientOrderId``, …, at any depth): the
  current spot/futures IDs and legacy ones (``BINANCE_BROKER_ID_LEGACY``). ccxt parses the already
  cleaned response → ``clientOrderId`` and ``info`` of orders/algo/trades are logical,
  services see their usual cids. Foreign cids (``x-OTHER…``, ``web_…``) are unchanged.
- **Fetch/cancel by cid** (GET/DELETE/PUT with ``origClientOrderId`` /
  ``clientAlgoId``): first with the prefix; if the exchange doesn't know the order (OrderNotFound,
  -2011/-2013, empty response) — with the bare cid (orders placed before the ID was enabled), then with
  legacy prefixes. The extra request happens only after «not found».
- **Length ≤ 36** (New Order: ``^[\.A-Z\:/a-z0-9_-]{1,36}$``). If ``len(prefix) + len(cid) > 36``, the cid is
  deterministically shortened to ``36 − len(prefix)`` (:func:`shorten_binance_cid`: the cid head + 12 hex of the
  SHA-256 of the whole cid) and sent WITH the prefix (reads and cancels use the same form); the exchange response
  carries the shortened cid (:func:`bytx.exchange_client_id` gives exactly what is sent).
- **Never prefixed twice:** a cid that already starts with a known prefix
  (case-insensitive) is sent as is.

Main invariant: if none of ``BINANCE_BROKER_ID_SPOT``, ``BINANCE_BROKER_ID_FUTURES``,
``BINANCE_BROKER_ID_LEGACY`` is set → the ``request`` wrapper is not installed, requests and
responses are byte-for-byte unchanged. Only a pass-through ``fetch2`` monitor is installed: if a
private API response contains a cid like ``x-<8 alnum><service cid>`` (an order under our
removed ID) — WARNING once per 10 minutes per instance and ``binance_orphan_counts()``; requests and
responses are not changed.

Disabling / rotating our ID (MANDATORY procedure):
    1. Do NOT delete the old prefix from env; move it to ``BINANCE_BROKER_ID_LEGACY``
       (comma-separated with other legacy ones). «LEGACY only» mode (own SPOT/FUTURES not
       set) is the standard way to drop an ID: new orders go out with a bare cid, responses
       are cleaned, fetch/cancel by cid after «not found» tries the legacy prefixes.
       On rotation — new ID in ``BINANCE_BROKER_ID_FUTURES``/``_SPOT``, the old one in LEGACY.
    2. Keep the prefix in LEGACY until ALL orders and algo orders placed under it are
       closed/canceled (DCA limit orders, TP/SL can live for weeks). Only then
       remove it from LEGACY. Removed earlier — orders under it are not found by cid →
       services consider them missing and place duplicates.
    3. Rolling the library back to a version without prefix stripping is FORBIDDEN while the exchange has orders with
       our ID prefix: such versions neither strip the prefix nor search by it.

Env:
    BINANCE_BROKER_ID_SPOT     — full prefix (``x-<ID>``), ≤ 14, ^[.A-Z:/a-z0-9_-]+$
    BINANCE_BROKER_ID_FUTURES  — same for futures
    BINANCE_BROKER_ID_LEGACY   — legacy prefixes, comma-separated (strip/search only);
                                 keep while live orders exist under them (see above)
"""
import hashlib
import inspect
import json
import os
import re
import time
from collections import Counter
from typing import Iterable, Optional

from loguru import logger

from bytx.net._sig.opts import resolve

BINANCE_CID_MAX_LEN = 36
ENV_BINANCE_LEGACY = 'BINANCE_BROKER_ID_LEGACY'
_ENV_SPOT = 'BINANCE_BROKER_ID_SPOT'
_ENV_FUTURES = 'BINANCE_BROKER_ID_FUTURES'
_PREFIX_RE = re.compile(r'^[.A-Z:/a-z0-9_-]{1,14}$')
_NEUTRAL = 'bx-'

# cid fields in the REQUEST: creation (POST) and addressing an existing order.
_CREATE_KEYS = ('newClientOrderId', 'clientAlgoId')
_LOOKUP_KEYS = ('origClientOrderId', 'clientAlgoId', 'cancelOrigClientOrderId')
# cid fields in the RESPONSE (any nesting level).
_RESPONSE_KEYS = frozenset({
    'clientOrderId', 'origClientOrderId', 'newClientOrderId', 'clientAlgoId',
    'origClientAlgoId', 'cancelOrigClientOrderId',
})
# Binance «order not found» codes: -2013 Order does not exist, -2011 Unknown order sent.
_NOT_FOUND_RE = re.compile(r'"code"\s*:\s*-20(?:11|13)(?!\d)')

_unattributed: Counter = Counter()
_orphans: Counter = Counter()


class BinanceCidConfig:
    """Prefixes of one ccxt instance: own spot/futures and legacy (strip only)."""

    __slots__ = ('spot', 'futures', 'legacy', 'known')

    def __init__(self, spot: Optional[str], futures: Optional[str], legacy: Iterable[str] = ()):
        self.spot = spot
        self.futures = futures
        self.legacy = tuple(p for p in dict.fromkeys(legacy) if p and p not in (spot, futures))
        known = [p for p in (spot, futures, *self.legacy) if p]
        # longest first: an ID that is a prefix of another won't «eat» someone else's tail
        self.known = tuple(sorted(dict.fromkeys(known), key=len, reverse=True))

    @property
    def active(self) -> bool:
        return bool(self.known)

    def prefix_for(self, market: str) -> Optional[str]:
        return self.spot if market == 'spot' else self.futures if market == 'futures' else None

    def __repr__(self):  # ID values aren't needed in logs — not secret, just long
        return f'BinanceCidConfig(spot={self.spot!r}, futures={self.futures!r}, legacy={self.legacy!r})'


def _env_prefix(name: str, exchange=None) -> Optional[str]:
    value = resolve(name, exchange)
    if not value or value == _NEUTRAL or not _PREFIX_RE.fullmatch(value):
        return None
    return value


def binance_cid_config_from_env(exchange=None) -> BinanceCidConfig:
    """Prefix config from the net._sig.opts registry (invalid values are ignored)."""
    legacy = []
    for part in resolve(ENV_BINANCE_LEGACY, exchange).split(','):
        part = part.strip()
        if not part:
            continue
        if _PREFIX_RE.fullmatch(part) and part != _NEUTRAL:
            legacy.append(part)
        else:
            logger.warning(f"{ENV_BINANCE_LEGACY}: value '{part}' is not a valid Binance prefix — skipped")
    return BinanceCidConfig(_env_prefix(_ENV_SPOT, exchange), _env_prefix(_ENV_FUTURES, exchange), legacy)


def binance_max_logical_cid_len(market: str = 'futures') -> int:
    """Max logical cid length at which it is sent WITH the prefix (36 − P).

    market: 'futures' (swap/future) or 'spot'. Without our own ID — 36 (no prefix).
    Caller contract: a longer cid is shortened, the prefix is kept (:func:`shorten_binance_cid`).
    """
    market = 'spot' if market in ('spot', 'margin') else 'futures'
    prefix = binance_cid_config_from_env().prefix_for(market)
    return BINANCE_CID_MAX_LEN - len(prefix) if prefix else BINANCE_CID_MAX_LEN


def strip_binance_prefix(cid, prefixes: Iterable[str]):
    """Logical cid: strips the EXACT known prefix (case-insensitive).

    Non-strings, empty and foreign cids are returned as is. The bare remainder is never
    empty: a cid equal to the prefix itself is left alone.
    """
    if not isinstance(cid, str) or not cid:
        return cid
    low = cid.lower()
    for prefix in prefixes:
        if len(cid) > len(prefix) and low.startswith(prefix.lower()):
            return cid[len(prefix):]
    return cid


def _has_known_prefix(cid: str, cfg: BinanceCidConfig) -> bool:
    low = cid.lower()
    return any(low.startswith(p.lower()) for p in cfg.known)


#: hex chars of SHA-256 in a shortened cid: different cids with a common head do not collide
_SHORT_HASH = 12


def shorten_binance_cid(cid: str, limit: int) -> str:
    """cid of length ≤ ``limit``: as is if it fits; otherwise the cid head + 12 hex of the SHA-256 of the whole cid
    (deterministic — the same cid gives the same form on retry, lookup and cancel)."""
    if len(cid) <= limit:
        return cid
    digest = hashlib.sha256(cid.encode()).hexdigest()
    return (cid[:max(0, limit - _SHORT_HASH)] + digest)[:limit]


def physical_binance_cid(cid, prefix: Optional[str], cfg: BinanceCidConfig):
    """cid to send: prefix + cid if a prefix is set and not already present; a cid that does not fit into 36 is
    shortened (:func:`shorten_binance_cid`), the prefix is kept."""
    if not prefix or not isinstance(cid, str) or not cid:
        return cid
    if _has_known_prefix(cid, cfg):
        return cid
    return prefix + shorten_binance_cid(cid, BINANCE_CID_MAX_LEN - len(prefix))


def binance_unattributed_counts() -> dict:
    """Metric: how many orders were sent WITHOUT the broker prefix (cid doesn't fit in 36)."""
    return dict(_unattributed)


def _market_of_api(api) -> Optional[str]:
    api = str(api or '')
    if api == 'private' or api.startswith('sapi'):
        return 'spot'
    if api.startswith(('fapiPrivate', 'dapiPrivate', 'papi', 'eapiPrivate')):
        return 'futures'
    return None


def _note_unattributed(cid: str, prefix: str, market: str, key: str) -> None:
    _unattributed[market] += 1
    logger.warning(
        f"🏷️ Binance {market}: {key}='{cid}' ({len(cid)} chars) with prefix {prefix} is longer than "
        f"{BINANCE_CID_MAX_LEN} — order sent WITHOUT broker attribution (cid unchanged)"
    )


def _prefix_create(value, prefix, cfg, market, key):
    out = physical_binance_cid(value, prefix, cfg)
    if out is value and isinstance(value, str) and value and not _has_known_prefix(value, cfg):
        _note_unattributed(value, prefix, market, key)
    return out


def _prefix_list_value(value, prefix, cfg):
    """origClientOrderIdList: list or a JSON array string."""
    if isinstance(value, (list, tuple)):
        return [physical_binance_cid(v, prefix, cfg) for v in value]
    if isinstance(value, str) and value.startswith('['):
        try:
            items = json.loads(value)
        except ValueError:
            return value
        if isinstance(items, list):
            return json.dumps([physical_binance_cid(v, prefix, cfg) for v in items], separators=(',', ':'))
    return value


_IDEMPOTENT = ('GET', 'DELETE', 'PUT')


def _lookup_candidates(logical: str, sent: str, cfg: BinanceCidConfig) -> list:
    """Physical cid forms for the retry lookup: bare → legacy prefixes (≤ 36), excluding sent."""
    out = []
    for candidate in [logical] + [p + logical for p in cfg.legacy
                                  if len(p) + len(logical) <= BINANCE_CID_MAX_LEN]:
        if candidate != sent and candidate not in out:
            out.append(candidate)
    return out


def _lookup_for(key: str, logical, sent, method: str, cfg: BinanceCidConfig):
    """lookup = (key, logical, sent) for the «not found» fallback, or None.

    Idempotent GET/DELETE/PUT only: a POST (spot cancelReplace with ALLOW_FAILURE) could
    place a new order a second time on retry. Set if there is at least one
    other physical form: the bare cid (≠ the sent one) or a legacy prefix with
    which the cid fits in 36 (including when our own prefix didn't fit, and in «LEGACY
    only» mode). A cid that already carries a known prefix is looked up only as is.
    """
    if method not in _IDEMPOTENT or not isinstance(logical, str) or not logical:
        return None
    if _has_known_prefix(logical, cfg):
        return None
    if not _lookup_candidates(logical, sent, cfg):
        return None
    return (key, logical, sent)


def _legacy_lookup(params: dict, method: str, cfg: BinanceCidConfig):
    """Mode without our own prefix (LEGACY only): params unchanged, lookup — the first
    non-empty key from _LOOKUP_KEYS (bare cid first, then legacy prefixes)."""
    if method not in _IDEMPOTENT or not cfg.legacy:
        return None
    for key in _LOOKUP_KEYS:
        value = params.get(key)
        if isinstance(value, str) and value:
            return _lookup_for(key, value, value, method, cfg)
    return None


def _prefix_params(params: dict, method: str, prefix: str, cfg: BinanceCidConfig, market: str):
    """Copy of params with the prefix → (params, lookup), lookup = (key, logical, sent) for the fallback."""
    out = dict(params)
    lookup = None
    is_post = method == 'POST'
    for key in _CREATE_KEYS:
        if is_post and key in out:
            out[key] = _prefix_create(out[key], prefix, cfg, market, key)
    for key in _LOOKUP_KEYS:
        if is_post and key in _CREATE_KEYS:
            continue
        value = out.get(key)
        if not isinstance(value, str) or not value:
            continue
        physical = physical_binance_cid(value, prefix, cfg)
        out[key] = physical
        if lookup is None:
            lookup = _lookup_for(key, value, physical, method, cfg)
    batch = out.get('batchOrders')
    if isinstance(batch, list):
        new_batch = []
        for item in batch:
            if isinstance(item, dict) and isinstance(item.get('newClientOrderId'), str):
                item = dict(item)
                item['newClientOrderId'] = _prefix_create(item['newClientOrderId'], prefix, cfg, market,
                                                          'newClientOrderId')
            new_batch.append(item)
        out['batchOrders'] = new_batch
    for key in ('origClientOrderIdList', 'origclientorderidlist'):
        if key in out:
            out[key] = _prefix_list_value(out[key], prefix, cfg)
    return out, lookup


# WS user data: order cid in field «c» (spot executionReport, futures ORDER_TRADE_UPDATE.o).
# Only for order/execution streams — in tickers «c» is a price.
_WS_KEYS = _RESPONSE_KEYS | {'c'}


def strip_binance_ws_payload(data, prefixes: Iterable[str]):
    """Like :func:`strip_binance_response`, plus field ``c`` of WS order events (in place)."""
    return strip_binance_response(data, prefixes, keys=_WS_KEYS)


def strip_binance_response(data, prefixes: Iterable[str], *, keys=_RESPONSE_KEYS):
    """Strips known prefixes from response cid fields at any depth (in place)."""
    prefixes = tuple(prefixes)
    if not prefixes:
        return data
    stack = [data]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            for key, value in node.items():
                if isinstance(value, str):
                    if key in keys:
                        node[key] = strip_binance_prefix(value, prefixes)
                elif isinstance(value, (dict, list)):
                    stack.append(value)
        elif isinstance(node, list):
            stack.extend(v for v in node if isinstance(v, (dict, list)))
    return data


def _is_not_found(exc: BaseException) -> bool:
    try:
        import ccxt
        if isinstance(exc, ccxt.OrderNotFound):
            return True
    except Exception:  # pragma: no cover
        pass
    return bool(_NOT_FOUND_RE.search(str(exc)))


def _is_empty(response) -> bool:
    return response is None or response == {} or response == []


def _plan(current, api, method, params) -> Optional[tuple]:
    """Shared wrapper decision (sync and async): None — pass-through call; otherwise (send, lookup)."""
    market = _market_of_api(api)
    if current is None or not current.active or market is None or not isinstance(params, dict):
        return None
    prefix = current.prefix_for(market)
    lookup = None
    send = params
    if prefix:
        send, lookup = _prefix_params(params, str(method).upper(), prefix, current, market)
    elif current.legacy:
        # «LEGACY only» (own ID removed): sent unchanged, but fetch/cancel by
        # cid after «not found» tries the legacy prefixes — orders under them are still live
        lookup = _legacy_lookup(params, str(method).upper(), current)
    return send, lookup


def _fallback_attempts(send, lookup, cfg):
    """Lookup retry params: order not found by the sent cid → bare cid (before the
    ID was enabled) → legacy prefixes. A candidate equal to the already sent cid is skipped."""
    key, logical, sent = lookup
    for candidate in _lookup_candidates(logical, sent, cfg):
        attempt = dict(send)
        attempt[key] = candidate
        yield attempt


def install_binance_cid_prefix(exchange, cfg: BinanceCidConfig) -> None:
    """Installs (once) the exchange.request transport wrapper; cfg is always refreshed.

    Inactive config (neither own nor legacy IDs) — the wrapper is not installed, and
    a previously installed one becomes pass-through. Async instance (ccxt.async_support) —
    async wrapper, sync ccxt — sync wrapper with the same logic (_plan/_fallback_attempts).
    """
    exchange._bytx_binance_cid = cfg
    if not cfg.active:
        _install_orphan_monitor(exchange)
        return
    if getattr(exchange, '_bytx_binance_cid_installed', False):
        return
    original = exchange.request
    if inspect.iscoroutinefunction(original):
        exchange.request = _async_request_wrapper(exchange, original)
    else:
        exchange.request = _sync_request_wrapper(exchange, original)
    exchange._bytx_binance_cid_installed = True


def _async_request_wrapper(exchange, original):
    async def request(path, api='public', method='GET', params={}, *rest, **kw):
        current = getattr(exchange, '_bytx_binance_cid', None)
        plan = _plan(current, api, method, params)
        if plan is None:
            return await original(path, api, method, params, *rest, **kw)
        send, lookup = plan
        try:
            response = await original(path, api, method, send, *rest, **kw)
        except Exception as exc:
            if lookup is None or not _is_not_found(exc):
                raise
            return await _fallback(path, api, method, send, rest, kw, lookup, current, exc)
        if lookup is not None and _is_empty(response):
            return await _fallback(path, api, method, send, rest, kw, lookup, current, None,
                                   first=response)
        return strip_binance_response(response, current.known)

    async def _fallback(path, api, method, send, rest, kw, lookup, cfg, first_exc, first=None):
        last_exc, last = first_exc, first
        for attempt in _fallback_attempts(send, lookup, cfg):
            try:
                response = await original(path, api, method, attempt, *rest, **kw)
            except Exception as exc:
                if not _is_not_found(exc):
                    raise
                last_exc, last = exc, None
                continue
            if _is_empty(response):
                last_exc, last = None, response
                continue
            return strip_binance_response(response, cfg.known)
        if last_exc is not None:
            raise last_exc
        return strip_binance_response(last, cfg.known)

    return request


def _sync_request_wrapper(exchange, original):
    """Sync ccxt (api_validator etc.): same logic as the async wrapper, without await."""
    def request(path, api='public', method='GET', params={}, *rest, **kw):
        current = getattr(exchange, '_bytx_binance_cid', None)
        plan = _plan(current, api, method, params)
        if plan is None:
            return original(path, api, method, params, *rest, **kw)
        send, lookup = plan
        try:
            response = original(path, api, method, send, *rest, **kw)
        except Exception as exc:
            if lookup is None or not _is_not_found(exc):
                raise
            return _fallback(path, api, method, send, rest, kw, lookup, current, exc)
        if lookup is not None and _is_empty(response):
            return _fallback(path, api, method, send, rest, kw, lookup, current, None,
                             first=response)
        return strip_binance_response(response, current.known)

    def _fallback(path, api, method, send, rest, kw, lookup, cfg, first_exc, first=None):
        last_exc, last = first_exc, first
        for attempt in _fallback_attempts(send, lookup, cfg):
            try:
                response = original(path, api, method, attempt, *rest, **kw)
            except Exception as exc:
                if not _is_not_found(exc):
                    raise
                last_exc, last = exc, None
                continue
            if _is_empty(response):
                last_exc, last = None, response
                continue
            return strip_binance_response(response, cfg.known)
        if last_exc is not None:
            raise last_exc
        return strip_binance_response(last, cfg.known)

    return request


# ─── «orphaned» prefixed orders without env (WARNING only) ──────────────────

# Bot logical cids (heuristic): limit d+20hex, algo s1/t1+hex, TWAP, closeall,
# MCP, library/SDK autogeneration with our own ID — ``uuid4().hex[:22]``.
# ccxt autogeneration — brokerId + ``uuid22()`` = ``format(getrandbits(88), 'x')``: RANDOM hex
# of 1–22 chars (not base62). Any ccxt-based software with its own broker ID (``options['broker']``)
# yields ``x-<8> + 22 hex`` — «22 hex» alone doesn't make it our order. Our tail is told apart by
# uuid4 structure (RFC 4122): 13th char — version ``4``, 17th — variant ``[89ab]``; a random
# ccxt tail passes with probability 1/64 (the rest is WARNING only, behavior unchanged).
# Orders from old library versions (ID in options['broker'], cid generated by ccxt) are
# indistinguishable by format from third-party ccxt software — the heuristic doesn't catch them (no false confidence).
_OWN_UUID22 = r'[0-9a-f]{12}4[0-9a-f]{3}[89ab][0-9a-f]{5}'
_BOT_CID_TAIL = (r'(?:d[0-9a-f]{20}|[st]1[0-9a-f]{16,24}|twap-[\w.-]+|closeall-[\w.-]+|mcp-[\w.-]+|'
                 + _OWN_UUID22 + ')')
_ORPHAN_CID_RE = re.compile(r'^(x-[A-Za-z0-9]{8})' + _BOT_CID_TAIL + r'$')
ORPHAN_WARN_INTERVAL_SEC = 600.0
_CCXT_IDS_LOWER = frozenset({'x-r4bd3s82', 'x-xcktghcu'})   # built-in ccxt IDs — not our orders


def find_orphan_prefixed_cid(data) -> Optional[tuple]:
    """(prefix, cid) of the first response cid like x-<8 alnum><service cid>, or None.

    Marks orders placed under our Binance ID that was later removed from env
    without moving it to BINANCE_BROKER_ID_LEGACY: fetch/cancel by logical cid won't find them.
    """
    stack = [data]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            for key, value in node.items():
                if isinstance(value, str):
                    if key in _RESPONSE_KEYS and value[:2] in ('x-', 'X-'):
                        m = _ORPHAN_CID_RE.match(value)
                        if m and m.group(1).lower() not in _CCXT_IDS_LOWER:
                            return m.group(1), value
                elif isinstance(value, (dict, list)):
                    stack.append(value)
        elif isinstance(node, list):
            stack.extend(v for v in node if isinstance(v, (dict, list)))
    return None


def find_orphan_prefixed_cids(data) -> list:
    """All (prefix, cid) like x-<8 alnum><service cid> in the response (excluding CCXT IDs), in traversal order."""
    found = []
    stack = [data]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            for key, value in node.items():
                if isinstance(value, str):
                    if key in _RESPONSE_KEYS and value[:2] in ('x-', 'X-'):
                        m = _ORPHAN_CID_RE.match(value)
                        if m and m.group(1).lower() not in _CCXT_IDS_LOWER:
                            found.append((m.group(1), value))
                elif isinstance(value, (dict, list)):
                    stack.append(value)
        elif isinstance(node, list):
            stack.extend(v for v in node if isinstance(v, (dict, list)))
    return found


# Account open orders in one request without symbol (Binance weight: openOrders 40 / 80 spot,
# openAlgoOrders without symbol — 40): regular and conditional (Algo Order API).
_SCAN_ENDPOINTS = {
    'futures': (('openOrders', 'fapiPrivate'), ('openAlgoOrders', 'fapiPrivate')),
    'spot': (('openOrders', 'private'),),
}


async def scan_prefixed_orders(exchange, market: str = 'futures') -> list:
    """Startup check: open orders with a broker prefix ``x-<ID>``
    that is not in the instance config (neither own nor in ``BINANCE_BROKER_ID_LEGACY``).

    Such orders were placed under an ID that was later removed (or the library/mode was rolled back): fetch and
    cancel by logical cid won't find them — the service will consider them missing and place duplicates.
    Found — WARNING (without key values) and counter ``binance_orphan_counts``. Returns
    a sorted list of such cids ([] — none, or the exchange is not Binance). Makes 1–2 private
    requests — call at startup, not in a loop. Request errors are not swallowed.
    """
    if str(getattr(exchange, 'id', '')).lower() != 'binance':
        return []
    market = 'spot' if market in ('spot', 'margin') else 'futures'
    cfg = exchange_binance_cid_config(exchange)
    known = {p.lower() for p in cfg.known} if cfg is not None else set()
    hits: dict = {}
    for path, api in _SCAN_ENDPOINTS[market]:
        response = await exchange.request(path, api, 'GET', {})
        for prefix, cid in find_orphan_prefixed_cids(response):
            if prefix.lower() not in known:
                hits[cid] = prefix
    if hits:
        prefixes = sorted(set(hits.values()))
        for prefix in prefixes:
            _orphans[prefix] += 1
        logger.warning(
            f"🏷️ Binance {market}: {len(hits)} open orders with broker prefix {', '.join(prefixes)}, "
            f"which is not in the config (BINANCE_BROKER_ID_FUTURES/SPOT/LEGACY). Fetch and cancel by cid "
            f"won't find them — risk of duplicates. Put the prefix back into {ENV_BINANCE_LEGACY} (or enable your ID)")
    return sorted(hits)


def _install_orphan_monitor(exchange) -> None:
    """Without own/legacy IDs: pass-through fetch2 wrapper, WARNING only (once per 10 min per instance).

    Requests and responses are unchanged (exchange.request is not touched). Parse errors are suppressed.
    Async instance only (ccxt.async_support): in sync ccxt fetch2 is a plain
    function, an async wrapper would return a coroutine instead of a response — the monitor is not installed.
    """
    if getattr(exchange, '_bytx_binance_cid_monitor', False):
        return
    original = getattr(exchange, 'fetch2', None)
    if original is None or not inspect.iscoroutinefunction(original):
        return
    exchange._bytx_binance_cid_monitor = True

    # fetch2 signature depends on the ccxt version (4.5 dropped headers/body) — pass through as is.
    async def fetch2(path, api='public', method='GET', *args, **kwargs):
        response = await original(path, api, method, *args, **kwargs)
        try:
            _check_orphans(exchange, api, response)
        except Exception:  # pragma: no cover — monitoring never breaks a request
            pass
        return response

    exchange.fetch2 = fetch2


def _check_orphans(exchange, api, response) -> None:
    if exchange_binance_cid_config(exchange) is not None or _market_of_api(api) is None:
        return
    now = time.monotonic()
    last = getattr(exchange, '_bytx_binance_cid_orphan_ts', None)
    if last is not None and now - last < ORPHAN_WARN_INTERVAL_SEC:
        return
    hit = find_orphan_prefixed_cid(response)
    if hit is None:
        return
    exchange._bytx_binance_cid_orphan_ts = now
    prefix, cid = hit
    _orphans[prefix] += 1
    logger.warning(
        f"🏷️ Binance: response has an order with broker prefix '{cid}', but BINANCE_BROKER_ID_* are not set. "
        f"If {prefix} is your removed ID, put it back into {ENV_BINANCE_LEGACY} until all orders under it "
        f"are closed: otherwise fetch/cancel by cid won't find them (risk of duplicates). Behavior unchanged")


def binance_orphan_counts() -> dict:
    """Metric: how many times (subject to the WARNING interval) prefixed cids were seen without env."""
    return dict(_orphans)


def exchange_binance_cid_config(exchange) -> Optional[BinanceCidConfig]:
    """Active prefix config of the instance (None — prefixes are inactive)."""
    cfg = getattr(exchange, '_bytx_binance_cid', None)
    return cfg if isinstance(cfg, BinanceCidConfig) and cfg.active else None


__all__ = [
    'BINANCE_CID_MAX_LEN', 'ENV_BINANCE_LEGACY', 'BinanceCidConfig',
    'binance_cid_config_from_env', 'binance_max_logical_cid_len', 'binance_unattributed_counts',
    'strip_binance_prefix', 'physical_binance_cid', 'strip_binance_response', 'strip_binance_ws_payload',
    'install_binance_cid_prefix', 'exchange_binance_cid_config', 'shorten_binance_cid',
    'find_orphan_prefixed_cid', 'find_orphan_prefixed_cids', 'scan_prefixed_orders',
    'binance_orphan_counts', 'ORPHAN_WARN_INTERVAL_SEC',
]
