"""Removes ccxt broker tags from exchange requests.

By default ccxt 4.4.65 tags requests with its own broker identifiers,
and the broker share of the fees goes to ccxt:

- binance: options['broker'] (x-R4BD3S82 spot/margin, x-xcKtGhcu futures) —
  newClientOrderId prefix when no cid is passed (create_order and implicit
  POST order in sign). An empty value/None does not disable it: ccxt falls back to
  the built-in default, so we set our own or a neutral prefix;
- okx: options['brokerId'] (e847386590ce4dBC) — clOrdId + tag in create_order
  and in sign for trade/order, trade/order-algo, trade/batch-orders (None also
  falls back to the default). ccxt sets tag ONLY when it auto-generates clOrdId; with
  the caller's own cid no tag is sent. So with our own code
  (OKX_BROKER_CODE) tag = code is set on ALL placement requests (see
  _install_okx_sign); without a code, tag is not sent at all;
- bybit: options['brokerId'] ('CCXT') — Referer header;
- bingx: header X-SOURCE-KEY = options['broker'] or 'CCXT' (cannot be disabled via
  options; the BingX signature covers only params, not headers);
- bitget: header X-CHANNEL-API-CODE = options['broker'] ('p4sve' — the ccxt code) on all
  signed requests (Place Order docs: «API Broker rebate identifier … X-CHANNEL-API-CODE»);
- gate: header X-Gate-Channel-Id = 'ccxt' in the instance ``headers`` (every REST request) and in
  WS order ``req_header`` (spot/futures.order_place); cannot be disabled via options;
- mexc: header source = options['broker'] ('CCXT') on all signed spot and contract requests
  (MEXC Broker FAQ: «header 'source: Broker_ID'», spot and futures share one Broker_ID);
- htx: undocumented channel_code = options['broker']['id'] ('AA03022abc') in every contract
  order and the same prefix in spot client-order-id when no own id is given — removed; HTX docs have no
  broker tag mechanism, so the SDK does not set its own ID;
- kucoin / kucoinfutures: options['partner'] (ccxt / ccxtfutures) — headers
  KC-API-PARTNER*;
- hyperliquid: action['brokerCode'] = 1 in create_orders (without vaultAddress),
  the field is part of the signed action. Own builder fee (HL_BUILDER_*) — exchanges.hyperliquid.builder.

apply_broker_ids(exchange) is called right after the ccxt instance is created (before
the broker provider is attached) and sets our IDs from env or neutral values with no
attribution. Safe to call repeatedly.

Env (all optional):
    BINANCE_BROKER_ID_SPOT     — spot/margin cid prefix (≤ 14 chars, ^[.A-Z:/a-z0-9_-]+$)
    BINANCE_BROKER_ID_FUTURES  — future/delivery/swap/option cid prefix (same)
    BINANCE_BROKER_ID_LEGACY   — previous prefixes, comma-separated: stripped from responses
                                 and tried when finding/cancelling by cid (binance_cid)
    Our binance ID is also put into callers' cids (transparently, see exchanges.binance.broker_cid).
    Disabling/rotating the Binance ID: MOVE the previous prefix to BINANCE_BROKER_ID_LEGACY
    (do not delete it) and keep it there until all orders/algos under it are closed; "LEGACY
    only" is the standard way to remove the ID. Rolling back to a library version without prefix stripping while
    prefixed orders exist is forbidden (such versions can't find them by cid → duplicates). Without env, on seeing
    x-<ID><service cid> in responses — a WARNING (binance_cid, behavior unchanged).
    OKX_BROKER_CODE            — OKX broker code (alnum, ≤ 16): tag on all orders
                                 (including those with own clOrdId) and the clOrdId auto-generation prefix
    BYBIT_BROKER_ID            — Referer header value
    BINGX_SOURCE_KEY           — X-SOURCE-KEY header value
    BITGET_BROKER_ID           — X-CHANNEL-API-CODE header value (broker channel api code)
    GATE_CHANNEL_ID            — X-Gate-Channel-Id header value
    MEXC_BROKER_ID             — source header value (MEXC Broker_ID)
KuCoin — KC-API-PARTNER* partner signature (_install_kucoin_sign).
"""
import base64
import hashlib
import hmac
import re
from typing import Optional

from loguru import logger

from bytx.exchanges.hyperliquid import builder as hl_builder
from bytx.net._sig import opts as registry
from bytx.net._sig.opts import BrokerConfig, attach_config, resolve
from bytx.exchanges.binance.broker_cid import (
    ENV_BINANCE_LEGACY,
    binance_cid_config_from_env,
    install_binance_cid_prefix,
)

# Built-in ccxt 4.4.65 broker IDs — must never reach the exchange.
CCXT_BINANCE_BROKER_IDS = frozenset({'x-R4BD3S82', 'x-xcKtGhcu'})
CCXT_OKX_BROKER_ID = 'e847386590ce4dBC'
CCXT_BYBIT_BROKER_ID = 'CCXT'
CCXT_BINGX_SOURCE_KEY = 'CCXT'
CCXT_KUCOIN_PARTNER_IDS = frozenset({'ccxt', 'ccxtfutures'})
CCXT_BITGET_BROKER_ID = 'p4sve'
CCXT_GATE_CHANNEL_ID = 'ccxt'
BITGET_HEADER = 'X-CHANNEL-API-CODE'
CCXT_MEXC_BROKER_ID = 'CCXT'
MEXC_HEADER = 'source'
CCXT_HTX_BROKER_ID = 'AA03022abc'
GATE_HEADER = 'X-Gate-Channel-Id'
# HTTP header value: printable ASCII without spaces (exchange docs don't specify the code format)
_HEADER_VALUE_RE = re.compile(r'^[\x21-\x7e]{1,64}$')
_GATE_IDS = ('gate', 'gateio')

# Neutral values with no broker attribution.
# Binance: prefix + uuid22 must pass ^[.A-Z:/a-z0-9_-]{1,36}$ → 4 + 22 = 26.
# No 'x-': Binance broker prefixes look like 'x-<code>'.
NEUTRAL_BINANCE_PREFIX = 'bx-'
# OKX: clOrdId is only [A-Za-z0-9]{1,32} → 3 + 16 = 19. Not sent as tag.
NEUTRAL_OKX_PREFIX = 'bx'

_BINANCE_SPOT_TYPES = ('spot', 'margin')
_BINANCE_FUTURES_TYPES = ('future', 'delivery', 'swap', 'option')
_BINANCE_PREFIX_RE = re.compile(r'^[.A-Z:/a-z0-9_-]{1,14}$')   # 14 + 22 ≤ 36
_OKX_CODE_RE = re.compile(r'^[A-Za-z0-9]{1,16}$')               # tag ≤ 16, 16 + 16 ≤ 32
_OKX_ORDER_PATHS = frozenset({'trade/order', 'trade/order-algo', 'trade/batch-orders'})
# OKX placement requests that accept tag (broker code). trade/close-position —
# closing a position with a market order (ccxt close_position). amend-order / amend-algos
# do not accept tag: tag lives on the order from placement.
_OKX_TAG_PATHS = _OKX_ORDER_PATHS | {'trade/close-position'}
_OKX_IDS = ('okx', 'myokx', 'okxus')

ENV_BINANCE_SPOT = 'BINANCE_BROKER_ID_SPOT'
ENV_BINANCE_FUTURES = 'BINANCE_BROKER_ID_FUTURES'
ENV_OKX = 'OKX_BROKER_CODE'
ENV_BYBIT = 'BYBIT_BROKER_ID'
ENV_BINGX = 'BINGX_SOURCE_KEY'
ENV_BITGET = registry.BITGET
ENV_GATE = registry.GATE
ENV_MEXC = registry.MEXC

_logged: set = set()


def _env(name: str, pattern: Optional[re.Pattern] = None, exchange=None) -> Optional[str]:
    """Value from the registry (net._sig.opts) or None; invalid — WARNING and None."""
    value = resolve(name, exchange)
    if not value:
        return None
    if pattern is not None and not pattern.fullmatch(value):
        logger.warning(f"{name}: value is not in the exchange format — using a neutral one")
        return None
    return value


def _flag(name: str, value: Optional[str]) -> str:
    return f"{name}={'set (own)' if value else 'not set (neutral)'}"


# Registry keys the exchange's tags depend on (source log).
_SOURCE_KEYS = {
    'binance': (registry.BINANCE_SPOT, registry.BINANCE_FUTURES, registry.BINANCE_LEGACY),
    'okx': (registry.OKX,),
    'bybit': (registry.BYBIT,),
    'bingx': (registry.BINGX,),
    'bitget': (registry.BITGET,),
    'gate': (registry.GATE,),
    'mexc': (registry.MEXC,),
    'kucoin': (registry.KUCOIN_AVAILABLE, registry.KUCOIN_PARTNER, registry.KUCOIN_KEY),
    'hyperliquid': (registry.HL_ADDRESS, registry.HL_TESTNET_ADDRESS, registry.HL_FEE),
}


def broker_id_sources(exchange) -> dict:
    """{key: source} of the instance exchange's broker IDs: instance / process / env / mode:* /
    disabled:* / none. ID values are not returned."""
    ex_id = _exchange_id(exchange)
    family = ('binance' if ex_id.startswith('binance') else 'okx' if ex_id in _OKX_IDS
              else 'kucoin' if ex_id in ('kucoin', 'kucoinfutures') else 'gate' if ex_id in _GATE_IDS else ex_id)
    return registry.sources(_SOURCE_KEYS.get(family, ()), exchange)


def _log_once(exchange_id: str, message: str, exchange=None) -> None:
    # once per (exchange, sources): a different ID source on the next instance is logged again
    src = broker_id_sources(exchange) if exchange is not None else {}
    key = (exchange_id, tuple(sorted(src.items())), registry.broker_mode())
    if key in _logged:
        return
    _logged.add(key)
    origin = ', '.join(f'{k}={v}' for k, v in src.items())
    logger.info(f"🏷️ ccxt broker tags removed [{exchange_id}]: {message}"
                + (f" | sources: {origin}" if origin else '')
                + f" | mode {registry.MODE_ENV}={registry.broker_mode()}")


def _exchange_id(exchange) -> str:
    return str(getattr(exchange, 'id', '') or '').lower()


# ─── binance ────────────────────────────────────────────────────────────────

def _apply_binance(exchange) -> str:
    spot = _env(ENV_BINANCE_SPOT, _BINANCE_PREFIX_RE, exchange)
    futures = _env(ENV_BINANCE_FUTURES, _BINANCE_PREFIX_RE, exchange)
    broker = {t: spot or NEUTRAL_BINANCE_PREFIX for t in _BINANCE_SPOT_TYPES}
    broker.update({t: futures or NEUTRAL_BINANCE_PREFIX for t in _BINANCE_FUTURES_TYPES})
    exchange.options['broker'] = broker   # new dict: don't mutate the shared default
    # Transparent own-ID prefix in callers' cids (newClientOrderId/clientAlgoId/
    # origClientOrderId) and stripping it from responses — exchanges.binance.broker_cid. Without env
    # (no own or previous IDs set) the wrapper is not installed: requests are byte-for-byte unchanged.
    cfg = binance_cid_config_from_env(exchange)
    install_binance_cid_prefix(exchange, cfg)
    legacy = f", {ENV_BINANCE_LEGACY}={len(cfg.legacy)} pcs." if cfg.legacy else ''
    return f"{_flag(ENV_BINANCE_SPOT, spot)}, {_flag(ENV_BINANCE_FUTURES, futures)}{legacy}"


# ─── okx ────────────────────────────────────────────────────────────────────

def okx_is_own_code(broker_id) -> bool:
    """True if brokerId is our broker code (not neutral and not ccxt's)."""
    return bool(broker_id) and broker_id not in (NEUTRAL_OKX_PREFIX, CCXT_OKX_BROKER_ID)


def okx_tag_from_options(options) -> Optional[str]:
    """OKX tag from the ccxt instance options: our code (options['brokerId']) or None.

    options['brokerId'] is set by apply_broker_ids from OKX_BROKER_CODE; a neutral
    prefix, the ccxt code and a value not in tag format (alnum ≤ 16) yield no tag.
    """
    code = options.get('brokerId') if isinstance(options, dict) else None
    if not okx_is_own_code(code) or not _OKX_CODE_RE.fullmatch(str(code)):
        return None
    return str(code)


def okx_broker_tag(exchange) -> Optional[str]:
    """Our OKX broker code for the tag field, or None (not OKX / code not set)."""
    if _exchange_id(exchange) not in _OKX_IDS:
        return None
    return okx_tag_from_options(getattr(exchange, 'options', None))


def _install_okx_sign(exchange) -> None:
    """OKX signing: own-code tag on all orders / neutral mode without tag.

    Own code (OKX_BROKER_CODE): for POST trade/order, trade/order-algo,
    trade/batch-orders and trade/close-position, tag = code is set in every params
    item — also when clOrdId is passed by the caller (ccxt does not set tag
    in that case). clOrdId is untouched: cid reconciliation is unchanged. Inserted before signing and
    before params are copied into the body, so tag goes out in both dict and list requests.

    ccxt.okx.sign for trade/order, trade/order-algo, trade/batch-orders without
    clOrdId appends clOrdId = brokerId + uuid16 and tag = brokerId directly into
    params. For dict params this never reaches the body (the body is built from a copy of
    params before the insertion); for a list (batch-orders, also used by create_order)
    it does. The wrapper runs before signing: removes tag with a neutral/ccxt value and
    for list items without clOrdId sets a neutral clOrdId itself (as
    ccxt would, but without tag). A dict without clOrdId is left alone — ccxt doesn't
    send it anyway.
    """
    original_sign = exchange.sign

    # The sign signature depends on the ccxt version (4.5 dropped headers/body) — pass through as is.
    def sign(path, api='public', method='GET', params={}, *args, **kwargs):
        if api == 'private' and method == 'POST' and path in _OKX_TAG_PATHS:
            own_tag = okx_broker_tag(exchange)
            is_list = isinstance(params, list)
            for entry in (params if is_list else [params]):
                if not isinstance(entry, dict):
                    continue
                if own_tag:
                    # tag is broker attribution only; it does not affect clOrdId/reconciliation
                    entry['tag'] = own_tag
                    continue
                if path not in _OKX_ORDER_PATHS:
                    continue
                if entry.get('tag') in (NEUTRAL_OKX_PREFIX, CCXT_OKX_BROKER_ID):
                    entry.pop('tag')
                if is_list and entry.get('clOrdId') is None:
                    entry['clOrdId'] = NEUTRAL_OKX_PREFIX + exchange.uuid16()
        return original_sign(path, api, method, params, *args, **kwargs)

    exchange.sign = sign


def _apply_okx(exchange, first: bool) -> str:
    code = _env(ENV_OKX, _OKX_CODE_RE, exchange)
    exchange.options['brokerId'] = code or NEUTRAL_OKX_PREFIX
    if first:
        # the neutral prefix is not sent as tag (tag is the OKX broker
        # attribution field; don't send an unregistered code there)
        _install_okx_sign(exchange)
    return _flag(ENV_OKX, code)


# ─── bybit ──────────────────────────────────────────────────────────────────

def _apply_bybit(exchange) -> str:
    referer = _env(ENV_BYBIT, exchange=exchange)
    if referer:
        exchange.options['brokerId'] = referer
    else:
        exchange.options.pop('brokerId', None)   # no key → no Referer header
    return _flag(ENV_BYBIT, referer)


# ─── bingx ──────────────────────────────────────────────────────────────────

def _install_bingx_sign(exchange) -> None:
    original_sign = exchange.sign

    def sign(path, api='public', method='GET', params={}, *args, **kwargs):
        request = original_sign(path, api, method, params, *args, **kwargs)
        h = request.get('headers')
        if isinstance(h, dict) and h.get('X-SOURCE-KEY') == CCXT_BINGX_SOURCE_KEY:
            # the BingX signature is an HMAC over request params; headers are not part of it
            h.pop('X-SOURCE-KEY')
        return request

    exchange.sign = sign


def _apply_bingx(exchange, first: bool) -> str:
    key = _env(ENV_BINGX, exchange=exchange)
    if key:
        exchange.options['broker'] = key
    else:
        exchange.options.pop('broker', None)
    if first:
        _install_bingx_sign(exchange)
    return _flag(ENV_BINGX, key)


# ─── bitget ─────────────────────────────────────────────────────────────────

def _install_bitget_sign(exchange) -> None:
    original_sign = exchange.sign

    def sign(path, api='public', method='GET', params={}, *args, **kwargs):
        request = original_sign(path, api, method, params, *args, **kwargs)
        h = request.get('headers')
        # ccxt sets the header from options['broker'] (own code — _apply_bitget); without a code — None or
        # the ccxt default: the header is removed. The Bitget signature is timestamp + method + path + query/body,
        # headers are not part of it
        if isinstance(h, dict) and h.get(BITGET_HEADER, '') in (None, CCXT_BITGET_BROKER_ID):
            h.pop(BITGET_HEADER)
        return request

    exchange.sign = sign


def _apply_bitget(exchange, first: bool) -> str:
    code = _env(ENV_BITGET, _HEADER_VALUE_RE, exchange)
    if code:
        exchange.options['broker'] = code
    else:
        exchange.options.pop('broker', None)
    if first:
        _install_bitget_sign(exchange)
    return _flag(ENV_BITGET, code)


# ─── gate ───────────────────────────────────────────────────────────────────

def _gate_ws_header(exchange) -> None:
    """Gate WS orders (``request_private``): ccxt puts ``req_header`` with 'ccxt' — our ID or
    no header. The WS signature covers ``req_param`` (event, channel, params, time), not the header."""
    original_watch = exchange.watch

    def watch(url, message_hash, message=None, *args, **kwargs):
        payload = message.get('payload') if isinstance(message, dict) else None
        header = payload.get('req_header') if isinstance(payload, dict) else None
        if isinstance(header, dict) and GATE_HEADER in header:
            own = (getattr(exchange, 'headers', None) or {}).get(GATE_HEADER)   # own — from _apply_gate
            if own:
                header[GATE_HEADER] = own
            else:
                header.pop(GATE_HEADER)
                if not header:
                    payload.pop('req_header')
        return original_watch(url, message_hash, message, *args, **kwargs)

    exchange.watch = watch


def _apply_gate(exchange, first: bool) -> str:
    code = _env(ENV_GATE, _HEADER_VALUE_RE, exchange)
    headers = {k: v for k, v in dict(getattr(exchange, 'headers', None) or {}).items() if k != GATE_HEADER}
    if code:
        headers[GATE_HEADER] = code
    exchange.headers = headers   # new dict: don't mutate the shared class default
    if first and callable(getattr(exchange, 'watch', None)):
        _gate_ws_header(exchange)
    return _flag(ENV_GATE, code)


# ─── mexc ───────────────────────────────────────────────────────────────────

def _install_mexc_sign(exchange) -> None:
    original_sign = exchange.sign

    def sign(path, api='public', method='GET', params={}, *args, **kwargs):
        request = original_sign(path, api, method, params, *args, **kwargs)
        h = request.get('headers')
        # ccxt sets source from options['broker'] (own — _apply_mexc), otherwise 'CCXT': removed.
        # The MEXC signature (spot — query, contracts — accessKey + time + params) does not include the header
        if isinstance(h, dict) and h.get(MEXC_HEADER, '') in (None, CCXT_MEXC_BROKER_ID):
            h.pop(MEXC_HEADER)
        return request

    exchange.sign = sign


def _apply_mexc(exchange, first: bool) -> str:
    code = _env(ENV_MEXC, _HEADER_VALUE_RE, exchange)
    if code:
        exchange.options['broker'] = code
    else:
        exchange.options['broker'] = CCXT_MEXC_BROKER_ID   # removed in sign (ccxt: safe_string → 'CCXT')
    if first:
        _install_mexc_sign(exchange)
    return _flag(ENV_MEXC, code)


# ─── htx ────────────────────────────────────────────────────────────────────

def _install_htx_requests(exchange) -> None:
    original_contract = exchange.create_contract_order_request

    def create_contract_order_request(symbol, type, side, amount, price=None, params={}):
        request = original_contract(symbol, type, side, amount, price, params)
        # channel_code is not in the Place An Order docs (ccxt — 'AA03022abc' or None from options)
        if isinstance(request, dict) and request.get('channel_code') in (None, '', CCXT_HTX_BROKER_ID):
            request.pop('channel_code', None)
        return request

    exchange.create_contract_order_request = create_contract_order_request
    original_spot = exchange.create_spot_order_request

    async def create_spot_order_request(symbol, type, side, amount, price=None, params={}):
        # without own client-order-id ccxt sets brokerId + uuid — use our own neutral uuid
        if exchange.safe_string_2(params, 'clientOrderId', 'client-order-id') is None:
            params = exchange.extend(params, {'clientOrderId': exchange.uuid()})
        return await original_spot(symbol, type, side, amount, price, params)

    exchange.create_spot_order_request = create_spot_order_request


def _apply_htx(exchange, first: bool) -> str:
    if first:
        _install_htx_requests(exchange)
    return 'HTX: no broker tag in the docs — not set'


# ─── kucoin / kucoinfutures ─────────────────────────────────────────────────

def kucoin_partner_sign(*, timestamp: str, partner: str, api_key: str, broker_key: str) -> str:
    """KC-API-PARTNER-SIGN (KuCoin API Broker): base64(HMAC-SHA256(timestamp + partner + api_key, broker_key))."""
    msg = f'{timestamp}{partner}{api_key}'.encode()
    return base64.b64encode(hmac.new(broker_key.encode(), msg, hashlib.sha256).digest()).decode()


def _install_kucoin_sign(exchange) -> None:
    if getattr(exchange, '_bytx_kucoin_sign', False):
        return
    exchange._bytx_kucoin_sign = True
    original = exchange.sign

    # The sign signature depends on the ccxt version (4.5 dropped headers/body) — pass through as is.
    def sign(path, api='public', method='GET', params={}, *args, **kwargs):
        request = original(path, api, method, params, *args, **kwargs)
        headers = request.get('headers') or {}
        # private requests only: ccxt has already set the user's key and time
        if 'KC-API-KEY' not in headers or 'KC-API-TIMESTAMP' not in headers:
            return request
        if not registry.broker_available('kucoin', exchange):
            return request
        partner = resolve(registry.KUCOIN_PARTNER, exchange)
        key = resolve(registry.KUCOIN_KEY, exchange)
        if not (partner and key):
            return request
        headers['KC-API-PARTNER'] = partner
        headers['KC-API-PARTNER-SIGN'] = kucoin_partner_sign(
            timestamp=str(headers['KC-API-TIMESTAMP']), partner=partner,
            api_key=str(headers['KC-API-KEY']), broker_key=key)
        headers['KC-BROKER-NAME'] = resolve(registry.KUCOIN_NAME, exchange) or partner
        headers['KC-API-PARTNER-VERIFY'] = 'true'
        request['headers'] = headers
        return request

    exchange.sign = sign


def _apply_kucoin(exchange) -> str:
    # without id/secret ccxt does not set its KC-API-PARTNER*; our headers — via the sign wrapper
    exchange.options['partner'] = {'spot': {}, 'future': {}}
    _install_kucoin_sign(exchange)
    own = registry.broker_available('kucoin', exchange)
    return 'ccxt partner removed, ' + ('own partner signature' if own else 'no partner signature')


# ─── hyperliquid ────────────────────────────────────────────────────────────

def _install_hyperliquid_sign(exchange) -> None:
    original = exchange.sign_l1_action

    def sign_l1_action(action, nonce, vaultAdress=None):
        # ccxt puts the same dict into the request body → the field is removed from both the signature and the request
        if isinstance(action, dict) and action.get('brokerCode') == 1:
            action.pop('brokerCode')
        # builder fee (layer A): only if layer B (the create_orders wrapper) allowed it
        # for THIS call; without HL_BUILDER_ADDRESS the action is unchanged (exchanges.hyperliquid.builder)
        hl_builder.on_sign_l1_action(action, nonce, vaultAdress)
        return original(action, nonce, vaultAdress)

    exchange.sign_l1_action = sign_l1_action


def _apply_hyperliquid(exchange, first: bool) -> str:
    if first:
        _install_hyperliquid_sign(exchange)
        # builder fee layer B: create_orders (gate, retry without builder), handle_errors
        # (typed rejection), on_rest_response (numeric maxBuilderFee response)
        hl_builder.install(exchange)
    cfg = hl_builder.config_for_exchange(exchange)
    return 'brokerCode removed, builder fee ' + (
        f'on ({cfg.address}, f={cfg.fee})' if cfg else 'off (HL_BUILDER_ADDRESS not set)')


# ─── public ─────────────────────────────────────────────────────────────────

def apply_broker_ids(exchange, config: Optional[BrokerConfig] = None) -> None:
    """Replaces ccxt broker IDs with ours (net._sig.opts) or neutral ones. Idempotent.

    Call right after creating the ccxt instance, before attaching the broker provider.
    config — configuration for this instance only (otherwise process / env / built-in).
    """
    if exchange is None or not isinstance(getattr(exchange, 'options', None), dict):
        return
    attach_config(exchange, config)
    ex_id = _exchange_id(exchange)
    first = not getattr(exchange, '_bytx_broker_ids_applied', False)
    if ex_id.startswith('binance'):
        info = _apply_binance(exchange)
    elif ex_id in ('okx', 'myokx', 'okxus'):
        info = _apply_okx(exchange, first)
    elif ex_id == 'bybit':
        info = _apply_bybit(exchange)
    elif ex_id == 'bingx':
        info = _apply_bingx(exchange, first)
    elif ex_id == 'bitget':
        info = _apply_bitget(exchange, first)
    elif ex_id in _GATE_IDS:
        info = _apply_gate(exchange, first)
    elif ex_id == 'mexc':
        info = _apply_mexc(exchange, first)
    elif ex_id in ('htx', 'huobi'):
        info = _apply_htx(exchange, first)
    elif ex_id in ('kucoin', 'kucoinfutures'):
        info = _apply_kucoin(exchange)
    elif ex_id == 'hyperliquid':
        info = _apply_hyperliquid(exchange, first)
    else:
        return
    exchange._bytx_broker_ids_applied = True
    _log_once(ex_id, info, exchange)


__all__ = [
    'apply_broker_ids', 'broker_id_sources', 'kucoin_partner_sign', 'okx_is_own_code', 'okx_broker_tag', 'okx_tag_from_options',
    'NEUTRAL_BINANCE_PREFIX', 'NEUTRAL_OKX_PREFIX',
    'CCXT_BINANCE_BROKER_IDS', 'CCXT_OKX_BROKER_ID',
    'ENV_BINANCE_SPOT', 'ENV_BINANCE_FUTURES', 'ENV_OKX', 'ENV_BYBIT', 'ENV_BINGX', 'ENV_BITGET', 'ENV_GATE',
    'ENV_MEXC', 'CCXT_BITGET_BROKER_ID', 'CCXT_GATE_CHANNEL_ID', 'BITGET_HEADER', 'GATE_HEADER',
    'CCXT_MEXC_BROKER_ID', 'MEXC_HEADER', 'CCXT_HTX_BROKER_ID',
]
