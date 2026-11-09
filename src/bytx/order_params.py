"""Stateless exchange-specific order parameters: client_id and position mode.

Rules: each exchange's client_id
format, the parameter key, generation with a broker prefix, hedge/one-way
and margin parameters.
"""
from __future__ import annotations

import re
import secrets
import uuid
from typing import Optional

from bytx.exchanges.binance.broker_cid import (
    BINANCE_CID_MAX_LEN,
    binance_cid_config_from_env,
    exchange_binance_cid_config,
    physical_binance_cid,
)
from bytx.net._sig.headers import (
    CCXT_BINANCE_BROKER_IDS,
    NEUTRAL_BINANCE_PREFIX,
    NEUTRAL_OKX_PREFIX,
    okx_is_own_code,
    okx_tag_from_options,
)
from bytx.errors import InvalidOrder

CLIENT_ID_FORMATS = {
    'binance': re.compile(r'^[.A-Z:/a-z0-9_-]{1,36}$'),
    'bybit': re.compile(r'^[A-Za-z0-9_-]{1,36}$'),
    'okx': re.compile(r'^[A-Za-z0-9]{1,32}$'),
    'kucoinfutures': re.compile(r'^[A-Za-z0-9_-]{1,40}$'),
    'kucoin': re.compile(r'^[A-Za-z0-9_-]{1,40}$'),
    # BingX lowercases clientOrderId itself, so uppercase is forbidden; otherwise
    # looking up the order by the original client_id would not find it
    'bingx': re.compile(r'^[^\sA-Z]{1,40}$'),
    'hyperliquid': re.compile(r'^0x[0-9a-fA-F]{32}$'),
    # Bitget: clientOid format is not specified in the Place Order docs; errors 40304/40305: "length cannot greater
    # than 50 / 64"; docs example: "BITGET#1627293504612". Conservative: ≤ 50, no spaces
    'bitget': re.compile(r'^[A-Za-z0-9_#.:-]{1,50}$'),
    # Gate: text: "1. prefixed with `t-` 2. no longer than 28 bytes without `t-` prefix 3. can only include
    # 0-9, A-Z, a-z, underscore(_), hyphen(-) or dot(.)" (Order / FuturesOrder models, APIv4)
    'gate': re.compile(r'^t-[0-9A-Za-z_.-]{1,28}$'),
    # MEXC: externalOid / newClientOrderId format is not specified in the docs; contract error 2030: "External order ID
    # is too long (Max. 32 bits)". Conservative: ≤ 32, letters/digits/_/-
    'mexc': re.compile(r'^[A-Za-z0-9_-]{1,32}$'),
    # HTX spot: client-order-id: "The allowed characters are letters (case sensitive), digit, underscore (_) and
    # hyphen (-), no more than 64 chars". Contracts use an integer (see MARKET_CLIENT_ID_FORMATS)
    'htx': re.compile(r'^[A-Za-z0-9_-]{1,64}$'),
}
# Format that differs by market type (key: ccxt id, market type). HTX USDT-M: client_order_id is a long
# "[1, 9223372036854775807]", error 1065 "The form number of client isn't an integer"
MARKET_CLIENT_ID_FORMATS = {
    ('htx', 'swap'): re.compile(r'^[1-9][0-9]{0,18}$'),
}
HTX_CLIENT_ID_MAX = 9223372036854775807
# generated numeric HTX id: 18 digits (< 10^18 < HTX_CLIENT_ID_MAX). With a bot tag: "9", tag length
# (1 digit), tag, random digits: an encoding without prefix collisions (tag "7" → 917…, "71" → 9271…);
# without a tag the first digit is 1–8, so such an id belongs to no tag
_NUMERIC_ID_LEN = 18
_NUMERIC_TAG = '9'
#: numeric id tag length: 18 − "9" − length digit − at least 8 random digits
NUMERIC_PREFIX_MAX = 8

CLIENT_ID_KEYS = {
    'binance': 'newClientOrderId',
    'bybit': 'orderLinkId',
    'okx': 'clOrdId',
    'kucoinfutures': 'clientOid',
    'kucoin': 'clientOid',
    'bingx': 'clientOrderID',
    'hyperliquid': 'clientOrderId',
    'bitget': 'clientOid',
    'gate': 'text',
    'mexc': 'clientOrderId',     # ccxt: spot → newClientOrderId, contracts → externalOid
    'htx': 'clientOrderId',      # ccxt: spot → client-order-id, contracts → client_order_id (integer)
}

AUTO_PREFIX = 'bt'

# Bot tag at the start of every client_id: letters/digits, up to 14 chars,
# which is what fits on Binance with your own broker ID (36 − up to 14 prefix − 8 random).
# The rest of the id is random hex (at least 8 chars); the id fits the exchange format.
CLIENT_ID_PREFIX_MAX = 14
_PREFIX_RE = re.compile(r'^[A-Za-z0-9]{1,%d}$' % CLIENT_ID_PREFIX_MAX)
# exchange's maximum client_id length (docs; HL: 128-bit hex without '0x')
_CLIENT_ID_MAX_LEN = {'binance': 36, 'bybit': 36, 'okx': 32, 'kucoinfutures': 40, 'kucoin': 40,
                      'bingx': 40, 'hyperliquid': 32, 'bitget': 50, 'gate': 30, 'mexc': 32, 'htx': 64}
_MARKET_CLIENT_ID_MAX_LEN = {('htx', 'swap'): 19}
# Gate: mandatory text prefix (not counted in the 28-byte length)
GATE_TEXT_PREFIX = 't-'


def numeric_client_id(exchange_id: str, market_type: str = 'swap') -> bool:
    """The exchange's client_id on this market type is an integer (HTX USDT-M)."""
    return (exchange_id, 'swap' if market_type != 'spot' else 'spot') in MARKET_CLIENT_ID_FORMATS


def client_id_format(exchange_id: str, market_type: str = 'swap') -> Optional[re.Pattern]:
    """Exchange client_id format for the market type (per docs) or None."""
    return MARKET_CLIENT_ID_FORMATS.get((exchange_id, 'swap' if market_type != 'spot' else 'spot'),
                                        CLIENT_ID_FORMATS.get(exchange_id))


def check_client_id_prefix(exchange_id: str, prefix: Optional[str],
                           market_type: Optional[str] = None) -> Optional[str]:
    """Validates the bot tag for client_id; an invalid one raises ConfigError (before the first order).

    Hyperliquid: hex only (cloid is a 128-bit hex number), a leading ``0x`` is stripped.
    BingX: no uppercase (the exchange lowercases clientOrderID).
    HTX USDT-M (``market_type`` swap): client_order_id is an integer, the tag is digits without a leading zero.
    """
    if prefix is None:
        return None
    from bytx.errors import ConfigError
    value = str(prefix)
    if exchange_id == 'hyperliquid' and value[:2].lower() == '0x':
        value = value[2:]
    if not _PREFIX_RE.fullmatch(value):
        raise ConfigError(f'client_id_prefix: letters and digits, 1–{CLIENT_ID_PREFIX_MAX} chars, not {prefix!r}',
                          exchange=exchange_id, details={'client_id_prefix': prefix})
    if exchange_id == 'hyperliquid' and not re.fullmatch(r'[0-9a-fA-F]+', value):
        raise ConfigError(f'client_id_prefix for Hyperliquid must be hex only, not {prefix!r}',
                          exchange=exchange_id, details={'client_id_prefix': prefix})
    if exchange_id == 'bingx' and value != value.lower():
        raise ConfigError(f'client_id_prefix for BingX must have no uppercase, not {prefix!r}',
                          exchange=exchange_id, details={'client_id_prefix': prefix})
    if market_type is not None and numeric_client_id(exchange_id, market_type) \
            and not re.fullmatch(rf'[1-9][0-9]{{0,{NUMERIC_PREFIX_MAX - 1}}}', value):
        raise ConfigError(f'client_id_prefix for {exchange_id} {market_type} must be 1–{NUMERIC_PREFIX_MAX} digits '
                          f'without a leading zero (client_order_id is an integer), not {prefix!r}', exchange=exchange_id,
                          details={'client_id_prefix': prefix})
    return value


def prefixed_client_id(exchange, prefix: str, market_type: str = 'swap') -> str:
    """client_id = bot tag + random hex, fitting the exchange's length and format.

    Binance with your own broker ID: the id is logical, the transport adds the broker prefix
    (36 − its length); without your own ID no neutral prefix is added, the bot tag comes first.
    """
    ex_id = exchange.id
    max_len = _CLIENT_ID_MAX_LEN.get(ex_id, 32)
    if ex_id == 'binance':
        cfg = exchange_binance_cid_config(exchange)
        broker = cfg.prefix_for('spot' if market_type == 'spot' else 'futures') if cfg is not None else None
        if broker:
            max_len -= len(broker)
    if ex_id == 'gate':
        return GATE_TEXT_PREFIX + (prefix + uuid.uuid4().hex)[:max_len - len(GATE_TEXT_PREFIX)]
    if numeric_client_id(ex_id, market_type):
        return _numeric_id(prefix)
    cid = (prefix + uuid.uuid4().hex + uuid.uuid4().hex)[:max_len]
    return '0x' + cid if ex_id == 'hyperliquid' else cid


def okx_tag_params(exchange) -> dict:
    """OKX: tag = your own broker code (ccxt does not set tag when clOrdId is supplied)."""
    if exchange.id != 'okx':
        return {}
    tag = okx_tag_from_options(getattr(exchange, 'options', None))
    return {'tag': tag} if tag else {}


def generate_client_id(exchange, market_type: str = 'swap', prefix: Optional[str] = None) -> str:
    """New client_id in the exchange format (with the broker prefix where it is part of the id).

    ``prefix``: bot tag (:func:`check_client_id_prefix`): the id starts with it, length is at most
    :func:`max_client_id_len` (accounting for the Binance broker prefix), the random part is
    at least 8 chars. ``exchange``: a ccxt instance or :class:`~bytx.Exchange`.
    """
    exchange = _ccxt_of(exchange) or exchange
    if prefix:
        return prefixed_client_id(exchange, prefix, market_type)
    if numeric_client_id(exchange.id, market_type):
        return _numeric_id('')
    token = uuid.uuid4().hex
    options = exchange.options if isinstance(getattr(exchange, 'options', None), dict) else {}
    ex_id = exchange.id
    if ex_id == 'binance':
        # Own ID: the id is logical (uuid22), the transport adds the prefix (exchanges.binance.broker_cid).
        cfg = exchange_binance_cid_config(exchange)
        if cfg is not None and cfg.prefix_for('spot' if market_type == 'spot' else 'futures'):
            return token[:22]
        broker: dict = options['broker'] if isinstance(options.get('broker'), dict) else {}
        prefix = broker.get('spot' if market_type == 'spot' else 'swap')
        if not prefix or prefix in CCXT_BINANCE_BROKER_IDS:
            prefix = NEUTRAL_BINANCE_PREFIX
        return (str(prefix) + token[:22])[:36]
    if ex_id == 'okx':
        broker_id = options.get('brokerId')
        if not okx_is_own_code(broker_id):
            broker_id = NEUTRAL_OKX_PREFIX
        return re.sub(r'[^0-9A-Za-z]', '', str(broker_id) + token[:16])[:32]
    if ex_id in ('kucoinfutures', 'kucoin'):
        return str(uuid.uuid4())
    if ex_id == 'hyperliquid':
        return '0x' + token
    if ex_id == 'gate':
        return GATE_TEXT_PREFIX + AUTO_PREFIX + token[:26]
    return AUTO_PREFIX + token[:30]


def _numeric_head(prefix: str) -> str:
    return f'{_NUMERIC_TAG}{len(prefix)}{prefix}'


def _numeric_id(prefix: str) -> str:
    """Integer client_id, 18 digits without a leading zero: with a tag, "9" + tag length + tag + random
    digits; without a tag, the first digit is 1–8 (not "9")."""
    head = _numeric_head(prefix) if prefix else str(1 + secrets.randbelow(8))
    tail = _NUMERIC_ID_LEN - len(head)
    return head + ''.join(str(secrets.randbelow(10)) for _ in range(tail))


def _ccxt_of(exchange):
    """ccxt instance from an Exchange / ccxt instance; a string (exchange name) gives None."""
    if isinstance(exchange, str):
        return None
    if hasattr(exchange, 'options') and hasattr(exchange, 'id'):
        return exchange
    return getattr(exchange, 'ccxt', None)


def _ccxt_id(exchange) -> str:
    if isinstance(exchange, str):
        from bytx.transport import EXCHANGES
        name = exchange.lower()
        return EXCHANGES.get(name, {}).get('swap', name)
    c = _ccxt_of(exchange)
    return str(getattr(c, 'id', '') or '')


def binance_broker_prefix(exchange, market_type: str = 'swap') -> Optional[str]:
    """Binance broker prefix (``x-<ID>``) that the transport adds to client_id, or None."""
    c = _ccxt_of(exchange)
    if c is None or getattr(c, 'id', None) != 'binance':
        return None
    cfg = exchange_binance_cid_config(c)
    if cfg is None:
        return None
    return cfg.prefix_for('spot' if market_type in ('spot', 'margin') else 'futures')


def max_client_id_len(exchange, market_type: str = 'swap') -> int:
    """Maximum length of a client_id that reaches the exchange intact.

    ``exchange``: :class:`~bytx.Exchange`, a ccxt instance or an exchange name (then no
    broker prefix). Length per exchange docs; Binance: 36 minus the length of your own
    ``x-<ID>`` broker prefix (added by the transport): a longer id is sent without
    attribution. Hyperliquid: the full cloid length (``0x`` + 32 hex, exactly 34).
    """
    ex_id = _ccxt_id(exchange)
    if ex_id == 'hyperliquid':
        return 34
    limit = _MARKET_CLIENT_ID_MAX_LEN.get((ex_id, 'swap' if market_type != 'spot' else 'spot'),
                                          _CLIENT_ID_MAX_LEN.get(ex_id, 32))
    prefix = binance_broker_prefix(exchange, market_type)
    return limit - len(prefix) if prefix else limit


def exchange_client_id(exchange, market_type: str, client_id: str) -> str:
    """The client_id as it goes to the exchange: Binance — with the broker prefix ``x-<ID>`` if it is set and the cid
    with it fits into 36 (otherwise as is, without attribution); other exchanges do not change the cid (their tag is
    a header / tag). ``exchange`` — :class:`~bytx.Exchange`, a ccxt instance or a name (then the ID from env)."""
    if _ccxt_id(exchange) != 'binance':
        return client_id
    c = _ccxt_of(exchange)
    cfg = exchange_binance_cid_config(c) if c is not None else binance_cid_config_from_env()
    if cfg is None:
        return client_id
    return physical_binance_cid(client_id, cfg.prefix_for('spot' if market_type in ('spot', 'margin') else 'futures'),
                                cfg)


def attribution_overflow(exchange, client_id: Optional[str], market_type: str = 'swap') -> bool:
    """True if a client_id with the Binance broker prefix does not fit in 36 chars.

    The transport sends such an id without the prefix: the order is accepted, but without broker
    attribution (``binance_unattributed_counts`` counter). An id that already starts with
    a known prefix is sent as is; that is not an overflow.
    """
    prefix = binance_broker_prefix(exchange, market_type)
    if not prefix or not client_id:
        return False
    cfg = exchange_binance_cid_config(_ccxt_of(exchange))
    known = cfg.known if cfg is not None else (prefix,)
    if any(client_id.lower().startswith(p.lower()) for p in known):
        return False
    return len(prefix) + len(client_id) > BINANCE_CID_MAX_LEN


def check_client_id(exchange, client_id: Optional[str], market_type: str = 'swap', *,
                    strict: bool = False) -> None:
    """Validates the caller's client_id before sending: the id is never replaced.

    Invalid exchange format raises :class:`~bytx.errors.InvalidOrder`. ``strict``: same on
    length overflow with the Binance broker prefix; otherwise such an id
    is sent without attribution.
    """
    if not client_id:
        return
    ex_id = _ccxt_id(exchange)
    pattern = client_id_format(ex_id, market_type)
    if pattern is not None and not pattern.fullmatch(str(client_id)):
        raise InvalidOrder(f'client_id {client_id!r} does not match the {ex_id} format: {pattern.pattern}',
                           exchange=ex_id, details={'client_id': client_id})
    if numeric_client_id(ex_id, market_type) and int(client_id) > HTX_CLIENT_ID_MAX:
        raise InvalidOrder(f'client_id {client_id!r} exceeds {HTX_CLIENT_ID_MAX} ({ex_id} {market_type})',
                           exchange=ex_id, details={'client_id': client_id})
    if strict and attribution_overflow(exchange, client_id, market_type):
        prefix = binance_broker_prefix(exchange, market_type) or ''
        raise InvalidOrder(
            f'client_id {client_id!r} ({len(client_id)} chars) with broker prefix {prefix} is longer than '
            f'{BINANCE_CID_MAX_LEN}: shorten to {BINANCE_CID_MAX_LEN - len(prefix)} (max_client_id_len)',
            exchange=ex_id, details={'client_id': client_id, 'max_len': BINANCE_CID_MAX_LEN - len(prefix)})


def client_id_params(exchange, client_id: Optional[str], *, market_type: str = 'swap',
                     strict: bool = False) -> dict:
    """params for client_id; an invalid format (and with ``strict``, overflow with the Binance broker
    prefix) raises InvalidOrder before sending. The caller's id is not changed."""
    if not client_id:
        return {}
    ex_id = exchange.id
    key = CLIENT_ID_KEYS.get(ex_id)
    if key is None:
        return {'clientOrderId': client_id}
    check_client_id(exchange, client_id, market_type, strict=strict)
    return {key: client_id, **okx_tag_params(exchange)}


#: how many trailing client_id chars :func:`amend_client_id` replaces (random tail)
AMEND_TAIL = 6
_LOWER36 = '0123456789abcdefghijklmnopqrstuvwxyz'


def amend_client_id(exchange, client_id: str, market_type: str = 'swap',
                    prefix: Optional[str] = None) -> Optional[str]:
    """client_id of the new order when an exchange amend is cancel + new order (Binance spot cancelReplace,
    BingX cancelReplace, Bitget modify-order, Hyperliquid modify).

    The same logical id with a new random tail (last :data:`AMEND_TAIL` chars): bot tag,
    role symbol, level and length are preserved, so ``owns`` / ``cancel_all`` by
    tag and the journal's role parsing see the new order as ours, and the transport adds the Binance broker
    prefix the same way as to the original. Reusing the same id is avoided: Binance New Order:
    "A unique id among open orders … Orders with the same newClientOrderID can be accepted only when
    the previous one is filled", BingX: "different orders cannot use the same clientOrderID"; besides,
    lookup by client_id would become ambiguous (canceled and new order with the same id).

    Short id (the tail would eat the ``prefix`` tag): the tail is appended if it fits in
    :func:`max_client_id_len`; otherwise, and for integer ids (HTX USDT-M), None.
    """
    ex_id = _ccxt_id(exchange)
    cid = str(client_id)
    if not cid or numeric_client_id(ex_id, market_type):
        return None
    if ex_id == 'hyperliquid':
        body = cid[2:] if cid[:2].lower() == '0x' else cid
        if not re.fullmatch(r'[0-9a-fA-F]{32}', body):
            return None
        alphabet, head = '0123456789abcdef', '0x' + body[:-AMEND_TAIL - 2]
        tail_len = AMEND_TAIL + 2   # 32 bits of randomness, the tag (≤ 14 hex) is untouched
    else:
        lead = GATE_TEXT_PREFIX if ex_id == 'gate' and cid.startswith(GATE_TEXT_PREFIX) else ''
        body = cid[len(lead):]
        own = bool(prefix) and body.lower().startswith(str(prefix).lower())
        keep_min = len(str(prefix)) + 3 if own else 1   # tag + role + level (2 chars): untouched
        alphabet, tail_len = _LOWER36, AMEND_TAIL
        if len(body) - AMEND_TAIL >= keep_min:
            head = lead + body[:-AMEND_TAIL]
        elif len(cid) + AMEND_TAIL <= max_client_id_len(exchange, market_type):
            head = cid
        else:
            return None
    pattern = client_id_format(ex_id, market_type)
    for _ in range(8):
        new = head + ''.join(secrets.choice(alphabet) for _ in range(tail_len))
        if new != cid and (pattern is None or pattern.fullmatch(new)):
            return new
    return None


def owns(order, prefix: Optional[str], exchange: Optional[str] = None) -> bool:
    """Whether the order is ours: client_id starts with the bot tag ``prefix``.

    ``order`` — :class:`~bytx.types.Order` / :class:`~bytx.protection.TpSlOrder`,
    ccxt dict (``clientOrderId``) or the client_id string itself. SDK client_ids are logical:
    the Binance broker prefix is already stripped by the transport. Hyperliquid: ``0x`` is stripped and
    comparison is case-insensitive (cloid is a hex number, the exchange may return it in a different
    case); BingX is case-insensitive too (the exchange stores clientOrderID in lowercase).
    ``exchange``: exchange name or ccxt id; without it HL is detected by a leading ``0x`` in the id.
    HTX: an integer id (USDT-M) is ours only if it encodes exactly this tag (``9`` + length + tag).
    Empty tag or id gives False.
    """
    if isinstance(order, str) or order is None:
        cid = order
    elif isinstance(order, dict):
        cid = order.get('clientOrderId') or order.get('client_id')
    else:
        cid = getattr(order, 'client_id', None)
    if not cid or not prefix:
        return False
    cid, prefix = str(cid), str(prefix)
    ex_id = (exchange or '').lower()
    if ex_id == 'hyperliquid' or (not ex_id and cid[:2].lower() == '0x'):
        if cid[:2].lower() != '0x':
            return False
        body = prefix[2:] if prefix[:2].lower() == '0x' else prefix
        return bool(body) and cid[2:].lower().startswith(body.lower())
    if ex_id == 'bingx':
        return cid.lower().startswith(prefix.lower())
    if ex_id == 'htx' and cid.isdigit() and prefix.isdigit():
        # USDT-M: integer id is "9", tag length, tag (_numeric_id); by bare prefix, tag "7" would own
        # ~1/9 of foreign ids and ids with tag "71"
        return len(cid) == _NUMERIC_ID_LEN and cid.startswith(_numeric_head(prefix))
    if ex_id == 'gate' or (not ex_id and cid.startswith(GATE_TEXT_PREFIX)):
        cid = cid.removeprefix(GATE_TEXT_PREFIX)    # Gate: text "t-<own id>"
        prefix = prefix.removeprefix(GATE_TEXT_PREFIX)
    return cid.startswith(prefix)


# The client id key on CANCEL differs from creation on some exchanges (Binance:
# newClientOrderId → origClientOrderId). ccxt reads these keys from params.
CANCEL_CLIENT_ID_KEYS = {
    'binance': 'origClientOrderId',
    'bybit': 'orderLinkId',
    'okx': 'clOrdId',
    'kucoinfutures': 'clientOid',
    'kucoin': 'clientOid',
    'bitget': 'clientOid',
}


def cancel_client_id_params(exchange, client_id: str) -> dict:
    """Cancel params by client_id: only the exchange's documented key."""
    return {CANCEL_CLIENT_ID_KEYS.get(exchange.id, 'clientOrderId'): client_id}


def position_params(exchange, *, side: str, hedge: bool, reduce_only: bool,
                    margin_mode: Optional[str] = None, leverage: Optional[float] = None) -> dict:
    """Position mode and margin parameters for create_order.

    side: order side (buy/sell). In hedge mode a closing order belongs to
    the opposite position: sell closes long.
    """
    ex_id = exchange.id
    if hedge:
        opens_long = (side == 'buy') != reduce_only
        pos = 'long' if opens_long else 'short'
    params: dict = {}
    if ex_id == 'okx':
        if margin_mode:
            params['tdMode'] = margin_mode
        params['posSide'] = pos if hedge else 'net'
    elif ex_id == 'bybit':
        params['positionIdx'] = (1 if pos == 'long' else 2) if hedge else 0
    elif ex_id in ('binance', 'bingx'):
        params['positionSide'] = pos.upper() if hedge else 'BOTH'
        if ex_id == 'binance':
            params['hedged'] = hedge
    elif ex_id == 'bitget':
        # Place Order: marginMode is mandatory (ccxt: cross → crossed); hedge uses tradeSide open/close,
        # a hedge close uses reduceOnly (ccxt maps it to the position side + tradeSide close)
        if margin_mode:
            params['marginMode'] = margin_mode
        params['hedged'] = hedge
    elif ex_id in ('mexc', 'htx'):
        # MEXC Place Order: openType (1 isolated, 2 cross) is mandatory, positionMode, leverage on open;
        # HTX: margin mode selects the endpoint (swap_cross_order / swap_order), lever_rate on every order
        if margin_mode:
            params['marginMode'] = margin_mode
        params['hedged'] = hedge
        if leverage:
            params['leverage'] = int(leverage)
    elif ex_id == 'kucoinfutures':
        if margin_mode:
            params['marginMode'] = margin_mode.upper()
        if leverage:
            # without leverage ccxt substitutes 1x: KuCoin leverage is set per order
            params['leverage'] = leverage
    if reduce_only and not (hedge and ex_id in ('binance', 'bingx', 'okx')):
        # Binance/BingX reject reduceOnly in hedge mode (-1106): positionSide defines the close.
        # OKX: reduceOnly is "only applicable to … FUTURES/SWAP orders in net mode" (v5 docs),
        # in long/short mode posSide defines the close (ccxt does not send it either).
        params['reduceOnly'] = True
    return params


__all__ = ['GATE_TEXT_PREFIX', 'HTX_CLIENT_ID_MAX', 'NUMERIC_PREFIX_MAX', 'MARKET_CLIENT_ID_FORMATS', 'client_id_format',
           'numeric_client_id', 'generate_client_id', 'check_client_id_prefix', 'prefixed_client_id', 'CLIENT_ID_PREFIX_MAX', 'client_id_params', 'position_params', 'okx_tag_params',
           'CLIENT_ID_FORMATS', 'CLIENT_ID_KEYS', 'max_client_id_len', 'check_client_id', 'attribution_overflow',
           'binance_broker_prefix', 'exchange_client_id', 'owns']
