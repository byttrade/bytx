"""Unified SDK error hierarchy.

All :class:`bytx.Exchange` methods raise only subclasses of :class:`BytxError`.
The original ccxt exception is available via ``__cause__``. The ``retryable`` flag tells
whether the request can be safely retried (orders are retried only with their own
``client_id``, otherwise a duplicate is possible).

Machine-readable attributes on every error:

- ``code`` — exchange code as a string (``'-2019'``, ``'110072'``), if known;
- ``error_code`` — unified :class:`ErrorCode`;
- ``is_business`` — a substantive rejection (funds, params, symbol, key, no position),
  not an infrastructure failure: a circuit breaker must not count it as a service failure.

ccxt compatibility: error classes are also subclasses
of the matching ccxt classes (``errors.InsufficientFunds`` ⊂ ``ccxt.InsufficientFunds``),
so existing ``except ccxt.InsufficientFunds`` handlers still fire. The "unknown" outcome
(:class:`OrderStatusUnknown`), :class:`BatchError`, :class:`ProtectionNotPlaced` and
:class:`ConfigError` are NOT ccxt subclasses: a broad ``except ccxt.ExchangeError``
or ``except ccxt.NetworkError`` must not swallow them or treat them as a retryable failure.
"""
from __future__ import annotations

import asyncio
import re
from enum import Enum
from typing import Any, Iterable, Optional

import ccxt


class ErrorCode(Enum):
    """Unified error codes across all exchanges.

    Category by thousands: 1xxx key and permissions, 2xxx order, 3xxx position, 4xxx leverage and margin,
    5xxx rate limit, 6xxx market, 7xxx network and exchange, 8xxx params, 9xxx broker and unknown.
    """

    # Authentication & Permission Errors (1xxx)
    INVALID_API_KEY = 1001
    INVALID_SIGNATURE = 1002
    PERMISSION_DENIED = 1003
    API_KEY_EXPIRED = 1004
    IP_NOT_WHITELISTED = 1005
    INSUFFICIENT_PERMISSIONS = 1006

    # Order Errors (2xxx)
    INSUFFICIENT_BALANCE = 2001
    ORDER_NOT_FOUND = 2002
    ORDER_ALREADY_CLOSED = 2003
    INVALID_ORDER_TYPE = 2004
    INVALID_ORDER_SIDE = 2005
    INVALID_ORDER_AMOUNT = 2006
    INVALID_ORDER_PRICE = 2007
    ORDER_WOULD_TRIGGER_IMMEDIATELY = 2008
    DUPLICATE_ORDER = 2009
    ORDER_AMOUNT_TOO_SMALL = 2010
    ORDER_AMOUNT_TOO_LARGE = 2011
    ORDER_PRICE_TOO_HIGH = 2012
    ORDER_PRICE_TOO_LOW = 2013

    # Position Errors (3xxx)
    POSITION_NOT_FOUND = 3001
    POSITION_MODE_INVALID = 3002
    CANNOT_CHANGE_POSITION_MODE = 3003
    MARGIN_MODE_INVALID = 3004
    CANNOT_CHANGE_MARGIN_MODE = 3005
    POSITION_SIDE_NOT_MATCH = 3006
    REDUCE_ONLY_REJECTED = 3007
    POSITION_COVERED = 3008

    # Leverage & Margin Errors (4xxx)
    LEVERAGE_TOO_HIGH = 4001
    LEVERAGE_TOO_LOW = 4002
    INSUFFICIENT_MARGIN = 4003
    MARGIN_CALL = 4004
    LIQUIDATION_RISK = 4005

    # Rate Limit Errors (5xxx)
    RATE_LIMIT_EXCEEDED = 5001
    TOO_MANY_REQUESTS = 5002
    IP_BANNED = 5003

    # Market Errors (6xxx)
    MARKET_CLOSED = 6001
    SYMBOL_NOT_FOUND = 6002
    SYMBOL_NOT_TRADING = 6003
    MARKET_ORDER_NOT_SUPPORTED = 6004

    # Network & System Errors (7xxx)
    NETWORK_ERROR = 7001
    TIMEOUT = 7002
    EXCHANGE_MAINTENANCE = 7003
    EXCHANGE_ERROR = 7004
    INTERNAL_ERROR = 7005

    # Validation Errors (8xxx)
    INVALID_PARAMETER = 8001
    MISSING_PARAMETER = 8002
    INVALID_SYMBOL_FORMAT = 8003
    INVALID_TIMESTAMP = 8004

    # ─── 9xxx — Broker-specific errors (9001-9099) ───
    BROKER_INVALID_PARTNER          = 9001
    BROKER_INVALID_PARTNER_SIGN     = 9002
    BROKER_NOT_AUTHORIZED           = 9003
    BROKER_CAPABILITY_NOT_SUPPORTED = 9004
    BROKER_SUB_ACCOUNT_NOT_FOUND    = 9005
    BROKER_SUB_ACCOUNT_LIMIT        = 9006
    BROKER_KYC_REQUIRED             = 9007
    BROKER_KYC_PENDING              = 9008
    BROKER_KYC_REJECTED             = 9009
    BROKER_TRANSFER_INSUFFICIENT    = 9010
    BROKER_RATE_LIMIT               = 9011
    BROKER_PARTNER_DISABLED         = 9012

    # Unknown (9xxx)
    UNKNOWN_ERROR = 9999

    @property
    def is_business(self) -> bool:
        """Substantive rejection (key, order, position, leverage, market, params, broker)?

        Rate limit (5xxx), network and exchange failure (7xxx) and UNKNOWN_ERROR — no.
        Broker BROKER_RATE_LIMIT and BROKER_KYC_PENDING (temporary) — no either.
        """
        if self in (ErrorCode.UNKNOWN_ERROR, ErrorCode.BROKER_RATE_LIMIT, ErrorCode.BROKER_KYC_PENDING):
            return False
        return self.value // 1000 not in (5, 7)


class BytxError(Exception):
    """Base of all SDK errors."""

    retryable: bool = False
    #: unified error code (the class sets the default)
    error_code: ErrorCode = ErrorCode.UNKNOWN_ERROR

    def __init__(self, message: str = '', *, exchange: Optional[str] = None,
                 code: Optional[str] = None, details: Any = None) -> None:
        super().__init__(message)
        self.message = message
        self.exchange = exchange
        self.code = code
        self.details = details

    @property
    def is_business(self) -> bool:
        """Business rejection (not an infrastructure failure) — by :attr:`error_code`."""
        return self.error_code.is_business

    def __str__(self) -> str:
        prefix = f'[{self.exchange}] ' if self.exchange else ''
        return f'{prefix}{self.message}'


class ConfigError(BytxError):
    """Invalid SDK parameters (exchange, keys, proxy, mode)."""

    error_code = ErrorCode.INVALID_PARAMETER


class NotSupported(BytxError, ccxt.NotSupported):
    """The exchange or mode does not support the operation."""

    error_code = ErrorCode.INVALID_PARAMETER


class BadSymbol(BytxError, ccxt.BadSymbol):
    """Symbol not found on the exchange."""

    error_code = ErrorCode.SYMBOL_NOT_FOUND


class AuthError(BytxError, ccxt.AuthenticationError):
    """Invalid key, signature or passphrase."""

    error_code = ErrorCode.INVALID_API_KEY


class PermissionDenied(AuthError, ccxt.PermissionDenied):
    """The key lacks permission for the operation (or the IP is not whitelisted)."""

    error_code = ErrorCode.PERMISSION_DENIED


class UnsafeApiKey(PermissionDenied):
    """Key rejected by SDK policy (:meth:`Exchange.check_key`): e.g. the key has withdrawal
    permission. ``details['key']`` — :class:`~bytx.key_check.KeyInfo`."""

    error_code = ErrorCode.PERMISSION_DENIED


class AccountSuspended(AuthError, ccxt.AccountSuspended):
    """Account blocked by the exchange."""

    error_code = ErrorCode.PERMISSION_DENIED


class InsufficientFunds(BytxError, ccxt.InsufficientFunds):
    """Insufficient funds or margin."""

    error_code = ErrorCode.INSUFFICIENT_BALANCE


class InvalidOrder(BytxError, ccxt.InvalidOrder):
    """Order rejected: params, precision, limits, post-only crossed the book, etc."""

    error_code = ErrorCode.INVALID_PARAMETER


class OrderTooSmall(InvalidOrder):
    """Below the minimum amount or notional (checked before sending)."""

    error_code = ErrorCode.ORDER_AMOUNT_TOO_SMALL


class MinSizeExceedsRisk(OrderTooSmall):
    """``open_position`` / ``risk_size``: the exchange minimum amount (``min amount``,
    ``min_cost``, step) at this stop risks more than ``risk_usdt`` — the order is not sent.

    ``details``: ``min_contracts``, ``min_risk`` (risk of the minimum amount), ``risk_usdt``,
    ``risk_per_contract``, ``symbol``, ``side``.
    """


class OrderNotFound(BytxError, ccxt.OrderNotFound):
    """Order not found (already filled, cancelled or not ours)."""

    error_code = ErrorCode.ORDER_NOT_FOUND


class DuplicateOrderId(InvalidOrder, ccxt.DuplicateOrderId):
    """client_id already used.

    On placement this is not a rejection: an order with this client_id is most likely already live
    (the response to the first send was lost) — ``place_order`` reconciles it.
    """

    error_code = ErrorCode.DUPLICATE_ORDER


class PositionAlreadyClosed(InvalidOrder):
    """Reduce-only order: no position in this direction (closed earlier — a normal race).

    Do not retry: the order was not placed and there is nothing to close. Bybit 110017, OKX 51169,
    BingX 101205/109420, KuCoin 300009 — or there was no position even before sending. Reduce-only whose side is
    fully covered by resting closing orders: Bybit 110017, Binance -2022, BingX 101212.
    """

    error_code = ErrorCode.POSITION_NOT_FOUND


class PositionCovered(InvalidOrder):
    """Reduce-only rejected with a "no position" code while the side's position is open: its size is covered by
    resting closing orders (Binance -2022, Bybit 110017, BingX 101212, HTX 1048). The position is NOT closed:
    cancel the closing orders (or wait for them) and retry.

    ``details``: ``symbol``, ``side`` — the position side, ``contracts`` — the open size, ``code`` — exchange code;
    ``__cause__`` — the original rejection (:class:`PositionAlreadyClosed`).
    """

    error_code = ErrorCode.POSITION_COVERED


class ReduceExceedsPosition(InvalidOrder):
    """Reduce-only order amount exceeds the open position (checked before sending; BingX 101290).

    ``details``: ``requested`` and ``position`` — contracts, ``symbol``, ``side``.
    """

    error_code = ErrorCode.ORDER_AMOUNT_TOO_LARGE


class PositionOpenError(BytxError):
    """Leverage/margin mode change blocked by an open position. ``symbol`` — the position symbol."""

    error_code = ErrorCode.CANNOT_CHANGE_MARGIN_MODE

    def __init__(self, message: str = '', *, symbol: Optional[str] = None, **kwargs: Any) -> None:
        super().__init__(message or f'position open on {symbol}', **kwargs)
        self.symbol = symbol


class NetworkError(BytxError, ccxt.NetworkError):
    """Network, timeout, connection drop."""

    retryable = True
    error_code = ErrorCode.NETWORK_ERROR


class RequestTimeout(NetworkError, ccxt.RequestTimeout):
    """No response in time (reads; for orders see :class:`OrderStatusUnknown`)."""

    error_code = ErrorCode.TIMEOUT


class RateLimited(NetworkError, ccxt.RateLimitExceeded):
    """Exchange rate limit exceeded. ``retry_after`` — how long to wait, s (if the exchange said)."""

    error_code = ErrorCode.RATE_LIMIT_EXCEEDED

    def __init__(self, *args: Any, retry_after: Optional[float] = None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.retry_after = retry_after


class ThrottleQueueFull(RateLimited):
    """The SDK limiter queue is full (``maxCapacity`` waiters): the request was not sent. The exchange limit was not
    exceeded — it would have been, had everything been sent at once. ``retry_after`` — how long until the queue
    drains, s; ``details`` — ``key`` (limiter key) and ``waiting``. Not retried automatically (a retry would only
    lengthen the queue)."""


class ExchangeUnavailable(NetworkError, ccxt.ExchangeNotAvailable):
    """Exchange unavailable or under maintenance."""

    error_code = ErrorCode.EXCHANGE_MAINTENANCE


class TimestampError(NetworkError, ccxt.InvalidNonce):
    """Clock skew with the exchange (retry after time sync)."""

    error_code = ErrorCode.INVALID_TIMESTAMP


class UnknownOutcome(NetworkError):
    """The exchange responded, but the request outcome is unknown (ccxt ``OperationFailed`` /
    ``BadResponse`` / ``NullResponse``; Binance -1006 «Execution status unknown»).

    Reads are retried; an order may have been accepted — ``place_order`` reconciles it by ``client_id``.
    """


class OrderStatusUnknown(BytxError):
    """Order placement outcome is unknown; reconciliation by ``client_id`` did not find it or failed.

    The order MAY be live on the exchange. Do not blindly retry placement (``retryable=False``):
    check the position and find the order by ``client_order_id`` (:meth:`Exchange.find_order`),
    see README "Unknown order outcome". Not a subclass of :class:`NetworkError` and not a ccxt
    subclass: ``except NetworkError: retry`` will not catch it.

    Fields: ``client_order_id``, ``exchange_order_id``, ``symbol``, ``side``, ``amount``.
    """

    error_code = ErrorCode.NETWORK_ERROR

    def __init__(self, message: str = '', *, exchange: Optional[str] = None, code: Optional[str] = None,
                 details: Any = None, client_order_id: Optional[str] = None,
                 exchange_order_id: Optional[str] = None, symbol: Optional[str] = None,
                 side: Optional[str] = None, amount: Optional[float] = None) -> None:
        super().__init__(message, exchange=exchange, code=code, details=details)
        self.client_order_id = client_order_id
        self.exchange_order_id = exchange_order_id
        self.symbol = symbol
        self.side = side
        self.amount = amount


class ExchangeError(BytxError, ccxt.ExchangeError):
    """Other exchange errors."""

    error_code = ErrorCode.EXCHANGE_ERROR


class BatchError(BytxError):
    """Batch operation partially completed: some items succeeded, some did not.

    What was and was not done is in ``details`` (format documented per method: ``place_orders``,
    ``cancel_orders``, ``cancel_all``, ``close_all``, ``kill_switch``). The original item errors
    are there too; ``__cause__`` is the first of them.
    """


class ProtectionNotPlaced(BytxError):
    """TP/SL placed only partially. The main order (if any) was placed.

    ``details``: ``order`` — the placed main order (None for ``set_tp_sl``),
    ``placed`` — already placed :class:`~bytx.protection.TpSlOrder`,
    ``client_ids`` — client_id of every TP/SL sent (for reconciliation after a timeout).
    Original error — ``__cause__``.
    """


# Order matters: ccxt subclasses before base classes.
_CCXT_MAP: tuple[tuple[type[BaseException], type[BytxError]], ...] = (
    (ccxt.InvalidNonce, TimestampError),
    (ccxt.RateLimitExceeded, RateLimited),
    (ccxt.DDoSProtection, RateLimited),
    (ccxt.RequestTimeout, RequestTimeout),
    (ccxt.OnMaintenance, ExchangeUnavailable),
    (ccxt.ExchangeNotAvailable, ExchangeUnavailable),
    (ccxt.NetworkError, NetworkError),
    # OperationFailed is the base of NetworkError and BadResponse/NullResponse/CancelPending: outcome unknown
    (ccxt.OperationFailed, UnknownOutcome),
    (ccxt.AccountSuspended, AccountSuspended),
    (ccxt.PermissionDenied, PermissionDenied),
    (ccxt.AuthenticationError, AuthError),
    (ccxt.InsufficientFunds, InsufficientFunds),
    (ccxt.OrderNotFound, OrderNotFound),
    (ccxt.DuplicateOrderId, DuplicateOrderId),
    (ccxt.InvalidOrder, InvalidOrder),
    (ccxt.BadSymbol, BadSymbol),
    (ccxt.NotSupported, NotSupported),
    (ccxt.ExchangeError, ExchangeError),
)


# Error code in the response body that ccxt puts into the exception text ("<exchange> {json}"):
# Binance/BingX/KuCoin/OKX ``code``, Bybit ``retCode``, OKX data[] item ``sCode``
# (quotes may be escaped; HTX USDT-M — err_code). Success — 0 / 200 / 200000 (KuCoin) — is not an error code.
_CODE_FIELD_RE = re.compile(r'\\?"(sCode|code|retCode|ret_code|err_code)\\?"\s*:\s*\\?"?(-?\d+)(?![\d.])')
_SUCCESS_CODES = frozenset({'0', '200', '200000'})
# Gate APIv4: error is {"label": "ORDER_NOT_FOUND", "message": …}; error code = label
_LABEL_FIELD_RE = re.compile(r'\\?"label\\?"\s*:\s*\\?"([A-Z][A-Z0-9_]{2,})\\?"')


def exchange_codes(text: str) -> list[str]:
    """Exchange error codes from the ccxt exception text, ``sCode`` first; success codes excluded."""
    found = _CODE_FIELD_RE.findall(text or '')
    found.sort(key=lambda kv: kv[0] != 'sCode')
    out: list[str] = []
    for _, value in found:
        if value not in _SUCCESS_CODES and value not in out:
            out.append(value)
    if not out and 'label' in (text or ''):
        out.extend(v for v in dict.fromkeys(_LABEL_FIELD_RE.findall(text)))
    return out


# ─── secrets in error text ────────────────────────────────────────────────
# ccxt puts the request URL into network error text; for HTX (and Binance/MEXC spot) the query string has
# AccessKeyId / signature (HTX docs "Signature Authentication"). Secret params are masked everywhere,
# a URL query string containing a secret is removed entirely (the path stays for diagnostics).
SECRET_PARAM_RE = re.compile(
    r'(?i)\b(signature|sign|access[_-]?key[_-]?id|access[_-]?key|api[_-]?key|apikey|secret|passphrase|'
    r'password|token|x-mbx-apikey|private[_-]?key)(["\']?\s*[=:]\s*["\']?)([^&\s"\',}]+)')
_URL_QUERY_RE = re.compile(r'(https?://[^\s?#"\'<>]+)\?([^\s#"\'<>]*)')
#: shorter exact values are not masked (otherwise '***' would replace ordinary words and numbers)
MIN_SECRET_LEN = 6
_SECRET_ATTRS = ('apiKey', 'secret', 'password', 'privateKey', 'token')


def _strip_query(m: 're.Match[str]') -> str:
    return f'{m.group(1)}?***' if SECRET_PARAM_RE.search(m.group(2)) else m.group(0)


def scrub(text: str, secrets: Iterable[Optional[str]] = ()) -> str:
    """Removes keys from text: exact ``secrets`` values, URL query strings with a signature/key
    and secret params (``AccessKeyId=``, ``signature=``, ``apiKey:`` …)."""
    for s in sorted({str(x) for x in secrets if x and len(str(x)) >= MIN_SECRET_LEN}, key=len, reverse=True):
        text = text.replace(s, '***')
    if '://' in text:
        text = _URL_QUERY_RE.sub(_strip_query, text)
    return SECRET_PARAM_RE.sub(r'\1\2***', text)


def ccxt_secrets(raw: Any) -> list[str]:
    """Secrets of the ccxt instance (apiKey, secret, password, privateKey, token) — for :func:`scrub`."""
    return [v for v in (getattr(raw, k, None) for k in _SECRET_ATTRS) if isinstance(v, str) and v]


def from_ccxt(error: BaseException, exchange: Optional[str] = None, *,
              secrets: Iterable[Optional[str]] = ()) -> BytxError:
    """Converts a ccxt exception (or any) into an SDK error; ``code`` — exchange code from the response.

    The text is scrubbed of keys (:func:`scrub`; ``secrets`` — exact key values of the instance),
    including the original exception in ``__cause__`` (traceback in logs)."""
    if isinstance(error, BytxError):
        return error
    text = str(error)
    clean = scrub(text, secrets)
    if clean != text and len(error.args) == 1 and isinstance(error.args[0], str):
        error.args = (clean,)
    result: BytxError
    if isinstance(error, asyncio.TimeoutError):
        result = RequestTimeout(f'timeout: {clean}' if clean else 'timeout', exchange=exchange)
    else:
        for ccxt_cls, sdk_cls in _CCXT_MAP:
            if isinstance(error, ccxt_cls):
                result = sdk_cls(clean, exchange=exchange)
                break
        else:
            result = BytxError(f'{type(error).__name__}: {clean}', exchange=exchange)
    codes = exchange_codes(clean)
    if codes:
        result.code = codes[0]
    result.__cause__ = error
    if codes:
        # codes ccxt did not parse (BingX 101205 "no position" → bare ExchangeError …)
        from bytx.outcome import refine
        result = refine(_EXCHANGE_IDS.get(exchange or '', exchange), result)
    return result


# SDK exchange name → ccxt id (code tables bytx.exchanges.<exchange>.error_codes)
_EXCHANGE_IDS = {'kucoin': 'kucoinfutures'}


__all__ = [
    'BytxError', 'ConfigError', 'NotSupported', 'BadSymbol', 'AuthError', 'PermissionDenied', 'UnsafeApiKey',
    'AccountSuspended', 'InsufficientFunds', 'InvalidOrder', 'OrderTooSmall', 'MinSizeExceedsRisk',
    'OrderNotFound',
    'DuplicateOrderId', 'NetworkError', 'RequestTimeout', 'RateLimited', 'ThrottleQueueFull', 'ExchangeUnavailable',
    'TimestampError', 'UnknownOutcome', 'ExchangeError', 'BatchError', 'ProtectionNotPlaced',
    'OrderStatusUnknown', 'PositionAlreadyClosed', 'PositionCovered', 'ReduceExceedsPosition', 'PositionOpenError', 'ErrorCode', 'exchange_codes', 'from_ccxt',
    'scrub', 'ccxt_secrets', 'SECRET_PARAM_RE',
]
