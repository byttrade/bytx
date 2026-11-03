"""Classification of a failed order placement's outcome.

Exchange codes come from the ``bytx.exchanges.<exchange>.error_codes`` tables (checked against the docs):

- ``'rejected'`` — an explicit reject: no order, nothing to reconcile (funds, parameters, symbol,
    key, rate limit, clock; no position — :class:`~bytx.errors.PositionAlreadyClosed`);
- ``'duplicate'`` — the exchange rejected a duplicate client_id: an order with it most likely already exists;
- ``'unknown'`` — the outcome is unknown (timeout, disconnect, "status unknown" / "server timeout" codes,
    an empty or unintelligible response): the order may have been accepted;
- ``'exchange_error'`` — any other exchange error without a known code: reconcile; if ≥ 2 successful
    scans did not find the order, it is a reject (heuristic).

After ``duplicate`` / ``unknown`` an empty reconciliation does not prove the order is absent (the engine may
have accepted it after the window) — the caller raises :class:`~bytx.errors.OrderStatusUnknown`.
"""
from __future__ import annotations

import importlib
import re
from functools import lru_cache
from types import ModuleType
from typing import Literal, Optional

from bytx.errors import (
    BytxError,
    ConfigError,
    DuplicateOrderId,
    ExchangeError,
    InsufficientFunds,
    InvalidOrder,
    NetworkError,
    NotSupported,
    OrderNotFound,
    PermissionDenied,
    PositionAlreadyClosed,
    RateLimited,
    ReduceExceedsPosition,
    TimestampError,
    exchange_codes,
)

Outcome = Literal['rejected', 'duplicate', 'unknown', 'exchange_error']

# ccxt id → exchange package (as in bytx.exchanges._MODULES)
_PACKAGES = {
    'binance': 'binance', 'bybit': 'bybit', 'okx': 'okx', 'bingx': 'bingx',
    'kucoinfutures': 'kucoin', 'kucoin': 'kucoin', 'hyperliquid': 'hyperliquid',
    'bitget': 'bitget', 'gate': 'gate', 'mexc': 'mexc', 'htx': 'htx',
}
# Text markers of an unknown outcome: Binance "Send status
# unknown; execution status unknown" — overrides ccxt's "reject" class
_STATUS_UNKNOWN_RE = re.compile(r'status unknown', re.I)
# "server timeout" in the text: only for other exchange errors without a code (it only changes the verdict
# after an empty reconciliation: "outcome unknown" instead of a reject)
_SERVER_TIMEOUT_TEXT_RE = re.compile(r'server timeout|timed out', re.I)
# "duplicate" next to a client_id field (BingX 101400 "Duplicate clientOrderID", HL cloid)
_CID_HINTS = ('clientorderid', 'orderlinkid', 'clordid', 'clientoid', 'cloid')


@lru_cache(maxsize=None)
def codes_of(exchange_id: str) -> Optional[ModuleType]:
    """Exchange code table (``bytx.exchanges.<exchange>.error_codes``) or None."""
    package = _PACKAGES.get(exchange_id)
    if package is None:
        return None
    return importlib.import_module(f'bytx.exchanges.{package}.error_codes')


def _codes(error: BaseException) -> set[str]:
    out = set(exchange_codes(str(error)))
    cause = error.__cause__
    if cause is not None:
        out.update(exchange_codes(str(cause)))
    code = getattr(error, 'code', None)
    if isinstance(code, (str, int)) and str(code):
        out.add(str(code))
    return out


def _text(error: BaseException) -> str:
    cause = error.__cause__
    return f'{error} {cause}' if cause is not None else str(error)


def refine(exchange_id: Optional[str], error: BytxError) -> BytxError:
    """Refines the SDK error class by an exchange code that ccxt 4.4.65 did not parse.

    "No position" / "size exceeds position" (unconditional codes) → B7 classes; funds —
    :class:`InsufficientFunds`; an explicit parameter reject (the table's ``INVALID_ORDER``) —
    :class:`InvalidOrder`; account position mode not selected (``POSITION_MODE_NOT_SET``, KuCoin 330011) —
    :class:`ConfigError` with a hint. Unknown code or exchange — the error unchanged.
    """
    table = codes_of(exchange_id or '')
    if table is None or isinstance(error, (NetworkError, PositionAlreadyClosed, ReduceExceedsPosition)):
        return error
    typed = position_error(exchange_id or '', error, reduce_only=False)
    if typed is not None:
        return typed
    if type(error) is ExchangeError and _codes(error) & table.INSUFFICIENT_FUNDS:
        funds = InsufficientFunds(error.message, exchange=error.exchange, code=error.code, details=error.details)
        funds.__cause__ = error.__cause__ or error
        return funds
    codes = _codes(error)
    if codes & getattr(table, 'POSITION_MODE_NOT_SET', frozenset()):
        # KuCoin 330011: account position mode not selected — an account setting, retrying will not help
        mode = ConfigError(f'{table.POSITION_MODE_HINT}: {error.message}', exchange=error.exchange,
                           code=error.code, details=error.details)
        mode.__cause__ = error.__cause__ or error
        return mode
    if type(error) in (ExchangeError, InvalidOrder):
        # API trading disabled by the exchange / the key lacks permissions — retrying will not help
        for attr, cls in (('TRADING_UNAVAILABLE', NotSupported), ('PERMISSION', PermissionDenied)):
            if codes & getattr(table, attr, frozenset()):
                closed: BytxError = cls(error.message, exchange=error.exchange, code=error.code,
                                            details=error.details)
                closed.__cause__ = error.__cause__ or error
                return closed
    if type(error) is ExchangeError and codes & getattr(table, 'INVALID_ORDER', frozenset()):
        invalid = InvalidOrder(error.message, exchange=error.exchange, code=error.code, details=error.details)
        invalid.__cause__ = error.__cause__ or error
        return invalid
    return error


def position_error(exchange_id: str, error: BytxError, *, reduce_only: bool) -> Optional[BytxError]:
    """A "no position" / "size exceeds position" reject by exchange code → a typed error."""
    table = codes_of(exchange_id)
    if table is None:
        return None
    codes = _codes(error)
    cls: Optional[type[BytxError]] = None
    if codes & table.POSITION_CLOSED or (reduce_only and codes & table.POSITION_CLOSED_IF_REDUCE):
        cls = PositionAlreadyClosed
    elif codes & table.REDUCE_EXCEEDS:
        cls = ReduceExceedsPosition
    if cls is None:
        return None
    typed = cls(error.message or str(error), exchange=error.exchange, code=error.code, details=error.details)
    typed.__cause__ = error.__cause__ or error
    return typed


def order_not_open(exchange_id: str, error: BaseException) -> bool:
    """Does a cancel reject mean "the order is no longer in the book" (filled, canceled or nonexistent)?

    :class:`OrderNotFound` or a code from the exchange table's ``ORDER_NOT_OPEN``. This does not
    answer what happened to the order — the final status is fetched by a separate request.
    """
    if isinstance(error, OrderNotFound):
        return True
    table = codes_of(exchange_id)
    return table is not None and bool(_codes(error) & getattr(table, 'ORDER_NOT_OPEN', frozenset()))


def classify(exchange_id: str, error: BaseException, *, has_client_id: bool = True) -> Outcome:
    """Outcome of a failed create_order (see the module). The order of checks matters."""
    table = codes_of(exchange_id)
    codes = _codes(error)
    text = _text(error)
    if has_client_id:
        lowered = text.lower()
        if isinstance(error, DuplicateOrderId) or (table is not None and codes & table.DUPLICATE_CLIENT_ID) \
                or ('duplicate' in lowered and any(h in lowered for h in _CID_HINTS)):
            return 'duplicate'
    if isinstance(error, (RateLimited, TimestampError)):
        return 'rejected'  # rejected before the engine: rate limit, clock/recvWindow
    if table is not None and codes & (table.UNKNOWN_OUTCOME | table.UNKNOWN_OUTCOME_UNDOCUMENTED):
        return 'unknown'  # gateway timeout under ccxt's "reject" class (KuCoin 408000 → BadRequest)
    if _STATUS_UNKNOWN_RE.search(text):
        return 'unknown'
    if not isinstance(error, BytxError) or isinstance(error, NetworkError):
        return 'unknown'
    if type(error) is ExchangeError:
        return 'unknown' if _SERVER_TIMEOUT_TEXT_RE.search(text) else 'exchange_error'
    if error.is_business:
        return 'rejected'
    return 'unknown'  # non-ccxt root, BytxError without a class — the order may have gone out


__all__ = ['Outcome', 'classify', 'codes_of', 'order_not_open', 'position_error', 'refine']
