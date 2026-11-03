"""Hyperliquid builder code (builder fee) on user orders.

Design: _broker/HL_BUILDER_CODE_DESIGN.md. The field ``builder: {"b": <address>, "f": <tenths of a bp>}``
is set at the L1 ``order`` action level (after ``grouping``, as in the HL SDK) and is part of the signature.

**Main invariant:** without ``HL_BUILDER_ADDRESS`` (or with ``HL_BUILDER_ENABLED=0``) the feature
is fully off: not a single info request, exchange requests are byte-for-byte unchanged.

Two layers (installed in net._sig.headers._apply_hyperliquid, once per instance):

- layer A — the ``sign_l1_action`` wrapper (ccxt_ids): for ``type == 'order'`` without a vault adds
  ``builder`` only if layer B allowed it for THIS call (ContextVar — the ccxt instance is
  shared by the bot's coroutines, an instance attribute won't do);
- layer B — the instance ``create_orders`` wrapper (ccxt.create_order goes through it on ALL paths:
  open, close, reduce, TP/SL, batch): decides «set / don't» (env, network, vault, order types,
  ``maxBuilderFee`` approval cache); on an explicit HL rejection due to builder it makes exactly one
  immediate retry without builder with the same cloid and a new nonce.

The «set builder» decision:
- only when the user approved ``maxBuilderFee >= f`` (process cache, TTL);
- the ``maxBuilderFee`` info request is NEVER made under the client's order lock:
  the cache refresh is a background task, one per (network, user, builder) at a time; a waiter's
  timeout doesn't cancel it. An opening order waits for an in-flight refresh at most 1.5 s BEFORE
  taking the lock (the ``before_create_order`` hook in the client's create_order wrapper, base.py); inside
  the create_orders wrapper only the cache is used: stale = «unknown» = no builder
  (and a background refresh is started). Wait timeout / failure / refresh cancel → the cache keeps for
  60 s the last known approval (> 0; if revoked, HL rejects → retry without builder),
  otherwise «unknown» — the next opening order doesn't wait again;
- reduceOnly / close / TP-SL **never wait** for the info request: whatever is in the cache is used,
  no value → no builder;
- TP/SL triggers — only with ``HL_BUILDER_ON_TRIGGERS=1`` (default 0: the behavior of a
  resting trigger with builder after approval is revoked hasn't been tested on testnet);
- testnet — only with ``HL_BUILDER_TESTNET=1`` and its own ``HL_BUILDER_TESTNET_ADDRESS``;
- with ``vaultAddress`` — never (a vault can't approve a fee).

An HL rejection due to builder is recognized by the word ``builder`` (case-insensitive) in ``status:"err"``
(``response``) or in ``statuses[i].error`` — only the text
``Builder fee has not been approved.`` is confirmed. Rejection of the whole action → retry without builder; mixed
statuses (some accepted) → ``HyperliquidBuilderFeeRejected(partial=True)`` without retry
(the caller reconciles by cloid as an ambiguous outcome). Timeout / network / 5xx → no retry,
the usual cloid reconciliation applies.

Env (all optional):
    HL_BUILDER_ADDRESS          builder address on mainnet (0x + 40 hex; lowercased)
    HL_BUILDER_FEE_TENTHS_BP    f in tenths of a bp, 1..100 (perps limit), default 30 (0.03 %)
    HL_BUILDER_ENABLED          kill switch, default 1 (if an address is set)
    HL_BUILDER_TESTNET          1 — set builder on testnet too (is_demo), default 0
    HL_BUILDER_TESTNET_ADDRESS  builder address on testnet (required with HL_BUILDER_TESTNET=1)
    HL_BUILDER_ON_TRIGGERS      1 — builder on TP/SL triggers too, default 0
    HL_BUILDER_APPROVAL_TTL_SEC TTL of a positive approval in the cache, default 600
                                (0 / unknown — 60 s)
    HL_BUILDER_RETRY_OPENING    1 — retry without builder for opening orders too (default 1)
    HL_BUILDER_REQUIRE_APPROVAL application policy at bot/cycle start (default 0 —
                                trade without the fee with a WARNING; 1 — don't start). The library
                                only reads it in check_start_approval / Exchange.check_builder_fee;
                                running cycles, reduce/close/SL are never blocked.
Invalid value → ERROR in the log and the feature is off (trading continues without builder).
"""
from __future__ import annotations

import asyncio
import contextvars
import json
import os
import re
import time
from collections import Counter
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Callable, Optional

import ccxt.async_support as ccxt
from loguru import logger

from bytx.net._sig import opts as registry

ENV_ADDRESS = 'HL_BUILDER_ADDRESS'
ENV_FEE = 'HL_BUILDER_FEE_TENTHS_BP'
ENV_ENABLED = 'HL_BUILDER_ENABLED'
ENV_TESTNET = 'HL_BUILDER_TESTNET'
ENV_TESTNET_ADDRESS = 'HL_BUILDER_TESTNET_ADDRESS'
ENV_ON_TRIGGERS = 'HL_BUILDER_ON_TRIGGERS'
ENV_TTL = 'HL_BUILDER_APPROVAL_TTL_SEC'
ENV_RETRY_OPENING = 'HL_BUILDER_RETRY_OPENING'
ENV_REQUIRE_APPROVAL = 'HL_BUILDER_REQUIRE_APPROVAL'

DEFAULT_FEE_TENTHS_BP = 30
MAX_PERP_FEE_TENTHS_BP = 100          # 0.1 % — HL limit on perps
DEFAULT_APPROVAL_TTL_SEC = 600.0
UNKNOWN_APPROVAL_TTL_SEC = 60.0       # for 0 / «unknown»
OPENING_REFRESH_TIMEOUT_SEC = 1.5
BACKGROUND_REFRESH_TIMEOUT_SEC = 10.0  # limit for the background cache refresh task
START_CHECK_TIMEOUT_SEC = 5.0
FILLS_PAGE = 2000
NOT_APPROVED_CODE = 'HL_BUILDER_FEE_NOT_APPROVED'
CHECK_UNAVAILABLE_CODE = 'HL_BUILDER_CHECK_UNAVAILABLE'

MAINNET = 'Mainnet'
TESTNET = 'Testnet'
_HL_EXCHANGE_URL = {MAINNET: 'https://api.hyperliquid.xyz/exchange',
                    TESTNET: 'https://api.hyperliquid-testnet.xyz/exchange'}

_ADDRESS_RE = re.compile(r'^0x[0-9a-fA-F]{40}$')
_SCALAR_RE = re.compile(r'^\s*-?\d+(\.\d+)?\s*$')
_SCALAR_KEY = '__bytx_scalar__'
_TRIGGER_KEYS = ('triggerPrice', 'stopPrice', 'stopLossPrice', 'takeProfitPrice')
_FALSY = ('0', 'false', 'no', 'off')
_TRUTHY = ('1', 'true', 'yes', 'on')

APPROVE_BUILDER_FEE_TYPES = {
    'EIP712Domain': [
        {'name': 'name', 'type': 'string'},
        {'name': 'version', 'type': 'string'},
        {'name': 'chainId', 'type': 'uint256'},
        {'name': 'verifyingContract', 'type': 'address'},
    ],
    'HyperliquidTransaction:ApproveBuilderFee': [
        {'name': 'hyperliquidChain', 'type': 'string'},
        {'name': 'maxFeeRate', 'type': 'string'},
        {'name': 'builder', 'type': 'address'},
        {'name': 'nonce', 'type': 'uint64'},
    ],
}
APPROVE_BUILDER_FEE_PRIMARY_TYPE = 'HyperliquidTransaction:ApproveBuilderFee'


# ─── errors ──────────────────────────────────────────────────────────────────

class HyperliquidBuilderFeeRejected(ccxt.InvalidOrder):
    """HL rejected the order(s) due to the builder fee (typed response parsing).

    whole_action — the whole action was rejected (no order placed) → safe retry;
    partial — some statuses accepted (impossible per docs: builder is action-level) →
    no retry, the outcome is ambiguous for the caller (reconcile by cloid).
    """

    def __init__(self, message: str, *, whole_action: bool, partial: bool,
                 status: str = '', statuses: Optional[list] = None):
        super().__init__(message)
        self.message = message
        self.whole_action = whole_action
        self.partial = partial
        self.status = status
        self.statuses = list(statuses or [])


class HyperliquidBuilderStartError(Exception):
    """Base for bot/cycle start refusals under the HL_BUILDER_REQUIRE_APPROVAL=1 policy (attributes code, status)."""

    code = 'HL_BUILDER_START_REFUSED'

    def __init__(self, status: dict, message: str):
        self.status = dict(status or {})
        super().__init__(message)


class HyperliquidBuilderNotApprovedError(HyperliquidBuilderStartError):
    """Bot/cycle start is forbidden by the HL_BUILDER_REQUIRE_APPROVAL=1 policy: the fee is not approved
    (HL answered, less than required is approved)."""

    code = NOT_APPROVED_CODE

    def __init__(self, status: dict):
        status = dict(status or {})
        need = status.get('max_fee_rate') or '?'
        have = status.get('approved_tenths_bp')
        reason = ('approval could not be verified' if have is None
                  else f'approved {have} of {status.get("fee_tenths_bp")} (tenths of a bp)')
        super().__init__(
            status,
            f"{NOT_APPROVED_CODE}: user {status.get('main_address')} has not approved "
            f"builder fee {need} for {status.get('builder')} on Hyperliquid "
            f"({status.get('network')}): {reason}. Approve the fee with the main wallet "
            f"(ApproveBuilderFee) and start again")


class HyperliquidBuilderCheckUnavailableError(HyperliquidBuilderStartError):
    """Start is forbidden by the HL_BUILDER_REQUIRE_APPROVAL=1 policy, but approval was NOT verified:
    info maxBuilderFee is unavailable (network/timeout/HL) and the cache has no value. This is not a user
    refusal — retry the check later (not a NotApproved subclass)."""

    code = CHECK_UNAVAILABLE_CODE

    def __init__(self, status: dict):
        status = dict(status or {})
        super().__init__(
            status,
            f"{CHECK_UNAVAILABLE_CODE}: could not verify approval of builder fee "
            f"{status.get('max_fee_rate') or '?'} by user {status.get('main_address')} for "
            f"{status.get('builder')} on Hyperliquid ({status.get('network')}): "
            f"{status.get('error') or 'no response'}. Try starting again later")


# ─── configuration ──────────────────────────────────────────────────────────

@dataclass(frozen=True)
class BuilderConfig:
    address: str            # lowercase
    fee: int                # tenths of a bp
    network: str            # 'Mainnet' / 'Testnet'
    on_triggers: bool = False
    retry_opening: bool = True
    approval_ttl: float = DEFAULT_APPROVAL_TTL_SEC

    @property
    def max_fee_rate(self) -> str:
        return fee_rate_str(self.fee)

    def wire(self) -> dict:
        return {'b': self.address, 'f': self.fee}


_logged: set = set()


def _log_once(key: str, level: str, message: str) -> None:
    if key in _logged:
        return
    _logged.add(key)
    getattr(logger.bind(hl_builder='config'), level)(message)


def fee_rate_str(fee_tenths_bp: int) -> str:
    """f (tenths of a bp) → HL maxFeeRate string: 30 → '0.03%', 1 → '0.001%', 100 → '0.1%'."""
    d = (Decimal(int(fee_tenths_bp)) / Decimal(1000)).normalize()
    s = format(d, 'f')
    return f'{s}%'


def _env(name: str, exchange=None) -> str:
    if name in registry.KEYS:
        return registry.resolve(name, exchange)
    return (os.getenv(name) or '').strip()


def _env_flag(name: str, default: bool) -> bool:
    v = _env(name).lower()
    if not v:
        return default
    if v in _TRUTHY:
        return True
    if v in _FALSY:
        return False
    _log_once(f'flag:{name}:{v}', 'error', f"{name}={v!r}: expected 0/1 — using {int(default)}")
    return default


def builder_require_approval() -> bool:
    """Service start policy: HL_BUILDER_REQUIRE_APPROVAL (default 0)."""
    return _env_flag(ENV_REQUIRE_APPROVAL, False)


def builder_config(testnet: bool = False, exchange=None) -> Optional[BuilderConfig]:
    """Builder config for the network, or None (off / not set / invalid)."""
    if not _env_flag(ENV_ENABLED, True):
        return None
    if testnet:
        if not _env_flag(ENV_TESTNET, False):
            return None
        raw_addr, addr_env = _env(ENV_TESTNET_ADDRESS, exchange), ENV_TESTNET_ADDRESS
        if not raw_addr:
            _log_once('testnet-no-address', 'error',
                      f"{ENV_TESTNET}=1 without {ENV_TESTNET_ADDRESS}: builder on testnet is off")
            return None
    else:
        raw_addr, addr_env = _env(ENV_ADDRESS, exchange), ENV_ADDRESS
        if not raw_addr:
            return None
    if not _ADDRESS_RE.fullmatch(raw_addr):
        _log_once(f'addr:{addr_env}', 'error',
                  f"{addr_env}: not a 0x + 40 hex address — builder fee is off (trading without it)")
        return None
    raw_fee = _env(ENV_FEE, exchange)
    try:
        fee = int(raw_fee) if raw_fee else DEFAULT_FEE_TENTHS_BP
    except ValueError:
        fee = None
    if fee is None or not 1 <= fee <= MAX_PERP_FEE_TENTHS_BP:
        _log_once(f'fee:{raw_fee}', 'error',
                  f"{ENV_FEE}={raw_fee!r}: expected an integer 1..{MAX_PERP_FEE_TENTHS_BP} "
                  f"(tenths of a bp) — builder fee is off (trading without it)")
        return None
    raw_ttl = _env(ENV_TTL)
    try:
        ttl = float(raw_ttl) if raw_ttl else DEFAULT_APPROVAL_TTL_SEC
        if ttl <= 0:
            raise ValueError
    except ValueError:
        _log_once(f'ttl:{raw_ttl}', 'error', f"{ENV_TTL}={raw_ttl!r}: using {DEFAULT_APPROVAL_TTL_SEC}")
        ttl = DEFAULT_APPROVAL_TTL_SEC
    return BuilderConfig(address=raw_addr.lower(), fee=fee, network=TESTNET if testnet else MAINNET,
                         on_triggers=_env_flag(ENV_ON_TRIGGERS, False),
                         retry_opening=_env_flag(ENV_RETRY_OPENING, True), approval_ttl=ttl)


def _is_testnet(exchange) -> bool:
    options = getattr(exchange, 'options', None)
    return bool(isinstance(options, dict) and options.get('sandboxMode'))


def config_for_exchange(exchange) -> Optional[BuilderConfig]:
    return builder_config(_is_testnet(exchange), exchange)


def _user_of(exchange) -> Optional[str]:
    w = getattr(exchange, 'walletAddress', None)
    return str(w).lower() if w else None


# ─── approval cache (process) ──────────────────────────────────────────────

# (network, user, builder) → (max_fee | None, ts, ttl)
_approvals: dict[tuple, tuple[Optional[int], float, float]] = {}


def _now() -> float:
    return time.monotonic()


def _key(cfg: BuilderConfig, user: str) -> tuple:
    return (cfg.network, user.lower(), cfg.address)


def _store(cfg: BuilderConfig, user: str, value: Optional[int]) -> None:
    ttl = cfg.approval_ttl if (value or 0) > 0 else UNKNOWN_APPROVAL_TTL_SEC
    _approvals[_key(cfg, user)] = (value, _now(), ttl)


def _store_unknown(cfg: BuilderConfig, user: str) -> Optional[int]:
    """Info failure / timeout / cancel: approval not verified.

    The last known positive approval is kept (with the short «unknown» TTL,
    60 s — then checked again): builder stays on. If approval was actually
    revoked, HL explicitly rejects the action → one retry without builder and the cache reset to 0.
    No positive value → «unknown» (None): no builder. Returns what was stored.
    """
    prev, _ = _lookup(cfg, user)
    if prev is not None and prev > 0:
        _approvals[_key(cfg, user)] = (prev, _now(), UNKNOWN_APPROVAL_TTL_SEC)
        return prev
    _store(cfg, user, None)
    return None


def _lookup(cfg: BuilderConfig, user: str) -> tuple[Optional[int], bool]:
    """(last known value | None, whether it is fresh)."""
    hit = _approvals.get(_key(cfg, user))
    if hit is None:
        return None, False
    value, ts, ttl = hit
    return value, (_now() - ts) < ttl


def reset_approval_cache(user: Optional[str] = None) -> None:
    """Resets the approval cache (entirely or for one user) — e.g. after an approve on the frontend."""
    if user is None:
        _approvals.clear()
        return
    u = user.lower()
    for k in [k for k in _approvals if k[1] == u]:
        _approvals.pop(k, None)


# ─── counters and hook ──────────────────────────────────────────────────────

_counts: Counter = Counter()
_global_hook: Optional[Callable[[dict], Any]] = None


def hl_builder_counts() -> dict:
    """Process counters: placed, fallback, fallback_reduce, skipped_<reason>, start_*."""
    return dict(_counts)


def set_builder_fallback_hook(hook: Optional[Callable[[dict], Any]]) -> None:
    """Global builder-rejection hook (e.g. for a metric or an alert)."""
    global _global_hook
    _global_hook = hook


def _call_hooks(exchange, info: dict) -> None:
    for hook in (getattr(exchange, '_bytx_hl_builder_on_fallback', None), _global_hook):
        if hook is None:
            continue
        try:
            res = hook(dict(info))
            if asyncio.iscoroutine(res):
                asyncio.ensure_future(res)
        except Exception as e:   # the hook never breaks trading
            logger.bind(hl_builder='hook').warning(f"HL builder: on_builder_fallback hook failed: {e}")


# ─── info: maxBuilderFee ────────────────────────────────────────────────────

def _parse_max_fee(resp) -> int:
    if isinstance(resp, dict) and _SCALAR_KEY in resp:
        resp = resp[_SCALAR_KEY]
    if isinstance(resp, bool):
        raise ValueError(f'unexpected maxBuilderFee response {resp!r}')
    if isinstance(resp, (int, float)):
        return int(resp)
    if isinstance(resp, str) and _SCALAR_RE.fullmatch(resp):
        return int(float(resp))
    raise ValueError(f'unexpected maxBuilderFee response {resp!r}')


async def fetch_max_builder_fee(exchange, user: str, builder: str) -> int:
    """POST /info {"type":"maxBuilderFee"} — approved maximum in tenths of a bp (public)."""
    resp = await exchange.publicPostInfo({'type': 'maxBuilderFee', 'user': user.lower(),
                                          'builder': builder.lower()})
    return _parse_max_fee(resp)


async def _refresh(exchange, cfg: BuilderConfig, user: str) -> Optional[int]:
    """Refreshes the cache. Failure → the last known approval (>0) or «unknown» for 60 s."""
    prev, _ = _lookup(cfg, user)
    try:
        value = await fetch_max_builder_fee(exchange, user, cfg.address)
    except Exception as e:
        kept = _store_unknown(cfg, user)
        logger.bind(hl_builder='approval', user=user).warning(
            f"HL builder: maxBuilderFee not received ({type(e).__name__}: {e}); for "
            f"{UNKNOWN_APPROVAL_TTL_SEC:.0f} s " + (
                f"keeping the last approval {kept}" if kept is not None
                else f"approval «unknown» (was {prev}) — orders without builder"))
        return kept
    _store(cfg, user, value)
    if value < cfg.fee and prev != value:
        _counts['approval_insufficient'] += 1
        logger.bind(hl_builder='approval', user=user, approved=value, fee=cfg.fee).warning(
            f"HL builder: user {user} approved {value} < {cfg.fee} (tenths of a bp) for "
            f"{cfg.address} — orders go WITHOUT builder fee (reason=insufficient_approval)")
    return value


# (network, user, builder) → in-flight background refresh task (at most one per key)
_refresh_tasks: dict[tuple, asyncio.Task] = {}


async def _background_refresh(exchange, cfg: BuilderConfig, user: str) -> Optional[int]:
    try:
        return await asyncio.wait_for(_refresh(exchange, cfg, user), BACKGROUND_REFRESH_TIMEOUT_SEC)
    except asyncio.CancelledError:
        _store_unknown(cfg, user)
        raise
    except Exception as e:   # background task timeout
        kept = _store_unknown(cfg, user)
        logger.bind(hl_builder='approval', user=user).warning(
            f"HL builder: maxBuilderFee not received ({type(e).__name__}); for "
            f"{UNKNOWN_APPROVAL_TTL_SEC:.0f} s " + (
                f"keeping the last approval {kept}" if kept is not None
                else "approval «unknown» — orders without builder"))
        return kept


def _ensure_refresh(exchange, cfg: BuilderConfig, user: str) -> asyncio.Task:
    """Background cache refresh task: the one already running for this key, or a new one."""
    key = _key(cfg, user)
    loop = asyncio.get_running_loop()
    task = _refresh_tasks.get(key)
    if task is not None and not task.done() and task.get_loop() is loop:
        return task
    task = loop.create_task(_background_refresh(exchange, cfg, user))
    _refresh_tasks[key] = task

    def _done(t, k=key):
        if _refresh_tasks.get(k) is t:
            _refresh_tasks.pop(k, None)
        if not t.cancelled():
            t.exception()   # avoids «Task exception was never retrieved»
    task.add_done_callback(_done)
    return task


async def _wait_refresh(cfg: BuilderConfig, user: str, task: asyncio.Task, timeout: float) -> None:
    """Waits for the background task at most timeout, WITHOUT cancelling it. Not done → the last
    known approval (>0) or «unknown» for 60 s (the next opening order doesn't wait)."""
    done, _ = await asyncio.wait({task}, timeout=timeout)
    if not done and not _lookup(cfg, user)[1]:
        _store_unknown(cfg, user)
        _counts['refresh_wait_timeout'] += 1


def _approval_for_order(exchange, cfg: BuilderConfig, user: str, opening: bool) -> Optional[int]:
    """Decision inside the create_orders wrapper (under the order lock): cache only, no waiting.

    reduce/close/TP-SL — the last value (even stale). Opening — fresh only;
    stale = «unknown» (no builder) + a background refresh for subsequent orders.
    """
    value, fresh = _lookup(cfg, user)
    if fresh or not opening:
        return value
    _ensure_refresh(exchange, cfg, user)
    return None


def _create_order_params(args, kwargs) -> dict:
    """params from a ccxt create_order(symbol, type, side, amount, price=None, params={}) call."""
    params = kwargs.get('params', args[5] if len(args) > 5 else None)
    return params if isinstance(params, dict) else {}


async def before_create_order(exchange, args=(), kwargs=None) -> None:
    """Hook BEFORE taking the client's order lock (base._serialize_create_order).

    With a stale cache an opening order waits for the background refresh at most 1.5 s — outside the lock,
    so this client's concurrent close/SL/TP don't wait for it. Off / reduce / trigger /
    fresh cache → returns immediately, with no requests and without yielding to the loop.
    """
    cfg = config_for_exchange(exchange)
    user = _user_of(exchange)
    if cfg is None or not user:
        return
    params = _create_order_params(args, kwargs or {})
    if params.get('vaultAddress'):
        return
    reduce_only, trigger = _classify([{'params': params}])
    if reduce_only or trigger or _lookup(cfg, user)[1]:
        return
    await _wait_refresh(cfg, user, _ensure_refresh(exchange, cfg, user), OPENING_REFRESH_TIMEOUT_SEC)


# ─── ContextVar: decision for the current create_orders call ───────────────

@dataclass
class _CallState:
    builder: Optional[dict]
    nonces: list = field(default_factory=list)


_CALL: contextvars.ContextVar[Optional[_CallState]] = contextvars.ContextVar('bytx_hl_builder_call',
                                                                              default=None)


def on_sign_l1_action(action, nonce, vault) -> None:
    """Layer A (called from the sign_l1_action wrapper in ccxt_ids, AFTER brokerCode is removed)."""
    st = _CALL.get()
    if st is None or not isinstance(action, dict) or action.get('type') != 'order':
        return
    st.nonces.append(nonce)
    if st.builder is None or vault is not None:
        return
    # the same dict that goes into the body → builder is both in the signature and the request; key AFTER grouping
    action['builder'] = {'b': st.builder['b'], 'f': st.builder['f']}


# ─── rejection detection ────────────────────────────────────────────────────

def _has_builder(text) -> bool:
    return isinstance(text, str) and 'builder' in text.lower()


def detect_builder_rejection(response) -> Optional[HyperliquidBuilderFeeRejected]:
    """HL rejection due to builder in the /exchange response → exception (not raised) or None."""
    if not isinstance(response, dict):
        return None
    status = str(response.get('status') or '')
    if status == 'err':
        text = response.get('response')
        if _has_builder(text):
            return HyperliquidBuilderFeeRejected(f'hyperliquid {text}', whole_action=True,
                                                 partial=False, status=status)
        return None
    payload = response.get('response')
    data = payload.get('data') if isinstance(payload, dict) else None
    statuses = data.get('statuses') if isinstance(data, dict) else None
    if not isinstance(statuses, list) or not statuses:
        return None
    errs = [s.get('error') for s in statuses if isinstance(s, dict) and _has_builder(s.get('error'))]
    if not errs:
        return None
    whole = len(errs) == len(statuses)
    return HyperliquidBuilderFeeRejected(f'hyperliquid {errs[0]}', whole_action=whole,
                                         partial=not whole, status=status, statuses=statuses)


# ─── wrapper installation (layer B) ─────────────────────────────────────────

def _classify(orders) -> tuple[bool, bool]:
    """(any_reduce_only, any_trigger) over the create_orders orders."""
    reduce_only = trigger = False
    for o in orders or []:
        p = (o.get('params') if isinstance(o, dict) else None) or {}
        if p.get('reduceOnly'):
            reduce_only = True
        if any(p.get(k) is not None for k in _TRIGGER_KEYS):
            trigger = True
    return reduce_only, trigger


def _cids(orders) -> list:
    out = []
    for o in orders or []:
        p = (o.get('params') if isinstance(o, dict) else None) or {}
        cid = p.get('clientOrderId') or p.get('client_id')
        if cid:
            out.append(str(cid))
    return out


async def _ensure_new_nonce(exchange, nonces: list) -> None:
    last = max(nonces) if nonces else None
    if last is None:
        return
    for _ in range(1000):
        if exchange.milliseconds() > last:
            return
        await asyncio.sleep(0.001)


def install(exchange) -> None:
    """create_orders / handle_errors / on_rest_response wrappers on ccxt.hyperliquid (once)."""
    if getattr(exchange, '_bytx_hl_builder_installed', False):
        return
    exchange._bytx_hl_builder_installed = True
    original_create_orders = exchange.create_orders
    original_handle_errors = exchange.handle_errors
    original_on_rest_response = exchange.on_rest_response

    def on_rest_response(code, reason, url, method, headers, body, request_headers, request_body):
        body = original_on_rest_response(code, reason, url, method, headers, body,
                                         request_headers, request_body)
        # ccxt parses JSON only for {..}/[..]: a bare maxBuilderFee number would otherwise come
        # back as an aiohttp StreamReader. Affects ONLY our maxBuilderFee request.
        if isinstance(request_body, (bytes, bytearray)):
            request_body = request_body.decode('utf-8', 'replace')
        if (isinstance(request_body, str) and '"maxBuilderFee"' in request_body
                and isinstance(body, str) and _SCALAR_RE.fullmatch(body)):
            return json.dumps({_SCALAR_KEY: body.strip()})
        return body

    def handle_errors(code, reason, url, method, headers, body, response, requestHeaders, requestBody):
        st = _CALL.get()
        if st is not None and st.builder is not None and str(url).endswith('/exchange'):
            rejected = detect_builder_rejection(response)
            if rejected is not None:
                raise rejected
        return original_handle_errors(code, reason, url, method, headers, body, response,
                                      requestHeaders, requestBody)

    async def create_orders(orders, params={}):
        cfg = config_for_exchange(exchange)
        user = _user_of(exchange)
        if cfg is None or not user or (isinstance(params, dict) and params.get('vaultAddress')):
            return await original_create_orders(orders, params)
        reduce_only, trigger = _classify(orders)
        if trigger and not cfg.on_triggers:
            _counts['skipped_trigger'] += 1
            return await original_create_orders(orders, params)
        opening = not reduce_only and not trigger
        approved = _approval_for_order(exchange, cfg, user, opening)
        if approved is None or approved < cfg.fee:
            _counts['skipped_unknown' if approved is None else 'skipped_not_approved'] += 1
            return await original_create_orders(orders, params)

        state = _CallState(builder=cfg.wire())
        token = _CALL.set(state)
        try:
            result = await original_create_orders(orders, params)
            _counts['placed'] += 1
            return result
        except HyperliquidBuilderFeeRejected as e:
            if e.partial or not e.whole_action:
                _counts['rejected_partial'] += 1
                logger.bind(hl_builder='partial', user=user, cids=_cids(orders)).error(
                    f"HL builder: mixed response (some orders accepted, some rejected due to "
                    f"builder) — no retry, outcome ambiguous: {e}")
                raise
            first_error = e
        finally:
            _CALL.reset(token)

        # explicit rejection of the WHOLE action due to builder: no order placed → one retry
        _store(cfg, user, 0)
        critical = reduce_only or trigger
        _counts['fallback'] += 1
        if critical:
            _counts['fallback_reduce'] += 1
        info = {'reason': 'rejected', 'reduce_only': reduce_only, 'trigger': trigger,
                'opening': opening, 'cids': _cids(orders), 'user': user, 'builder': cfg.address,
                'fee': cfg.fee, 'network': cfg.network, 'error': first_error.message,
                'retried': bool(critical or cfg.retry_opening)}
        log = logger.bind(hl_builder='fallback', reduce_only=reduce_only, trigger=trigger,
                          cid=','.join(info['cids']))
        (log.error if critical else log.warning)(
            f"HL builder: order rejected due to builder fee ({first_error.message}); "
            f"{'retrying without builder' if info['retried'] else 'no retry (HL_BUILDER_RETRY_OPENING=0)'}, "
            f"approval cache for {user} reset to 0")
        _call_hooks(exchange, info)
        if not info['retried']:
            raise first_error
        await _ensure_new_nonce(exchange, state.nonces)
        token = _CALL.set(_CallState(builder=None))
        try:
            return await original_create_orders(orders, params)
        finally:
            _CALL.reset(token)

    async def before(args=(), kwargs=None):
        await before_create_order(exchange, args, kwargs)

    exchange.on_rest_response = on_rest_response
    exchange.handle_errors = handle_errors
    exchange.create_orders = create_orders
    # called by the client's create_order wrapper (base.py) BEFORE taking the order lock
    exchange._bytx_before_create_order = before


# ─── public API: approval status ────────────────────────────────────────────

def _status(cfg: Optional[BuilderConfig], user: Optional[str], value: Optional[int],
            testnet: bool, checked: bool, error: Optional[str] = None) -> dict:
    if cfg is None:
        return {'enabled': False, 'builder': None, 'fee_tenths_bp': None, 'max_fee_rate': None,
                'approved_tenths_bp': None, 'approved': None, 'checked': False,
                'network': TESTNET if testnet else MAINNET,
                'main_address': user.lower() if user else None, 'error': None}
    return {'enabled': True, 'builder': cfg.address, 'fee_tenths_bp': cfg.fee,
            'max_fee_rate': cfg.max_fee_rate, 'approved_tenths_bp': value,
            'approved': None if value is None else value >= cfg.fee, 'checked': checked,
            'network': cfg.network, 'main_address': user.lower() if user else None,
            'error': error}


def cached_status(exchange) -> Optional[dict]:
    """Status from the cache, no requests (None — not HL)."""
    if str(getattr(exchange, 'id', '')).lower() != 'hyperliquid':
        return None
    cfg = config_for_exchange(exchange)
    user = _user_of(exchange)
    value = _lookup(cfg, user)[0] if cfg is not None and user else None
    return _status(cfg, user, value, _is_testnet(exchange), checked=False)


async def refresh_status(exchange, timeout: Optional[float] = START_CHECK_TIMEOUT_SEC) -> Optional[dict]:
    """Fetches maxBuilderFee, refreshes the cache and returns the status (None — not HL)."""
    if str(getattr(exchange, 'id', '')).lower() != 'hyperliquid':
        return None
    testnet = _is_testnet(exchange)
    cfg = builder_config(testnet, exchange)   # instance config (BrokerConfig) — same as for orders
    user = _user_of(exchange)
    if cfg is None or not user:
        return _status(cfg, user, None, testnet, checked=False)
    error = None
    try:
        coro = fetch_max_builder_fee(exchange, user, cfg.address)
        value = await (asyncio.wait_for(coro, timeout) if timeout else coro)
        _store(cfg, user, value)
        checked = True
    except Exception as e:
        error = f'{type(e).__name__}: {e}'
        value, _ = _lookup(cfg, user)
        checked = False
    return _status(cfg, user, value, testnet, checked=checked, error=error)


def check_start_approval(status: Optional[dict], require: Optional[bool] = None,
                         bot_id: Optional[str] = None) -> Optional[dict]:
    """Bot/cycle start policy by status. require=None → HL_BUILDER_REQUIRE_APPROVAL.

    Off / approved → status. Not approved (or not verified): require → exception
    (HL answered «too little/none» → HyperliquidBuilderNotApprovedError, code HL_BUILDER_FEE_NOT_APPROVED;
    check failed and there is no value — checked=False, approved=None →
    HyperliquidBuilderCheckUnavailableError, code HL_BUILDER_CHECK_UNAVAILABLE); otherwise WARNING +
    counter and status (trading without the fee).
    For START only: running cycles, reduce/close/SL don't call this check.
    """
    if not status or not status.get('enabled') or status.get('approved'):
        return status
    require = builder_require_approval() if require is None else require
    log = logger.bind(hl_builder='start', bot_id=bot_id, user=status.get('main_address'))
    if require:
        if not status.get('checked') and status.get('approved') is None:
            _counts['start_check_unavailable'] += 1
            log.error(f"[{bot_id}]\tHL builder: start forbidden — approval not verified "
                      f"({status.get('error')})")
            raise HyperliquidBuilderCheckUnavailableError(status)
        _counts['start_refused'] += 1
        log.error(f"[{bot_id}]\tHL builder: start forbidden — fee not approved "
                  f"(approved={status.get('approved_tenths_bp')}, required {status.get('fee_tenths_bp')})")
        raise HyperliquidBuilderNotApprovedError(status)
    _counts['start_without_approval'] += 1
    log.warning(f"[{bot_id}]\tHL builder: fee not approved (approved={status.get('approved_tenths_bp')}, "
                f"required {status.get('fee_tenths_bp')}) — the bot trades WITHOUT builder fee "
                f"({ENV_REQUIRE_APPROVAL}=0)")
    return status


def _public_exchange(testnet: bool):
    from bytx.net._sig.headers import apply_broker_ids
    ex = ccxt.hyperliquid({'options': {'defaultType': 'swap'}})
    apply_broker_ids(ex)
    if testnet:
        ex.set_sandbox_mode(True)
    return ex


async def get_builder_approval(user: str, testnet: bool = False, refresh: bool = True,
                               timeout: Optional[float] = START_CHECK_TIMEOUT_SEC,
                               exchange=None) -> dict:
    """User's builder fee approval status (HL main address) — no keys needed.

    Response: {enabled, builder, fee_tenths_bp, max_fee_rate, approved_tenths_bp, approved,
    checked, network, main_address, error}. refresh=False — process cache only.
    """
    cfg = builder_config(testnet, exchange)
    if cfg is None or not user:
        return _status(cfg, user, None, testnet, checked=False)
    if not refresh:
        return _status(cfg, user, _lookup(cfg, user)[0], testnet, checked=False)
    own = exchange is None
    ex = exchange if exchange is not None else _public_exchange(testnet)
    try:
        error = None
        try:
            coro = fetch_max_builder_fee(ex, user, cfg.address)
            value = await (asyncio.wait_for(coro, timeout) if timeout else coro)
            _store(cfg, user, value)
            checked = True
        except Exception as e:
            error = f'{type(e).__name__}: {e}'
            value, _ = _lookup(cfg, user)
            checked = False
        return _status(cfg, user, value, testnet, checked=checked, error=error)
    finally:
        if own:
            await ex.close()


def builder_approval_typed_data(testnet: bool = False, nonce: Optional[int] = None,
                                chain_id: Optional[int] = None,
                                max_fee_rate: Optional[str] = None, exchange=None) -> dict:
    """EIP-712 ApproveBuilderFee template for signing with the MAIN wallet (eth_signTypedData_v4).

    chain_id not set → domain.chainId = None: the frontend sets the wallet network's chainId and
    action.signatureChainId = hex(chainId). max_fee_rate='0%' — revoke. Not configured → ValueError.
    """
    cfg = builder_config(testnet, exchange)   # exchange — config from its BrokerConfig
    if cfg is None:
        raise ValueError('HL builder fee is not configured (HL_BUILDER_ADDRESS / network)')
    nonce = int(nonce if nonce is not None else time.time() * 1000)
    rate = max_fee_rate or cfg.max_fee_rate
    message = {'hyperliquidChain': cfg.network, 'maxFeeRate': rate, 'builder': cfg.address,
               'nonce': nonce}
    return {
        'domain': {'name': 'HyperliquidSignTransaction', 'version': '1',
                   'chainId': int(chain_id) if chain_id is not None else None,
                   'verifyingContract': '0x0000000000000000000000000000000000000000'},
        'types': {k: [dict(f) for f in v] for k, v in APPROVE_BUILDER_FEE_TYPES.items()},
        'primaryType': APPROVE_BUILDER_FEE_PRIMARY_TYPE,
        'message': message,
        # POST /exchange body after signing: {"action": action, "nonce": nonce,
        # "signature": {"r","s","v"}}; signatureChainId = hex(domain.chainId)
        'action': {'type': 'approveBuilderFee', 'hyperliquidChain': cfg.network,
                   'signatureChainId': hex(int(chain_id)) if chain_id is not None else None,
                   'maxFeeRate': rate, 'builder': cfg.address, 'nonce': nonce},
        'nonce': nonce,
        'exchange_url': _HL_EXCHANGE_URL[cfg.network],
        'fee_tenths_bp': cfg.fee,
        'network': cfg.network,
    }


__all__ = [
    'BuilderConfig', 'HyperliquidBuilderFeeRejected', 'HyperliquidBuilderNotApprovedError',
    'HyperliquidBuilderStartError', 'HyperliquidBuilderCheckUnavailableError',
    'NOT_APPROVED_CODE', 'CHECK_UNAVAILABLE_CODE', 'builder_config', 'builder_require_approval', 'fee_rate_str',
    'get_builder_approval', 'builder_approval_typed_data', 'reset_approval_cache', 'hl_builder_counts',
    'set_builder_fallback_hook', 'check_start_approval', 'detect_builder_rejection',
    'fetch_max_builder_fee',
    'ENV_ADDRESS', 'ENV_FEE', 'ENV_ENABLED', 'ENV_TESTNET', 'ENV_TESTNET_ADDRESS',
    'ENV_ON_TRIGGERS', 'ENV_TTL', 'ENV_RETRY_OPENING', 'ENV_REQUIRE_APPROVAL',
]
