"""Next-generation client_id (introduced together with the journal).

Format: ``{tag}{role}{tail}``, where

- **tag** — the default bot tag ``d`` + 12 hex of sha1(bot_id) (13 chars,
    :func:`bot_tag`; a contract across releases) or a custom ``client_id_prefix`` (letters/digits,
    up to 14; case is preserved — ``owns()`` compares the prefix as is; BingX allows only
    lowercase: the exchange lowercases ids itself);
- **role** — one character **outside hex**: old ids (tag + hex tail) never get confused with new ones;

  ====  =========  ==================================================
  code  role       what else is in the id
  ====  =========  ==================================================
  n     entry
  g     dca
  p     tp         level: 2 base36 chars (0–1295)
  q     sl         level: 2 base36 chars
  r     tp_reduce  (tag + r + 12 hex)
  x     close
  ====  =========  ==================================================

- **tail** — random, lowercase base36 (BingX lowercases ids).

Length ≤ 24 (with a Binance broker prefix ``x-<ID>`` of up to 12 chars — exactly 36, New Order),
and no more than the exchange's :func:`bytx.order_params.max_client_id_len`: with a long broker
prefix the tail gets shorter, not the tag. The tail is at least 6 chars.

The position is **not encoded** in the cid: the journal stores the cid → position → role link (the ``intent`` event
is written before sending) and it survives a process crash.

Hyperliquid: cloid is a 128-bit hex, the role character does not fit; there id = ``0x`` + tag +
hex tail, role — from the journal only.

HTX USDT-M: client_order_id is an integer ≤ 2^63−1 (19 digits): neither the ``d`` + hex tag nor the role character
fits — :func:`make_client_id` raises ConfigError, the journal (``PositionJournal.client_id``) does not
encode the id (None): the SDK sets a numeric id, the cid → role link lives only in the journal (the ``intent`` event).

Old formats are recognized (:func:`parse_client_id`): a bot limit order (tag + 8 hex)
→ role ``legacy_dca``; algo TP/SL ``t|s{level}{20–24 hex}`` (no bot tag) — role and
level, ``generation='legacy_algo'``.
"""
from __future__ import annotations

import hashlib
import re
import secrets
from dataclasses import dataclass
from typing import Any, Iterable, Optional

from bytx.errors import ConfigError
from bytx.exchanges.hyperliquid.cloid import cloid_body, tagged_cloid
from bytx.order_params import CLIENT_ID_FORMATS, GATE_TEXT_PREFIX, _ccxt_id, max_client_id_len, numeric_client_id

ROLE_CODES = {'entry': 'n', 'dca': 'g', 'tp': 'p', 'sl': 'q', 'tp_reduce': 'r', 'close': 'x'}
CODE_ROLES = {code: role for role, code in ROLE_CODES.items()}
LEVEL_ROLES = frozenset({'tp', 'sl'})
LEVEL_WIDTH = 2
LEVEL_MAX = 36 ** LEVEL_WIDTH - 1
V2_MAX_LEN = 24
MIN_TAIL = 6

BOT_TAG_RE = re.compile(r'd[0-9a-f]{12}')
_TAG_RE = re.compile(r'[A-Za-z0-9]{1,14}')
_HEX = frozenset('0123456789abcdef')
_B36 = '0123456789abcdefghijklmnopqrstuvwxyz'
# old-format algo TP/SL: t|s + level + 20 (new) / 24 hex
LEGACY_ALGO_RE = re.compile(r'([ts])(\d{1,3})([0-9a-f]{20}|[0-9a-f]{24})')


def bot_tag(bot_id: Any) -> str:
    """Default bot tag: ``"d" + sha1(str(bot_id))[:12]`` (a contract across releases)."""
    return 'd' + hashlib.sha1(str(bot_id).encode()).hexdigest()[:12]  # nosec B324 — not a crypto primitive


@dataclass(frozen=True, slots=True)
class ClientIdInfo:
    """Parse a client_id.

    ``generation``: ``'v2'`` — the new format with a role; ``'legacy'`` — tag + hex (role
    ``legacy_dca``, the exact role comes from the journal); ``'legacy_algo'`` — old-format algo TP/SL without
    a tag; ``'hyperliquid'`` — cloid with a tag (role from the journal only); ``'tagged'`` — tag present,
    role character unknown; ``'foreign'`` — not a BYTtrade format; ``'none'`` — no id.
    """

    client_id: Optional[str]
    generation: str
    bot_tag: Optional[str] = None
    role: Optional[str] = None
    level_id: Optional[int] = None


def _b36(value: int, width: int) -> str:
    out = ''
    for _ in range(width):
        value, rem = divmod(value, 36)
        out = _B36[rem] + out
    return out


def _check_tag(tag: Any, ex_id: str) -> str:
    value = str(tag) if tag is not None else ''
    if ex_id == 'hyperliquid' and value[:2].lower() == '0x':
        value = value[2:]
    if not _TAG_RE.fullmatch(value):
        raise ConfigError(f'client_id tag: letters and digits, 1–14 characters, not {tag!r}',
                          exchange=ex_id or None, details={'tag': tag})
    if ex_id == 'bingx' and value != value.lower():
        # BingX lowercases clientOrderID — an uppercase tag would not be found by prefix
        raise ConfigError(f'client_id tag for BingX must have no uppercase, not {tag!r}', exchange=ex_id,
                          details={'tag': tag})
    return value


def make_client_id(exchange: Any, tag: str, role: str, *, level_id: Optional[int] = None,
                   market_type: str = 'swap') -> str:
    """New client_id ``{tag}{role}{tail}`` in the exchange's format and length.

    ``exchange`` — :class:`~bytx.Exchange`, a ccxt instance or an exchange name (without the broker
    prefix). ``role`` — a :data:`ROLE_CODES` key; ``level_id`` — required for ``tp``/``sl``
    and forbidden for the rest. Invalid arguments or an id that does not fit — :class:`ConfigError`.
    """
    ex_id = _ccxt_id(exchange)
    t = _check_tag(tag, ex_id)
    if role not in ROLE_CODES:
        raise ConfigError(f'client_id role: one of {sorted(ROLE_CODES)}, not {role!r} '
                          '(other roles exist only in the journal)', exchange=ex_id or None, details={'role': role})
    if role in LEVEL_ROLES:
        if not isinstance(level_id, int) or isinstance(level_id, bool) or not 0 <= level_id <= LEVEL_MAX:
            raise ConfigError(f'level_id for {role}: integer 0–{LEVEL_MAX}, not {level_id!r}',
                              exchange=ex_id or None, details={'level_id': level_id})
    elif level_id is not None:
        raise ConfigError(f'level_id is encoded only for tp/sl, not for {role}', exchange=ex_id or None,
                          details={'level_id': level_id, 'role': role})
    if numeric_client_id(ex_id, market_type):
        raise ConfigError(f'{ex_id} {market_type}: client_id is an integer, the tag and role do not fit in it '
                          '(role — only in the journal)', exchange=ex_id, details={'tag': tag, 'role': role})
    if ex_id == 'hyperliquid':
        try:
            return tagged_cloid(t)
        except ValueError as e:
            raise ConfigError(str(e), exchange=ex_id, details={'tag': tag}) from None
    head = t + ROLE_CODES[role] + (_b36(level_id, LEVEL_WIDTH) if role in LEVEL_ROLES else '')  # type: ignore[arg-type]
    # Gate: text = "t-" + up to 28 chars (the prefix is mandatory and not counted in the length)
    lead = GATE_TEXT_PREFIX if ex_id == 'gate' else ''
    limit = min(V2_MAX_LEN, max_client_id_len(exchange, market_type) - len(lead))
    tail_len = limit - len(head)
    if tail_len < MIN_TAIL:
        raise ConfigError(f'client_id does not fit: {len(head)} + tail {MIN_TAIL} > {limit}',
                          exchange=ex_id or None, details={'tag': tag, 'max_len': limit})
    cid = lead + head + ''.join(secrets.choice(_B36) for _ in range(tail_len))
    pattern = CLIENT_ID_FORMATS.get(ex_id)
    if pattern is not None and not pattern.fullmatch(cid):  # pragma: no cover — the format above is already narrower
        raise ConfigError(f'client_id {cid!r} does not match the {ex_id} format', exchange=ex_id)
    return cid


def parse_client_id(client_id: Optional[str], *, tag: Optional[str] = None,
                    broker_prefixes: Iterable[str] = ()) -> ClientIdInfo:
    """Parses a client_id of any generation (see the module).

    ``tag`` — a custom tag (by default the ``d`` + 12 hex tag is looked for).
    ``broker_prefixes`` — Binance broker prefixes (``x-<ID>``), if the id is physical:
    the SDK's ``Order.client_id`` is already logical, the prefix is stripped by the transport.
    """
    if not client_id:
        return ClientIdInfo(client_id, 'none')
    cid = str(client_id)
    s = cid.removeprefix(GATE_TEXT_PREFIX)   # Gate: text "t-…" — our id follows the prefix
    for prefix in broker_prefixes:
        if prefix and s.lower().startswith(prefix.lower()):
            s = s[len(prefix):]
            break
    want = tag.lower().removeprefix('0x') if tag else None
    body = cloid_body(s)
    if body is not None:
        if want and body.startswith(want):
            return ClientIdInfo(cid, 'hyperliquid', want)
        m = BOT_TAG_RE.match(body)
        if not want and m:
            return ClientIdInfo(cid, 'hyperliquid', m.group())
        return ClientIdInfo(cid, 'foreign')
    low = s.lower()   # BingX returns ids in lowercase; our ids are always lowercase
    if want is not None:
        found = want if low.startswith(want) else None
    else:
        m = BOT_TAG_RE.match(low)
        found = m.group() if m else None
    if found:
        rest = low[len(found):]
        if not rest:
            return ClientIdInfo(cid, 'tagged', found)
        code = rest[0]
        if code in _HEX:
            return ClientIdInfo(cid, 'legacy', found, 'legacy_dca')
        role = CODE_ROLES.get(code)
        if role is None:
            return ClientIdInfo(cid, 'tagged', found)
        level = None
        if role in LEVEL_ROLES:
            digits = rest[1:1 + LEVEL_WIDTH]
            if len(digits) != LEVEL_WIDTH or any(c not in _B36 for c in digits):
                return ClientIdInfo(cid, 'tagged', found)
            level = int(digits, 36)
        return ClientIdInfo(cid, 'v2', found, role, level)
    m2 = LEGACY_ALGO_RE.fullmatch(s)
    if m2:
        return ClientIdInfo(cid, 'legacy_algo', None, 'tp' if m2.group(1) == 't' else 'sl', int(m2.group(2)))
    return ClientIdInfo(cid, 'foreign')


__all__ = [
    'BOT_TAG_RE', 'CODE_ROLES', 'ClientIdInfo', 'LEGACY_ALGO_RE', 'LEVEL_MAX', 'LEVEL_ROLES', 'MIN_TAIL',
    'ROLE_CODES', 'V2_MAX_LEN', 'bot_tag', 'make_client_id', 'parse_client_id',
]
