"""Local intent store: every order is recorded BEFORE sending.

::

    store = IntentStore('bot.sqlite', account='main')     # sqlite from stdlib, no dependencies
    ex = create_exchange('okx', ..., intents=store)
    report = await ex.recover()                            # at startup: reconcile with the exchange

Recording (opt-in; without a store Exchange works as before — one check at the recording point):

- ``pending`` — right before the placement network request (after the ``pre_submit`` hook and the journal):
  an order, a batch, a WS order, each TP/SL leg with a client_id, a retry during recovery;
- ``placed`` — the exchange accepted it (id known), ``final`` — the order is in a final status (filled,
  cancelled, expired; from the response, an SDK cancel or an :class:`~bytx.OrderTracker` update);
- ``rejected`` — explicit exchange rejection (no order, :func:`bytx.outcome.classify` → rejected);
- ``unknown`` — outcome unknown (:class:`~bytx.errors.OrderStatusUnknown`, network);
- ``abandoned`` — closed by recovery: no order on the exchange and it was not re-created.

Open states (reconciled by :func:`bytx.recovery.recover`): ``pending``, ``unknown``,
``placed``. Key — ``(namespace, client_id)``; namespace — exchange, market, mode
(live/demo/paper), the store's ``account`` and the API key fingerprint (a hash, no secrets): one file can serve
several accounts — intents of different keys are not mixed. Rows of schemas 1–4 (namespace without the key
fingerprint) are moved with :meth:`IntentStore.claim_legacy` (automatically when ``account`` is set). TP/SL legs
without a client_id (Bybit trading-stop) are not recorded.

Execution algorithms (``ex.algos``, schema 3): an algorithm start is a row in the ``algos`` table (id, kind, symbol,
side, size; state ``running``) BEFORE the first child order, its end — ``finished`` (the algorithm canceled its working
orders itself) or ``unclean`` (couldn't cancel, a child's outcome is unknown); the intent of every child order and
TP/SL leg carries ``algo_id``. The algorithm's progress (filled, schedule, levels) is not stored: after a restart
:func:`~bytx.recovery.recover` uses these records to cancel the working orders of unfinished algorithms, leaves
position protection in place and closes the record (``recovered``).

An algorithm's position protection plan (schema 4, column ``algos.plan``): bracket writes it to its record BEFORE the
entry is sent — TP/SL prices, trigger type, position side, size, leg client_ids (:class:`AlgoRecord`). If the process
died between the entry and the legs, :func:`~bytx.recovery.recover` (``protect=True``) places the missing legs from
the plan on the current position of the plan's side (this is position protection, not resuming the algorithm). Legs
placed by the algorithm itself, the algorithm canceled or protection failed — the plan is ``retired``: recovery places
no legs for it. Schema 5 — ``intents.plan_retired``: an entry's TP/SL plan closed for recovery.

One process per store file: a file lock ``<db>.lock`` (POSIX); another process gets :class:`~bytx.errors.ConfigError`
on open. Rolling back to an SDK version with an older schema — ``tools/intents_downgrade.py``.

Durability: WAL; ``durable=True`` (default) — ``synchronous=FULL``, a write survives both
a process kill and a power loss; ``durable=False`` — ``NORMAL``: survives a process kill
(data in the OS cache), faster. Calls are synchronous (local file, a few ms per write).
"""
from __future__ import annotations

import contextlib
import contextvars
import json
import math
import sqlite3
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Iterable, Iterator, Optional, Union

if TYPE_CHECKING:
    from bytx.exchange import Exchange
    from bytx.types import Order

# 2: trigger_by column (TP/SL leg trigger type); 3: algo_id of intents, algos table; 4: algos.plan (protection
# plan); 5: intents.plan_retired
SCHEMA_VERSION = 5
OPEN_STATES = frozenset({'pending', 'unknown', 'placed'})
STATES = frozenset({'pending', 'unknown', 'placed', 'final', 'rejected', 'abandoned'})
_TERMINAL_STATUS = frozenset({'closed', 'canceled', 'expired', 'rejected'})
_KIND_ROLES = {'take_profit': 'tp', 'stop_loss': 'sl', 'tp_sl': 'tp'}
#: algorithm record states: ``running`` — running (or the process died), ``finished`` — finished, working orders
#: canceled by the algorithm itself, ``unclean`` — finished, but working orders may remain (cancel failed, a child's
#: outcome is unknown), ``recovered`` — working orders canceled by recovery
ALGO_STATES = frozenset({'running', 'finished', 'unclean', 'recovered'})
OPEN_ALGO_STATES = frozenset({'running', 'unclean'})

# client_id of the entry whose TP/SL are being placed now (Exchange._protect) — parent of the legs
_PARENT: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar('bytx_intent_parent', default=None)
# entry protection plan that the caller places separately after the entry (open_position, separate path)
_PLAN: contextvars.ContextVar[Optional[dict]] = contextvars.ContextVar('bytx_intent_plan', default=None)
# execution algorithm whose child order / leg is being placed now (ExecAlgo._bind)
_ALGO: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar('bytx_intent_algo', default=None)

_COLUMNS = ('namespace', 'client_id', 'exchange', 'market_type', 'symbol', 'kind', 'side', 'type', 'amount',
            'price', 'trigger_price', 'reduce_only', 'params', 'tp_sl', 'role', 'position_id', 'level_id',
            'parent', 'state', 'order_id', 'status', 'filled', 'protected', 'attempts', 'note', 'created_ms',
            'updated_ms', 'trigger_by', 'algo_id', 'plan_retired')
_ALGO_COLUMNS = ('namespace', 'algo_id', 'kind', 'symbol', 'side', 'size', 'state', 'status', 'reason', 'note',
                 'created_ms', 'updated_ms', 'plan')

# request fields: match — a retry of the same order (re-creation), no match — a different order with the same client_id
_REQUEST_COLUMNS = ('symbol', 'kind', 'side', 'type', 'amount', 'price', 'trigger_price', 'reduce_only', 'params')

_DDL = f"""
CREATE TABLE IF NOT EXISTS intents (
    namespace TEXT NOT NULL, client_id TEXT NOT NULL, exchange TEXT, market_type TEXT, symbol TEXT,
    kind TEXT, side TEXT, type TEXT, amount REAL, price REAL, trigger_price REAL, reduce_only INTEGER,
    params TEXT, tp_sl TEXT, role TEXT, position_id TEXT, level_id INTEGER, parent TEXT,
    state TEXT NOT NULL, order_id TEXT, status TEXT, filled REAL, protected INTEGER NOT NULL DEFAULT 0,
    attempts INTEGER NOT NULL DEFAULT 1, note TEXT, created_ms INTEGER, updated_ms INTEGER, trigger_by TEXT,
    algo_id TEXT, plan_retired TEXT, PRIMARY KEY (namespace, client_id));
CREATE INDEX IF NOT EXISTS intents_state ON intents (namespace, state);
CREATE INDEX IF NOT EXISTS intents_order ON intents (namespace, order_id);
CREATE INDEX IF NOT EXISTS intents_parent ON intents (namespace, parent);
CREATE INDEX IF NOT EXISTS intents_algo ON intents (namespace, algo_id);
CREATE TABLE IF NOT EXISTS algos (
    namespace TEXT NOT NULL, algo_id TEXT NOT NULL, kind TEXT, symbol TEXT, side TEXT, size REAL,
    state TEXT NOT NULL, status TEXT, reason TEXT, note TEXT, created_ms INTEGER, updated_ms INTEGER, plan TEXT,
    PRIMARY KEY (namespace, algo_id));
CREATE INDEX IF NOT EXISTS algos_state ON algos (namespace, state);
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
INSERT OR IGNORE INTO meta (key, value) VALUES ('schema', '{SCHEMA_VERSION}');
"""


@dataclass(frozen=True, slots=True)
class Intent:
    """Intent — an order recorded before sending.

    ``amount`` — contracts (as sent to the exchange), ``params`` — ccxt/exchange request params (for
    retrying with the same request), ``tp_sl`` — TP/SL placed as separate orders after the
    entry (Binance/HL), ``protected`` — they have been placed. ``kind``: 'order' / 'take_profit' /
    'stop_loss' / 'tp_sl'; ``parent`` — the entry's client_id for a TP/SL leg; ``role`` /
    ``position_id`` / ``level_id`` — position journal binding (``journal.bind``); ``trigger_by`` —
    TP/SL leg trigger type ('mark' / 'last'; re-created with the same), None for an entry; ``algo_id`` — the
    execution algorithm (``ex.algos``) whose child order or leg this is, otherwise None.

    Entry protection plan (``tp_sl``): ``stop_loss`` / ``take_profit`` — prices, ``trigger_by``; for
    :meth:`Exchange.open_position` also ``mode`` ('attached' — SL goes in the entry request,
    'separate' — as a separate order afterwards), ``side`` — position side, ``stop_client_id`` —
    the stop's client_id if it is sent to the exchange (otherwise None: the stop is looked up by kind, side and price).
    ``plan_retired`` (schema 5) — the plan is closed, recover places no legs for it: 'protection_failed'
    (ProtectionNotPlaced was reported to the caller), 'expired' (older than ``protect_max_age``), 'superseded' (there
    is a newer entry of the same side).
    """

    namespace: str
    client_id: str
    exchange: str
    market_type: str
    symbol: str
    kind: str
    state: str
    side: Optional[str] = None
    type: Optional[str] = None
    amount: Optional[float] = None
    price: Optional[float] = None
    trigger_price: Optional[float] = None
    reduce_only: bool = False
    params: dict = field(default_factory=dict)
    tp_sl: Optional[dict] = None
    role: Optional[str] = None
    position_id: Optional[str] = None
    level_id: Optional[int] = None
    parent: Optional[str] = None
    order_id: Optional[str] = None
    status: Optional[str] = None
    filled: Optional[float] = None
    protected: bool = False
    attempts: int = 1
    note: Optional[str] = None
    created_ms: int = 0
    updated_ms: int = 0
    trigger_by: Optional[str] = None
    algo_id: Optional[str] = None
    plan_retired: Optional[str] = None

    @property
    def is_open(self) -> bool:
        """Outcome not closed: ``pending`` / ``unknown`` / ``placed``."""
        return self.state in OPEN_STATES

    @property
    def resubmittable(self) -> bool:
        """Can be retried with the same request: params were saved losslessly."""
        return _OPAQUE not in self.params and self.type is not None and self.side is not None \
            and self.amount is not None

    @property
    def position_side(self) -> Optional[str]:
        """Position side: an opening buy and a closing sell — long."""
        if self.side not in ('buy', 'sell'):
            return None
        return 'long' if (self.side == 'buy') != self.reduce_only else 'short'

    def legs_plan(self) -> Optional[dict]:
        """TP/SL prices placed as separate orders after the entry (``request['tp_sl']`` form);
        an entry with an attached SL (``mode='attached'``) — None: the SL goes in the entry's own params."""
        plan = self.tp_sl or {}
        if plan.get('mode') == 'attached':
            return None
        legs = {k: plan.get(k) for k in ('take_profit', 'stop_loss')}
        return legs if any(v is not None for v in legs.values()) else None

    def request(self) -> dict:
        """Placement request in Exchange form (for a retry with the same client_id and params)."""
        return {'symbol': self.symbol, 'type': self.type, 'side': self.side, 'amount': self.amount,
                'price': self.price, 'params': dict(self.params), 'client_id': self.client_id,
                'reduce_only': self.reduce_only, 'tp_sl': self.legs_plan(),
                'journal_role': self.role, 'submitted_at': self.created_ms}


_OPAQUE = '__bytx_opaque__'   # params had a non-serializable value — retrying the request is impossible


@dataclass(frozen=True, slots=True)
class AlgoRecord:
    """Record of an execution algorithm (``ex.algos``) — without progress: what was started and how it ended.

    ``state`` — :data:`ALGO_STATES`; ``status`` / ``reason`` — the algorithm's result (``AlgoResult``), None while it
    runs; ``size`` — the requested size (base; None for trailing without ``size``); ``note`` — what recovery did.

    ``plan`` — the position protection plan written by the algorithm before its first child order (bracket; None for
    the others and for schema 3 records): ``side`` — position side ('long' / 'short'), ``take_profit`` /
    ``stop_loss`` — trigger prices, ``trigger_by`` — 'mark' / 'last' (None — the exchange default), ``size`` — the
    algorithm size (base; legs are placed for the size filled by the entry, no more than the position),
    ``take_profit_client_id`` / ``stop_loss_client_id`` — client_ids of the legs recovery places (the same on every
    recover(): looked up before placing); ``retired`` — the algorithm closed the plan itself: 'protected' (legs
    placed), a ``cancel()`` reason, 'protection_failed' / 'keep' (``on_protect_fail``) — no legs are placed for it.
    """

    namespace: str
    algo_id: str
    kind: str
    symbol: str
    side: Optional[str]
    size: Optional[float]
    state: str
    status: Optional[str] = None
    reason: Optional[str] = None
    note: Optional[str] = None
    created_ms: int = 0
    updated_ms: int = 0
    plan: Optional[dict] = None

    @property
    def is_open(self) -> bool:
        """The algorithm didn't finish by itself (``running``) or left working orders (``unclean``)."""
        return self.state in OPEN_ALGO_STATES


def _json(value: Any) -> Optional[str]:
    if value is None:
        return None
    opaque: list[str] = []

    def fallback(v: Any) -> Any:
        if isinstance(v, (set, frozenset, tuple)):
            return list(v)
        opaque.append(type(v).__name__)
        return repr(v)

    text = json.dumps(value, sort_keys=True, separators=(',', ':'), default=fallback)
    if opaque and isinstance(value, dict):
        text = json.dumps({**json.loads(text), _OPAQUE: sorted(set(opaque))}, sort_keys=True,
                          separators=(',', ':'))
    return text


def _num(value: Any) -> Optional[float]:
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def legacy_namespace_of(ex: Any, account: str = '') -> str:
    """Namespace of schemas 1–4: ``exchange:market:mode:account`` — without the key fingerprint."""
    mode = 'paper' if getattr(ex, 'is_paper', False) else ('demo' if getattr(ex, 'demo', False) else 'live')
    return f'{ex.name}:{ex.market_type}:{mode}:{account}'


def key_fingerprint(ex: Any) -> str:
    """Exchange API key fingerprint (a hash, :func:`bytx.hubs.account_fingerprint`, no secrets); paper without a key
    — 'paper', no key — 'nokey'."""
    cc = getattr(ex, '_ex', None) or getattr(ex, 'ccxt', None)
    creds = {'api_key': getattr(cc, 'apiKey', None) or None, 'wallet_address': getattr(cc, 'walletAddress', None) or None}
    if not any(creds.values()):
        return 'paper' if getattr(ex, 'is_paper', False) else 'nokey'
    from bytx.hubs import account_fingerprint
    return account_fingerprint(str(ex.name), bool(getattr(ex, 'demo', False)), creds)


def namespace_of(ex: Any, account: str = '') -> str:
    """Exchange intent namespace: ``exchange:market:mode:account:key fingerprint`` (mode — live/demo/paper). The
    fingerprint tells API keys apart: AccountPool(intents=store) and a shared file of several accounts do not mix
    intents — recover of account A does not touch the intents of B."""
    return f'{legacy_namespace_of(ex, account)}:{key_fingerprint(ex)}'


# One process per store file: a file lock <db>.lock — per process, shared by all IntentStore objects of this process on
# the same file (a counter); another process gets ConfigError on open.
_LOCKS: dict[str, list] = {}       # absolute path → [fd, number of open IntentStore objects]


def _acquire_lock(path: str) -> Optional[str]:
    import os
    if path == ':memory:' or path.startswith('file:'):
        return None
    try:
        import fcntl
    except ImportError:            # Windows: no lock — one process per file is up to the deployment (README)
        return None
    key = os.path.abspath(path)
    held = _LOCKS.get(key)
    if held is not None:
        held[1] += 1
        return key
    fd = os.open(key + '.lock', os.O_RDWR | os.O_CREAT, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        os.close(fd)
        from bytx.errors import ConfigError
        raise ConfigError(f'intent store {path} is open in another process (lock {key}.lock): one process per file — '
                          f'deploy stop-first, different processes — different files', details={'path': path})
    _LOCKS[key] = [fd, 1]
    return key


def _release_lock(key: Optional[str]) -> None:
    import os
    held = _LOCKS.get(key) if key else None
    if held is None:
        return
    held[1] -= 1
    if held[1] <= 0:
        del _LOCKS[key]  # type: ignore[arg-type]
        os.close(held[0])          # closing the descriptor releases the flock


def now_ms(ex: Any) -> int:
    """Exchange time: the paper clock (virtual in replay and tests), otherwise wall clock."""
    clock = getattr(ex, 'clock', None)
    if clock is not None and hasattr(clock, 'now_ms'):
        return int(clock.now_ms())
    return int(time.time() * 1000)


class IntentStore:
    """sqlite intent store (see module).

    ``path`` — database file (``':memory:'`` — tests only: doesn't survive the process); ``account`` —
    part of the namespace (several accounts in one file); ``durable`` — ``synchronous=FULL``.
    """

    def __init__(self, path: Union[str, Any] = ':memory:', *, account: str = '', durable: bool = True) -> None:
        self.path = str(path)
        self.account = str(account)
        self._lock_key: Optional[str] = None
        self._lock_key = _acquire_lock(self.path)
        # algorithms running with this store in this process: recovery doesn't touch them (the live algorithm
        # manages their orders); a new process opens the store anew — the set is empty
        self.running_algos: set[str] = set()
        self._claimed: set[str] = set()
        self._db = sqlite3.connect(self.path, isolation_level=None, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        if self.path != ':memory:':
            self._db.execute('PRAGMA journal_mode=WAL')
        self._db.execute(f"PRAGMA synchronous={'FULL' if durable else 'NORMAL'}")
        row = self._schema()
        if row is not None and row > SCHEMA_VERSION:
            from bytx.errors import ConfigError
            self._db.close()
            _release_lock(self._lock_key)
            raise ConfigError(f'intent store {self.path}: schema {row} is newer than supported '
                              f'{SCHEMA_VERSION} — upgrade bytx (rolling back the SDK: tools/intents_downgrade.py)')
        if row is not None and row < SCHEMA_VERSION:
            self._migrate()
        self._db.executescript(_DDL)

    def _schema(self) -> Optional[int]:
        try:
            row = self._db.execute("SELECT value FROM meta WHERE key='schema'").fetchone()
        except sqlite3.OperationalError:   # new database: no tables yet
            return None
        return int(row[0]) if row is not None else None

    def _migrate(self) -> None:
        """Schema 1/2/3/4 → 5 in place, in one transaction (another process may have migrated first —
        the columns are checked inside): ``trigger_by`` (2) and ``algo_id`` (3) on intents, ``plan`` (4) on algorithm
        records, ``plan_retired`` (5) on intents, existing rows — NULL (no plan — the previous recovery); the ``algos``
        table of schemas 1/2 is created by the DDL after the migration (already with ``plan``). The migration does not
        rename row namespaces (it does not know the key fingerprint) — see :meth:`IntentStore.claim_legacy`."""
        self._db.execute('BEGIN IMMEDIATE')
        try:
            cols = {r[1] for r in self._db.execute('PRAGMA table_info(intents)')}
            if 'trigger_by' not in cols:
                self._db.execute('ALTER TABLE intents ADD COLUMN trigger_by TEXT')
            if 'algo_id' not in cols:
                self._db.execute('ALTER TABLE intents ADD COLUMN algo_id TEXT')
            if 'plan_retired' not in cols:
                self._db.execute('ALTER TABLE intents ADD COLUMN plan_retired TEXT')
            algo_cols = {r[1] for r in self._db.execute('PRAGMA table_info(algos)')}
            if algo_cols and 'plan' not in algo_cols:
                self._db.execute('ALTER TABLE algos ADD COLUMN plan TEXT')
            self._db.execute("UPDATE meta SET value=? WHERE key='schema'", (str(SCHEMA_VERSION),))
            self._db.execute('COMMIT')
        except BaseException:
            self._db.execute('ROLLBACK')
            raise

    # ─── public API ──────────────────────────────────────────────────────

    def namespace(self, ex: Any) -> str:
        ns = namespace_of(ex, self.account)
        if self.account and ns not in self._claimed:
            # the user separated accounts (account=…): rows of schemas 1–4 of this account are theirs, claimed once
            self._claimed.add(ns)
            self.claim_legacy(ex)
        return ns

    def claim_legacy(self, ex: Any, namespace: Optional[str] = None) -> int:
        """Move intents and algorithm records from the namespace without the key fingerprint (schemas 1–4,
        :func:`legacy_namespace_of`) or from ``namespace`` (e.g. of the previous key after a key change) into the
        namespace of Exchange ``ex``. Returns the number of moved intents; a client_id already taken in the target
        namespace is not moved. Recorded in meta per source."""
        src = namespace or legacy_namespace_of(ex, self.account)
        dst = namespace_of(ex, self.account)
        if src == dst:
            return 0
        self._db.execute('BEGIN IMMEDIATE')
        try:
            cur = self._db.execute('UPDATE OR IGNORE intents SET namespace=? WHERE namespace=?', (dst, src))
            self._db.execute('UPDATE OR IGNORE algos SET namespace=? WHERE namespace=?', (dst, src))
            self._db.execute('INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)', (f'claimed:{src}', dst))
            self._db.execute('COMMIT')
        except BaseException:
            self._db.execute('ROLLBACK')
            raise
        return int(cur.rowcount or 0)

    def legacy_intents(self, ex: Any) -> list[Intent]:
        """Open intents of schemas 1–4 of this Exchange without the key fingerprint (not claimed by
        :meth:`claim_legacy`)."""
        src = legacy_namespace_of(ex, self.account)
        st = sorted(OPEN_STATES)
        rows = self._db.execute(f"SELECT * FROM intents WHERE namespace=? AND state IN ({','.join('?' * len(st))}) "
                                'ORDER BY created_ms, rowid', (src, *st)).fetchall()
        return [_intent(r) for r in rows]

    def _retire_plan(self, ex: Any, client_id: str, reason: str) -> None:
        """The entry's protection plan is closed (schema 5): recover places no legs for it."""
        self._db.execute('UPDATE intents SET plan_retired=?, updated_ms=? WHERE namespace=? AND client_id=?',
                         (reason, now_ms(ex), self.namespace(ex), str(client_id)))

    def get(self, ex: Any, client_id: str) -> Optional[Intent]:
        """Intent by client_id (in the namespace of Exchange ``ex``)."""
        row = self._db.execute('SELECT * FROM intents WHERE namespace=? AND client_id=?',
                               (self.namespace(ex), str(client_id))).fetchone()
        return _intent(row) if row is not None else None

    def intents(self, ex: Any, *, states: Optional[Iterable[str]] = None,
                symbol: Optional[str] = None) -> list[Intent]:
        """Intents of Exchange ``ex`` (all by default) by recording time."""
        sql = 'SELECT * FROM intents WHERE namespace=?'
        args: list[Any] = [self.namespace(ex)]
        if states is not None:
            st = sorted(set(states))
            sql += f" AND state IN ({','.join('?' * len(st))})"
            args += st
        if symbol is not None:
            sql += ' AND symbol=?'
            args.append(symbol)
        rows = self._db.execute(sql + ' ORDER BY created_ms, rowid', args).fetchall()
        return [_intent(r) for r in rows]

    def open_intents(self, ex: Any) -> list[Intent]:
        """Intents with an unclosed outcome (reconciled by recovery)."""
        return self.intents(ex, states=OPEN_STATES)

    def children(self, ex: Any, parent: str) -> list[Intent]:
        """TP/SL legs of entry ``parent`` (client_id)."""
        rows = self._db.execute('SELECT * FROM intents WHERE namespace=? AND parent=? ORDER BY created_ms, rowid',
                                (self.namespace(ex), parent)).fetchall()
        return [_intent(r) for r in rows]

    def algos(self, ex: Any, *, states: Optional[Iterable[str]] = None) -> list[AlgoRecord]:
        """Execution algorithm records of Exchange ``ex`` (all by default) by start time."""
        sql = 'SELECT * FROM algos WHERE namespace=?'
        args: list[Any] = [self.namespace(ex)]
        if states is not None:
            st = sorted(set(states))
            sql += f" AND state IN ({','.join('?' * len(st))})"
            args += st
        rows = self._db.execute(sql + ' ORDER BY created_ms, rowid', args).fetchall()
        return [_algo(r) for r in rows]

    def algo_record(self, ex: Any, algo_id: str) -> Optional[AlgoRecord]:
        """Algorithm record by id (in the namespace of Exchange ``ex``)."""
        row = self._db.execute('SELECT * FROM algos WHERE namespace=? AND algo_id=?',
                               (self.namespace(ex), str(algo_id))).fetchone()
        return _algo(row) if row is not None else None

    def algo_children(self, ex: Any, algo_id: str) -> list[Intent]:
        """Intents of the child orders and TP/SL legs of algorithm ``algo_id``."""
        rows = self._db.execute('SELECT * FROM intents WHERE namespace=? AND algo_id=? ORDER BY created_ms, rowid',
                                (self.namespace(ex), str(algo_id))).fetchall()
        return [_intent(r) for r in rows]

    def mark_algo(self, ex: Any, algo_id: str, state: str, *, note: Optional[str] = None) -> None:
        """Changes the algorithm record state (``state`` from :data:`ALGO_STATES`)."""
        if state not in ALGO_STATES:
            from bytx.errors import ConfigError
            raise ConfigError(f'algorithm state: one of {sorted(ALGO_STATES)}, not {state!r}')
        self._db.execute('UPDATE algos SET state=?, note=COALESCE(?, note), updated_ms=? '
                         'WHERE namespace=? AND algo_id=?', (state, note, now_ms(ex), self.namespace(ex), str(algo_id)))

    def mark(self, ex: Any, client_id: str, state: str, *, order_id: Optional[str] = None,
             status: Optional[str] = None, filled: Optional[float] = None, note: Optional[str] = None) -> None:
        """Changes the intent state (``state`` from :data:`STATES`); None fields are left unchanged."""
        if state not in STATES:
            from bytx.errors import ConfigError
            raise ConfigError(f'intent state: one of {sorted(STATES)}, not {state!r}')
        self._db.execute(
            'UPDATE intents SET state=?, order_id=COALESCE(?, order_id), status=COALESCE(?, status), '
            'filled=COALESCE(?, filled), note=COALESCE(?, note), updated_ms=? WHERE namespace=? AND client_id=?',
            (state, order_id, status, filled, note, now_ms(ex), self.namespace(ex), str(client_id)))

    def adopt(self, ex: Any, order: 'Order', *, kind: str = 'order', role: Optional[str] = None,
              position_id: Optional[str] = None, note: str = 'adopted', parent: Optional[str] = None,
              algo_id: Optional[str] = None) -> None:
        """Records a live order without an intent (found during recovery by the bot tag; ``parent`` —
        the entry whose protection it provides; ``algo_id`` — the algorithm whose leg it is)."""
        if not order.client_id:
            return
        ts = now_ms(ex)
        self._upsert({
            'namespace': self.namespace(ex), 'client_id': order.client_id, 'exchange': ex.name,
            'market_type': ex.market_type, 'symbol': order.symbol, 'kind': kind, 'side': order.side,
            'type': order.type, 'amount': _num(order.contracts), 'price': _num(order.price),
            'trigger_price': _num(order.trigger_price), 'reduce_only': int(bool(order.reduce_only)),
            'params': '{}', 'tp_sl': None, 'role': role, 'position_id': position_id, 'level_id': None,
            'parent': parent, 'state': _state_of(order), 'order_id': order.id, 'status': order.status,
            'filled': _num(order.filled_contracts), 'protected': 1, 'attempts': 0, 'note': note,
            'created_ms': order.timestamp or ts, 'updated_ms': ts, 'trigger_by': None, 'algo_id': algo_id, 'plan_retired': None},
            replace=False)

    def prune(self, ex: Any, *, older_than_ms: int) -> int:
        """Deletes closed intents (final / rejected / abandoned) and records of finished algorithms
        (finished / recovered) updated earlier than ``older_than_ms``. Returns the number of deleted rows."""
        cur = self._db.execute(
            "DELETE FROM intents WHERE namespace=? AND state IN ('final','rejected','abandoned') AND updated_ms<?",
            (self.namespace(ex), int(older_than_ms)))
        algos = self._db.execute(
            "DELETE FROM algos WHERE namespace=? AND state IN ('finished','recovered') AND updated_ms<?",
            (self.namespace(ex), int(older_than_ms)))
        return int(cur.rowcount or 0) + int(algos.rowcount or 0)

    def close(self) -> None:
        self._db.close()
        _release_lock(self._lock_key)
        self._lock_key = None

    # ─── Exchange recording points ──────────────────────────────────────────────

    @staticmethod
    @contextlib.contextmanager
    def parent(client_id: Optional[str]) -> Iterator[None]:
        """Context: TP/SL legs inside are children of entry ``client_id``."""
        token = _PARENT.set(client_id)
        try:
            yield
        finally:
            _PARENT.reset(token)

    @staticmethod
    @contextlib.contextmanager
    def algo(algo_id: Optional[str]) -> Iterator[None]:
        """Context: orders and TP/SL legs inside are children of algorithm ``algo_id`` (the ``algo_id`` column)."""
        token = _ALGO.set(algo_id)
        try:
            yield
        finally:
            _ALGO.reset(token)

    @staticmethod
    @contextlib.contextmanager
    def protection(plan: Optional[dict]) -> Iterator[None]:
        """Context: the entry protection plan inside (``stop_loss`` / ``take_profit`` / ``trigger_by``; for
        open_position also ``mode`` / ``side`` / ``stop_client_id``, see :class:`Intent`) is written to
        its intent's ``tp_sl`` BEFORE sending. If the process crashes between the entry and the stop,
        :func:`~bytx.recovery.recover` reconciles the exchange TP/SL and places missing legs. After
        placement — :meth:`parent` for legs and ``_on_protected`` for the entry, as in place_order."""
        token = _PLAN.set({k: v for k, v in (plan or {}).items() if v is not None} or None)
        try:
            yield
        finally:
            _PLAN.reset(token)

    def _on_intent(self, ex: 'Exchange', request: dict, kind: str, extra: dict) -> None:
        cid = request.get('client_id')
        if not cid:
            return
        role, position_id, level_id = request.get('journal_role') or _KIND_ROLES.get(kind), None, None
        if ex.journal is not None:
            from bytx.journal.journal import current_binding
            b = current_binding()
            role, position_id, level_id = b.role or role, b.position_id, b.level_id
        tp_sl = (request.get('tp_sl') or _PLAN.get()) if kind == 'order' else None
        if tp_sl and tp_sl.get('mode') == 'attached' and request.get('attach_client_id') \
                and not tp_sl.get('stop_client_id'):
            # OKX attachAlgoClOrdId: the attached algo id (TP+SL pair or a single SL) is sent to the exchange
            tp_sl = {**tp_sl, 'stop_client_id': request['attach_client_id']}
        ts = now_ms(ex)
        self._write_intent({
            'namespace': self.namespace(ex), 'client_id': str(cid), 'exchange': ex.name,
            'market_type': ex.market_type, 'symbol': request.get('symbol'), 'kind': kind,
            'side': request.get('side'), 'type': request.get('type'), 'amount': _num(request.get('amount')),
            'price': _num(request.get('price')), 'trigger_price': _num(extra.get('trigger_price')),
            'reduce_only': int(bool(request.get('reduce_only'))), 'params': _json(request.get('params') or {}),
            'tp_sl': _json(tp_sl) if tp_sl else None, 'role': role, 'position_id': position_id,
            'level_id': level_id, 'parent': _PARENT.get() if kind != 'order' else None, 'state': 'pending',
            'order_id': None, 'status': None, 'filled': None, 'protected': 0, 'attempts': 1, 'note': None,
            'created_ms': ts, 'updated_ms': ts,
            'trigger_by': extra.get('trigger_by') if kind != 'order' else None, 'algo_id': _ALGO.get(),
            'plan_retired': None})

    def _on_algo_started(self, ex: 'Exchange', algo_id: str, kind: str, symbol: str, side: Optional[str],
                         size: Optional[float]) -> None:
        """Algorithm started — a ``running`` record before the first child order."""
        ts = now_ms(ex)
        row = {'namespace': self.namespace(ex), 'algo_id': str(algo_id), 'kind': kind, 'symbol': symbol, 'side': side,
               'size': _num(size), 'state': 'running', 'status': None, 'reason': None, 'note': None,
               'created_ms': ts, 'updated_ms': ts, 'plan': None}
        cols = ','.join(_ALGO_COLUMNS)
        self._db.execute(f"INSERT OR REPLACE INTO algos ({cols}) VALUES ({','.join('?' * len(_ALGO_COLUMNS))})",
                         tuple(row[c] for c in _ALGO_COLUMNS))
        self.running_algos.add(str(algo_id))

    def _on_algo_plan(self, ex: 'Exchange', algo_id: str, plan: Optional[dict]) -> None:
        """The algorithm's position protection plan (see :class:`AlgoRecord`) — before the first child order; again —
        with ``retired`` (the algorithm handles the protection itself: recover() places no legs)."""
        self._db.execute('UPDATE algos SET plan=?, updated_ms=? WHERE namespace=? AND algo_id=?',
                         (_json(plan), now_ms(ex), self.namespace(ex), str(algo_id)))

    def _on_algo_finished(self, ex: 'Exchange', algo_id: str, status: str, reason: Optional[str], *,
                          clean: bool, note: Optional[str] = None) -> None:
        """Algorithm finished: ``finished`` (working orders canceled) or ``unclean`` (they may remain)."""
        self.running_algos.discard(str(algo_id))
        self._db.execute('UPDATE algos SET state=?, status=?, reason=?, note=COALESCE(?, note), updated_ms=? '
                         "WHERE namespace=? AND algo_id=? AND state='running'",
                         ('finished' if clean else 'unclean', status, reason, note, now_ms(ex), self.namespace(ex),
                          str(algo_id)))

    def _on_placed(self, ex: 'Exchange', order: 'Order', client_id: Optional[str] = None) -> None:
        cid = client_id or order.client_id
        if cid:
            self.mark(ex, cid, _state_of(order), order_id=order.id or None, status=order.status,
                      filled=_num(order.filled_contracts))

    def _on_failed(self, ex: 'Exchange', request: dict, error: BaseException) -> None:
        cid = request.get('client_id')
        if not cid:
            return
        from bytx.outcome import classify
        definite = classify(ex.id, error, has_client_id=True) == 'rejected'
        self.mark(ex, cid, 'rejected' if definite else 'unknown', note=f'{type(error).__name__}: {error}'[:300])

    def _on_update(self, ex: 'Exchange', order: 'Order') -> None:
        """Order update (SDK cancel, tracker): only for known intents."""
        if not order.status:
            return
        state = _state_of(order)
        args = (state, order.status, _num(order.filled_contracts), now_ms(ex), self.namespace(ex))
        if order.client_id:
            self._db.execute(
                "UPDATE intents SET state=?, status=?, filled=COALESCE(?, filled), updated_ms=?, "
                "order_id=COALESCE(order_id, ?) WHERE namespace=? AND client_id=? AND state IN ('pending','unknown','placed')",
                (*args[:4], order.id or None, args[4], order.client_id))
        elif order.id:
            self._db.execute(
                "UPDATE intents SET state=?, status=?, filled=COALESCE(?, filled), updated_ms=? "
                "WHERE namespace=? AND order_id=? AND state IN ('pending','unknown','placed')", (*args, order.id))

    def _on_replaced(self, ex: 'Exchange', old_order_id: str, old_client_id: str, order: 'Order') -> None:
        """amend via cancel + new order (new id and client_id): the previous intent is closed
        (``final``, ``canceled``, note ``amended → …``), the new one keeps the same binding (kind, role,
        position, level, parent, protection plan) under the new order's client_id and id: ``recover``
        sees it as its own. No previous intent (order not via the SDK) — nothing is written."""
        if not order.client_id:
            return
        ns = self.namespace(ex)
        old = self._db.execute('SELECT * FROM intents WHERE namespace=? AND client_id=?',
                               (ns, str(old_client_id))).fetchone()
        if old is None:
            old = self._db.execute('SELECT * FROM intents WHERE namespace=? AND order_id=?',
                                   (ns, str(old_order_id))).fetchone()
        if old is None or old['client_id'] == order.client_id:
            return
        ts = now_ms(ex)
        row = {c: old[c] for c in _COLUMNS}
        row.update({
            'client_id': order.client_id, 'state': _state_of(order), 'order_id': order.id or None,
            'status': order.status, 'filled': _num(order.filled_contracts),
            'amount': _num(order.contracts) if order.contracts is not None else old['amount'],
            'price': _num(order.price) if order.price is not None else old['price'],
            'attempts': 1, 'note': f"amended: replaced {old['client_id']} (id {old_order_id})",
            'created_ms': ts, 'updated_ms': ts})
        self._upsert(row, replace=False)
        self.mark(ex, old['client_id'], 'final', status='canceled',
                  note=f'amended: replaced by {order.client_id} (id {order.id})')

    def _set_plan(self, ex: 'Exchange', client_id: str, plan: dict) -> None:
        """Protection plan of an already sent entry (open_position: the attached SL failed) — before
        placing the legs: if the process crashes, recover() will place them."""
        clean = {k: v for k, v in plan.items() if v is not None}
        self._db.execute('UPDATE intents SET tp_sl=?, protected=0, updated_ms=? WHERE namespace=? AND client_id=?',
                         (_json(clean) if clean else None, now_ms(ex), self.namespace(ex), client_id))

    def _on_protected(self, ex: 'Exchange', client_id: str) -> None:
        self._db.execute('UPDATE intents SET protected=1, updated_ms=? WHERE namespace=? AND client_id=?',
                         (now_ms(ex), self.namespace(ex), client_id))

    # ─── internal ─────────────────────────────────────────────────────────

    def _write_intent(self, row: dict) -> None:
        """New intent; client_id already exists: the previous one is open and the request is the same (retry during
        recovery) — state reset, attempt counter; the previous one is closed — full overwrite (a new order with the same
        client_id: the previous entry's protection plan, protected, filled are not carried over); open with a different
        request — ConfigError before any network call."""
        cols = ','.join(_COLUMNS)
        marks = ','.join('?' * len(_COLUMNS))
        cur = self._db.execute(f'INSERT INTO intents ({cols}) VALUES ({marks}) '
                               'ON CONFLICT(namespace, client_id) DO NOTHING', tuple(row[c] for c in _COLUMNS))
        if cur.rowcount:
            return
        old = self._db.execute('SELECT * FROM intents WHERE namespace=? AND client_id=?',
                               (row['namespace'], row['client_id'])).fetchone()
        if old is not None and old['state'] in OPEN_STATES and all(old[c] == row[c] for c in _REQUEST_COLUMNS):
            self._upsert(row, replace=True)
            return
        if old is not None and old['state'] in OPEN_STATES:
            from bytx.errors import ConfigError
            raise ConfigError(f"client_id {row['client_id']} already belongs to an intent in state {old['state']} with a different "
                              f'request: the previous order outcome is not closed — a new client_id is required',
                              details={'client_id': row['client_id'], 'state': old['state']})
        # a closed intent — full overwrite; legs of the previous entry are re-parented to "cid#created_ms" of the
        # previous record: legs of the new entry do not see them, the history of the previous entry is kept
        self._db.execute('BEGIN IMMEDIATE')
        try:
            if old is not None:
                self._db.execute('UPDATE intents SET parent=? WHERE namespace=? AND parent=?',
                                 (f"{row['client_id']}#{old['created_ms']}", row['namespace'], row['client_id']))
            self._db.execute(f'INSERT OR REPLACE INTO intents ({cols}) VALUES ({marks})',
                             tuple(row[c] for c in _COLUMNS))
            self._db.execute('COMMIT')
        except BaseException:
            self._db.execute('ROLLBACK')
            raise

    def _upsert(self, row: dict, *, replace: bool) -> None:
        cols = ','.join(_COLUMNS)
        marks = ','.join('?' * len(_COLUMNS))
        if replace:
            # retry of the same client_id (re-creation): state reset, first-record time and counter
            conflict = ('ON CONFLICT(namespace, client_id) DO UPDATE SET state=excluded.state, '
                        'order_id=NULL, status=NULL, attempts=intents.attempts+1, updated_ms=excluded.updated_ms, '
                        'note=NULL')
        else:
            conflict = 'ON CONFLICT(namespace, client_id) DO NOTHING'
        self._db.execute(f'INSERT INTO intents ({cols}) VALUES ({marks}) {conflict}',
                         tuple(row[c] for c in _COLUMNS))


def _state_of(order: 'Order') -> str:
    return 'final' if order.status in _TERMINAL_STATUS else 'placed'


def _intent(row: sqlite3.Row) -> Intent:
    return Intent(
        namespace=row['namespace'], client_id=row['client_id'], exchange=row['exchange'],
        market_type=row['market_type'], symbol=row['symbol'], kind=row['kind'], state=row['state'],
        side=row['side'], type=row['type'], amount=row['amount'], price=row['price'],
        trigger_price=row['trigger_price'], reduce_only=bool(row['reduce_only']),
        params=json.loads(row['params']) if row['params'] else {},
        tp_sl=json.loads(row['tp_sl']) if row['tp_sl'] else None, role=row['role'],
        position_id=row['position_id'], level_id=row['level_id'], parent=row['parent'],
        order_id=row['order_id'], status=row['status'], filled=row['filled'], protected=bool(row['protected']),
        attempts=int(row['attempts']), note=row['note'], created_ms=int(row['created_ms'] or 0),
        updated_ms=int(row['updated_ms'] or 0), trigger_by=row['trigger_by'], algo_id=row['algo_id'],
        plan_retired=row['plan_retired'])


def _algo(row: sqlite3.Row) -> AlgoRecord:
    return AlgoRecord(
        namespace=row['namespace'], algo_id=row['algo_id'], kind=row['kind'], symbol=row['symbol'], side=row['side'],
        size=row['size'], state=row['state'], status=row['status'], reason=row['reason'], note=row['note'],
        created_ms=int(row['created_ms'] or 0), updated_ms=int(row['updated_ms'] or 0),
        plan=json.loads(row['plan']) if row['plan'] else None)


__all__ = ['ALGO_STATES', 'AlgoRecord', 'Intent', 'IntentStore', 'OPEN_ALGO_STATES', 'OPEN_STATES', 'SCHEMA_VERSION',
           'STATES', 'key_fingerprint', 'legacy_namespace_of', 'namespace_of', 'now_ms']
