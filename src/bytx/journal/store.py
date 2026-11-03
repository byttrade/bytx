"""Position journal store (the SDK never writes to the application DB — the application plugs in its own).

:class:`JournalStore` — the protocol; :class:`InMemoryJournalStore` — the built-in implementation
(tests, scripts, a process without a DB). A custom store (e.g. a ``position_journal`` collection
in Mongo) implements the same protocol — schema in the README, section "Position journal".

Contract:

- ``append`` is idempotent by ``event.key``: a repeat returns the already stored event and
    ``created=False``, changing nothing;
- the store assigns ``seq``: strictly increasing within ``(bot_id, position_id)`` with no
    gaps in-memory (Mongo may have gaps, but not reordering);
- stored events are never changed or deleted (append-only).
"""
from __future__ import annotations

import asyncio
import dataclasses
from dataclasses import dataclass
from typing import Optional, Protocol, runtime_checkable

from bytx.journal.models import JournalEvent


@dataclass(frozen=True, slots=True)
class AppendResult:
    """Result of ``append``: ``event`` — the stored event (with ``seq``); ``created`` — False if the key already existed."""

    event: JournalEvent
    created: bool


@runtime_checkable
class JournalStore(Protocol):
    """Journal store protocol (all methods are async — the Mongo driver is asynchronous)."""

    async def append(self, event: JournalEvent) -> AppendResult:
        """Appends an event, assigning ``seq``; if ``event.key`` already exists — that event, ``created=False``."""
        ...

    async def read(self, position_id: str, *, bot_id: Optional[str] = None,
                   since_seq: int = 0) -> list[JournalEvent]:
        """Position events in ascending ``seq`` (only those with ``seq > since_seq``)."""
        ...

    async def find_order(self, *, bot_id: Optional[str] = None, exchange_order_id: Optional[str] = None,
                         client_order_id: Optional[str] = None) -> list[JournalEvent]:
        """Order events by exchange id or client_id (links order → position after a restart)."""
        ...

    async def positions(self, *, bot_id: Optional[str] = None) -> list[str]:
        """position_ids that have events (in order of the first event)."""
        ...


class InMemoryJournalStore:
    """In-process memory journal. Safe within a single event loop."""

    def __init__(self) -> None:
        self._by_key: dict[str, JournalEvent] = {}
        self._by_position: dict[tuple[Optional[str], str], list[JournalEvent]] = {}
        self._by_order: dict[str, list[JournalEvent]] = {}
        self._lock = asyncio.Lock()

    async def append(self, event: JournalEvent) -> AppendResult:
        key = event.key or event.default_key()
        async with self._lock:
            existing = self._by_key.get(key)
            if existing is not None:
                return AppendResult(dataclasses.replace(existing), False)
            items = self._by_position.setdefault((event.bot_id, event.position_id), [])
            stored = dataclasses.replace(event, seq=len(items) + 1, key=key, raw=dict(event.raw))
            items.append(stored)
            self._by_key[key] = stored
            for ref in self._order_refs(stored.bot_id, stored.exchange_order_id, stored.client_order_id):
                self._by_order.setdefault(ref, []).append(stored)
            return AppendResult(dataclasses.replace(stored), True)

    async def read(self, position_id: str, *, bot_id: Optional[str] = None,
                   since_seq: int = 0) -> list[JournalEvent]:
        items = self._by_position.get((bot_id, position_id), [])
        return [dataclasses.replace(e) for e in items if (e.seq or 0) > since_seq]

    async def find_order(self, *, bot_id: Optional[str] = None, exchange_order_id: Optional[str] = None,
                         client_order_id: Optional[str] = None) -> list[JournalEvent]:
        seen: dict[str, JournalEvent] = {}
        for ref in self._order_refs(bot_id, exchange_order_id, client_order_id):
            for e in self._by_order.get(ref, []):
                seen[e.key or ''] = e
        return sorted((dataclasses.replace(e) for e in seen.values()), key=lambda e: (e.ts, e.seq or 0))

    async def positions(self, *, bot_id: Optional[str] = None) -> list[str]:
        return [pid for (b, pid) in self._by_position if b == bot_id]

    @staticmethod
    def _order_refs(bot_id: Optional[str], oid: Optional[str], cid: Optional[str]) -> list[str]:
        refs = []
        if oid:
            refs.append(f'{bot_id or ""}|oid:{oid}')
        if cid:
            refs.append(f'{bot_id or ""}|cid:{cid}')
        return refs


__all__ = ['AppendResult', 'InMemoryJournalStore', 'JournalStore']
