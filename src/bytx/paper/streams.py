"""Paper-mode WS streams: the same ``ex.stream.*`` and the same supervisor (:class:`Feed`,
mailboxes, ``on_state``, trackers), but:

- private streams (``orders`` / ``fills`` / ``positions`` / ``balance``) get events
  from the paper engine, not the exchange;
- live paper: public streams come from the exchange, and each order book / trades / ticker message
  goes to the engine first (fills of resting orders), then to consumers;
- replay: public streams deliver recorded messages in recording order (driver
  :class:`bytx.paper.replay.Replay`), no network.
"""
from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Optional

from bytx.streams import Streams
from bytx.streams.supervisor import Feed, FeedSpec, Policy, StreamConfig

if TYPE_CHECKING:
    from bytx.paper.exchange import PaperExchange

PRIVATE = ('orders', 'fills', 'positions', 'balance')
MARKET_KINDS = frozenset({'order_book', 'trades', 'ticker', 'best_bid_ask', 'candles'})


class Channel:
    """Message queue of one stream (like ``watch_*``: ``get`` returns everything accumulated)."""

    __slots__ = ('items', '_event', 'maxlen', 'dropped')

    def __init__(self, maxlen: int = 10_000) -> None:
        self.items: list = []
        self._event: Optional[asyncio.Event] = None
        self.maxlen = maxlen
        self.dropped = 0

    def put(self, item: Any) -> None:
        if len(self.items) >= self.maxlen:
            self.items.pop(0)
            self.dropped += 1
        self.items.append(item)
        if self._event is not None:
            self._event.set()

    async def get(self) -> list:
        while not self.items:
            if self._event is None:
                self._event = asyncio.Event()
            self._event.clear()
            await self._event.wait()
        out, self.items = self.items, []
        return out


def _identity(raw: Any, feed: Feed) -> list:
    return list(raw)


def replay_config() -> StreamConfig:
    """Replay: no silence watchdog (driver pauses are not a connection drop)."""
    cfg = StreamConfig()
    cfg.stale_after = {k: None for k in cfg.stale_after}
    return cfg


class PaperStreams(Streams):
    """``ex.stream`` of :class:`~bytx.paper.PaperExchange` (see module)."""

    def __init__(self, exchange: 'PaperExchange', config: Optional[StreamConfig] = None) -> None:
        super().__init__(exchange, config or (replay_config() if exchange.replay_mode else None))
        self.paper = exchange
        self._private: dict[str, Channel] = {}
        self._public: dict[tuple, Channel] = {}

    def exchange_now_ms(self) -> float:
        return float(self.paper.clock.now_ms())

    # ─── sources ──────────────────────────────────────────────────────────

    def _spec(self, kind: str, symbol: Optional[str], market: Any, opts: dict) -> FeedSpec:
        if kind in PRIVATE:
            ch = self._channel(self._private, kind, (kind,))
            return FeedSpec(kind=kind, key=(kind,), symbol=None, watch=ch.get, convert=_identity,
                            policy='latest' if kind == 'balance' else 'queue')
        if not self.paper.replay_mode:
            return super()._spec(kind, symbol, market, opts)
        key: tuple = (kind, symbol, opts.get('timeframe') or '1m') if kind == 'candles' else (kind, symbol)
        ch = self._channel(self._public, key, key)
        policy: Policy = 'latest' if kind in ('order_book', 'ticker', 'best_bid_ask') else 'queue'
        return FeedSpec(kind=kind, key=key, symbol=symbol, watch=ch.get, convert=_identity, policy=policy)

    def _channel(self, table: dict, name: Any, key: tuple) -> Channel:
        # a second subscriber to the same stream shares the Feed — and its queue
        ch = table.get(name)
        if ch is None or not self._running(key):
            ch = table[name] = Channel(self.config.queue_size)
        return ch

    def _running(self, key: tuple) -> bool:
        feed = self._feeds.get(key)
        return feed is not None and feed.running

    def push_private(self, kind: str, item: Any) -> None:
        """Engine event → private stream (only if subscribed, as on the exchange)."""
        ch = self._private.get(kind)
        if ch is not None and self._running((kind,)):
            ch.put(item)

    def push_public(self, kind: str, symbol: str, item: Any, timeframe: Optional[str] = None) -> None:
        """Replay: recorded message → public stream (if subscribed)."""
        key: tuple = (kind, symbol, timeframe or '1m') if kind == 'candles' else (kind, symbol)
        ch = self._public.get(key)
        if ch is not None and self._running(key):
            ch.put(item)

    def _on_items(self, feed: Feed, items: list) -> None:
        super()._on_items(feed, items)
        kind = feed.spec.kind
        if kind in MARKET_KINDS and not self.paper.replay_mode:
            self.paper._on_market(kind, items)


__all__ = ['PaperStreams', 'Channel', 'PRIVATE', 'MARKET_KINDS', 'replay_config']
