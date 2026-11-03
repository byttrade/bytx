"""WS order book: conversion, integrity checks, local top-of-book cache.

How ccxt.pro 4.4.65 builds the book and what happens on reconnect
(checked against the ``ccxt/pro`` sources):

- binance, kucoin — after the subscription is confirmed (``handle_order_book_subscription``)
  the symbol's book is deleted and recreated empty, the snapshot is fetched via REST,
  buffered deltas are applied by nonce (``load_order_book`` / ``fetch_order_book_snapshot``);
- bybit, okx — the exchange itself sends a ``snapshot`` as the first subscription message, ccxt does
  ``orderbook.reset(snapshot)``;
- bingx, hyperliquid — every message is a full snapshot (``reset``);
- bitget (``books``) — snapshot first, then deltas; integrity via seq/pseq (``exchanges/bitget/ccxt_fixes``);
- gate — REST snapshot + deltas by id (like binance).

The subscription is re-sent only by a new ccxt client (the old one already has it in
``client.subscriptions``). So after ANY order book stream error the
supervisor closes its ccxt client (:func:`supervisor.force_reconnect`): the first
book after reconnect is guaranteed to be built from a fresh snapshot.

In addition: a crossed book (best_bid >= best_ask) and a nonce going backwards
within one connection → forced resync.
"""
from __future__ import annotations

from typing import Any, Optional

from bytx import convert
from bytx.streams.supervisor import Feed, Resync
from bytx.types import Market, OrderBook, Ticker


def to_book(raw: Any, market: Optional[Market], depth: Optional[int] = None) -> OrderBook:
    """ccxt book (shared mutable ccxt object) → immutable :class:`OrderBook` snapshot.

    Slicing to ``depth`` levels happens BEFORE conversion to the base coin — on binance the
    local ccxt book holds up to 1000 levels.
    """
    bids = raw.get('bids') or []
    asks = raw.get('asks') or []
    if depth is not None:
        bids = bids[:depth]
        asks = asks[:depth]
    return convert.order_book({'symbol': raw.get('symbol'), 'bids': bids, 'asks': asks,
                               'timestamp': raw.get('timestamp'), 'nonce': raw.get('nonce')}, market)


def trim(book: OrderBook, depth: Optional[int]) -> OrderBook:
    """Copy of the book with ``depth`` levels (for a subscriber with a smaller depth)."""
    if depth is None or (len(book.bids) <= depth and len(book.asks) <= depth):
        return book
    return OrderBook(book.symbol, book.bids[:depth], book.asks[:depth], book.timestamp, book.nonce)


def is_crossed(book: OrderBook) -> bool:
    """best_bid >= best_ask — the book is corrupted (missed deltas)."""
    return bool(book.bids and book.asks and book.bids[0][0] >= book.asks[0][0])


def check_book(items: list, feed: Feed) -> None:
    """Integrity check for the order book :class:`Feed`; raises :class:`Resync`."""
    book: OrderBook = items[-1]
    if is_crossed(book):
        raise Resync(f'crossed order book {book.symbol}: bid {book.bids[0][0]} >= ask {book.asks[0][0]}',
                     'crossed')
    if not feed.hub.config.check_nonce or book.nonce is None:
        return
    prev = feed.state.get('nonce')
    feed.state['nonce'] = book.nonce
    if prev is not None and book.nonce < prev:
        raise Resync(f'order book nonce {book.symbol} went backwards: {prev} → {book.nonce}', 'nonce')


def reset_book_state(feed: Feed) -> None:
    """Reset local order book state before reconnecting."""
    feed.state.pop('nonce', None)


class TopOfBookCache:
    """Latest order book and ticker per symbol — for non-blocking reads in strategies."""

    __slots__ = ('books', 'tickers')

    def __init__(self) -> None:
        self.books: dict[str, OrderBook] = {}
        self.tickers: dict[str, Ticker] = {}

    def put_book(self, book: OrderBook) -> None:
        self.books[book.symbol] = book

    def put_ticker(self, ticker: Ticker) -> None:
        prev = self.tickers.get(ticker.symbol)
        if prev is not None and ticker.timestamp and prev.timestamp and ticker.timestamp < prev.timestamp:
            return  # ticker and best_bid_ask write to one cache: older data does not overwrite newer
        self.tickers[ticker.symbol] = ticker

    def clear(self, symbol: Optional[str] = None) -> None:
        if symbol is None:
            self.books.clear()
            self.tickers.clear()
        else:
            self.books.pop(symbol, None)
            self.tickers.pop(symbol, None)


__all__ = ['to_book', 'trim', 'is_crossed', 'check_book', 'reset_book_state', 'TopOfBookCache']
