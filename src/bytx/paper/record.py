"""Recording WS streams to a file for replay.

    async with create_exchange('binance') as ex:
        with Recorder(ex, 'btc.jsonl.gz'):
            async for book in ex.stream.order_book('BTC/USDT'):
                ...

Every message of this Exchange's public ``ex.stream`` streams (order book, trades, ticker, best_bid_ask,
candles) is written — losslessly, before delivery to consumers (a slow consumer does not lose
records), plus live paper funding rates. Recording is passive: the code itself makes the subscriptions.

Format — JSON Lines (``.gz`` — gzip), version :data:`RECORD_VERSION`:

- line 1 — header ``{"format": "bytx.ws-record", "version": 1, "exchange", "market_type",
  "created_ms", "sdk"}``;
- ``{"k":"market","s":symbol,"d":ccxt market}`` — before a symbol's first message (replay without network);
- ``{"k":kind,"t":receive ms,"s":symbol,"d":data[,"tf":timeframe]}`` — one message (one
  SDK model without ``raw``); ``t`` — receive clock (for paper, its own clock), which drives the replay
  virtual clock. Fields are only ever added; a file newer than the supported version — :class:`ConfigError`.
"""
from __future__ import annotations

import gzip
import json
import time
from dataclasses import dataclass, fields
from pathlib import Path
from typing import IO, Any, Callable, Iterator, Optional, Union, cast

from bytx.errors import ConfigError
from bytx.streams.supervisor import FeedSpec
from bytx.types import Candle, FundingRate, OrderBook, Ticker, Trade

RECORD_FORMAT = 'bytx.ws-record'
RECORD_VERSION = 1
DEFAULT_KINDS = frozenset({'order_book', 'trades', 'ticker', 'best_bid_ask', 'candles', 'funding'})
_MARKET_PREFIX = '{"k":"market"'
_MODELS: dict[str, Any] = {'order_book': OrderBook, 'trades': Trade, 'ticker': Ticker, 'best_bid_ask': Ticker,
                           'candles': Candle, 'funding': FundingRate}
PathLike = Union[str, Path]


def FUNDING_SPEC(symbol: str) -> FeedSpec:  # noqa: N802 — "spec" factory for tap
    """Pseudo-stream of the funding rate (polling live paper) for :class:`Recorder`."""
    async def _never() -> Any:  # pragma: no cover
        raise NotImplementedError
    return FeedSpec(kind='funding', key=('funding', symbol), symbol=symbol, watch=_never,
                    convert=lambda raw, feed: [])


def _open(path: PathLike, mode: str) -> IO[str]:
    p = str(path)
    if p.endswith('.gz'):
        return cast(IO[str], gzip.open(p, mode + 't', encoding='utf-8'))
    return open(p, mode, encoding='utf-8')


def encode(item: Any) -> dict:
    """SDK model → dict without ``raw``."""
    return {f.name: getattr(item, f.name) for f in fields(item) if f.name != 'raw'}


def decode(kind: str, data: dict) -> Any:
    """Record dict → SDK model (unknown fields ignored; book levels as tuples)."""
    model = _MODELS[kind]
    names = {f.name for f in fields(model)}
    kw = {k: v for k, v in data.items() if k in names}
    if model is OrderBook:
        kw['bids'] = [tuple(x) for x in kw.get('bids') or []]
        kw['asks'] = [tuple(x) for x in kw.get('asks') or []]
    return model(**kw)


@dataclass(slots=True)
class RecordHeader:
    format: str
    version: int
    exchange: str
    market_type: str
    created_ms: int
    extra: dict


@dataclass(slots=True)
class Record:
    """One recorded message: ``t`` — receive ms, ``item`` — SDK model."""

    t: int
    kind: str
    symbol: Optional[str]
    item: Any
    timeframe: Optional[str] = None


class Recorder:
    """Writes public ``ex.stream`` streams to a file (see module). ``kinds`` — which kinds to record.

    ``start()`` / ``close()`` or ``with`` / ``async with``. One Recorder per Exchange.
    ``count`` — messages written; :meth:`write` — write your own (e.g. a funding rate).
    """

    def __init__(self, exchange: Any, path: PathLike, *, kinds: Optional[set[str]] = None,
                 clock: Optional[Callable[[], int]] = None) -> None:
        self.exchange = exchange
        self.path = Path(path)
        self.kinds = frozenset(kinds) if kinds is not None else DEFAULT_KINDS
        unknown = set(self.kinds) - set(_MODELS)
        if unknown:
            raise ConfigError(f'Recorder: unknown kinds {sorted(unknown)}; available: {sorted(_MODELS)}')
        paper_clock = getattr(exchange, 'clock', None)
        self._now = clock or (paper_clock.now_ms if paper_clock is not None else (lambda: int(time.time() * 1000)))
        self._file: Optional[IO[str]] = None
        self._markets: set[str] = set()
        self.count = 0

    def start(self) -> 'Recorder':
        if self._file is not None:
            return self
        streams = self.exchange.stream
        if streams._tap is not None:
            raise ConfigError('streams are already being recorded on this Exchange')
        from bytx.__version__ import __version__
        self._file = _open(self.path, 'w')
        self._line({'format': RECORD_FORMAT, 'version': RECORD_VERSION, 'exchange': self.exchange.name,
                    'market_type': self.exchange.market_type, 'created_ms': self._now(), 'sdk': __version__})
        streams._tap = self._tap
        return self

    def close(self) -> None:
        streams = getattr(self.exchange, '_streams', None)
        if streams is not None and streams._tap == self._tap:
            streams._tap = None
        if self._file is not None:
            self._file.close()
            self._file = None

    def __enter__(self) -> 'Recorder':
        return self.start()

    def __exit__(self, *exc: Any) -> None:
        self.close()

    async def __aenter__(self) -> 'Recorder':
        return self.start()

    async def __aexit__(self, *exc: Any) -> None:
        self.close()

    def write(self, kind: str, symbol: Optional[str], item: Any, *, timeframe: Optional[str] = None) -> None:
        """Writes one message (an SDK model of kind ``kind``)."""
        if self._file is None:
            raise ConfigError('Recorder is not started (start())')
        if symbol and symbol not in self._markets:
            self._markets.add(symbol)
            raw = (getattr(self.exchange.ccxt, 'markets', None) or {}).get(symbol)
            if raw is not None:
                self._line({'k': 'market', 's': symbol, 'd': raw})
        row: dict = {'k': kind, 't': self._now(), 's': symbol, 'd': encode(item)}
        if timeframe:
            row['tf'] = timeframe
        self._line(row)
        self.count += 1

    def _tap(self, spec: FeedSpec, items: list) -> None:
        if spec.kind not in self.kinds:
            return
        tf = spec.key[2] if spec.kind == 'candles' and len(spec.key) > 2 else None
        for item in items:
            self.write(spec.kind, spec.symbol, item, timeframe=tf)

    def _line(self, row: dict) -> None:
        assert self._file is not None
        self._file.write(json.dumps(row, separators=(',', ':'), ensure_ascii=False, default=str))
        self._file.write('\n')


def read_header(path: PathLike) -> tuple[RecordHeader, dict[str, dict]]:
    """Recording header and markets (markets — a pass over ``market`` lines without parsing the rest)."""
    markets: dict[str, dict] = {}
    with _open(path, 'r') as f:
        first = f.readline()
        if not first:
            raise ConfigError(f'recording {path}: empty file')
        head = json.loads(first)
        if head.get('format') != RECORD_FORMAT:
            raise ConfigError(f'recording {path}: not {RECORD_FORMAT} format')
        version = int(head.get('version', 0))
        if version > RECORD_VERSION or version < 1:
            raise ConfigError(f'recording {path}: version {version} is not supported (up to {RECORD_VERSION})')
        for line in f:
            if line.startswith(_MARKET_PREFIX):
                row = json.loads(line)
                markets[row['s']] = row['d']
    known = {'format', 'version', 'exchange', 'market_type', 'created_ms'}
    header = RecordHeader(head['format'], version, head['exchange'], head.get('market_type', 'swap'),
                          int(head.get('created_ms') or 0), {k: v for k, v in head.items() if k not in known})
    return header, markets


def iter_records(path: PathLike, *, kinds: Optional[set[str]] = None) -> Iterator[Record]:
    """Recorded messages in order (without header and markets); unknown kinds are skipped."""
    with _open(path, 'r') as f:
        f.readline()
        for line in f:
            if not line.strip() or line.startswith(_MARKET_PREFIX):
                continue
            row = json.loads(line)
            kind = row.get('k')
            if kind not in _MODELS or (kinds is not None and kind not in kinds):
                continue
            yield Record(int(row['t']), kind, row.get('s'), decode(kind, row['d']), row.get('tf'))


__all__ = ['Recorder', 'Record', 'RecordHeader', 'read_header', 'iter_records', 'encode', 'decode',
           'RECORD_FORMAT', 'RECORD_VERSION', 'DEFAULT_KINDS', 'FUNDING_SPEC']
