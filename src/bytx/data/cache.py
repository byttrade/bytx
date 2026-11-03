"""On-disk market history cache: parquet when pyarrow is available, otherwise csv.

One series key (exchange, market, demo, kind, symbol, timeframe) is one data file plus a sibling
``*.meta.json`` listing the time intervals ``[a, b)`` already fully downloaded (``covered``). An empty
interval (the exchange did not trade) is covered too and is not requested again. Writes are atomic
(temp file + ``os.replace``): data first, then meta — after a crash between the two, meta is
stale and the interval is simply downloaded again.

One writer per key: within a process, a per-key lock in :class:`~bytx.data.loader.HistoryLoader`;
several processes on one directory — last writer wins (data is never corrupted).
"""
from __future__ import annotations

import csv
import json
import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Literal, Optional

Fmt = Literal['auto', 'parquet', 'csv']
Interval = tuple[int, int]
META_VERSION = 1

#: series columns and types (int / float / str; empty value is None)
COLUMNS: dict[str, tuple[tuple[str, type], ...]] = {
    'candles': (('timestamp', int), ('open', float), ('high', float), ('low', float), ('close', float),
                ('volume', float)),
    'funding': (('timestamp', int), ('rate', float)),
    'trades': (('timestamp', int), ('id', str), ('side', str), ('price', float), ('size', float)),
}


def have_pyarrow() -> bool:
    """pyarrow (with parquet) is importable — the ``parquet`` extra."""
    try:
        import pyarrow.parquet  # type: ignore[import-not-found,import-untyped,unused-ignore]  # noqa: F401
    except ImportError:
        return False
    return True


def default_cache_dir() -> Path:
    """``$BYTX_CACHE_DIR``, else ``$XDG_CACHE_HOME/bytx/history`` or ``~/.cache/bytx/history``."""
    env = os.environ.get('BYTX_CACHE_DIR')
    if env:
        return Path(env).expanduser()
    base = os.environ.get('XDG_CACHE_HOME') or str(Path.home() / '.cache')
    return Path(base).expanduser() / 'bytx' / 'history'


# ─── intervals [a, b) ────────────────────────────────────────────────────────

def merge(intervals: Iterable[Interval]) -> list[Interval]:
    """Union of overlapping and adjacent intervals, ascending."""
    out: list[list[int]] = []
    for a, b in sorted((int(a), int(b)) for a, b in intervals if b > a):
        if out and a <= out[-1][1]:
            out[-1][1] = max(out[-1][1], b)
        else:
            out.append([a, b])
    return [(a, b) for a, b in out]


def subtract(start: int, end: int, covered: Iterable[Interval]) -> list[Interval]:
    """Parts of ``[start, end)`` not covered by ``covered``."""
    out: list[Interval] = []
    t = start
    for a, b in merge(covered):
        if b <= t:
            continue
        if a >= end:
            break
        if a > t:
            out.append((t, a))
        t = max(t, b)
        if t >= end:
            break
    if t < end:
        out.append((t, end))
    return out


# ─── key and storage ───────────────────────────────────────────────────────

@dataclass(frozen=True)
class CacheKey:
    exchange: str
    market_type: str
    demo: bool
    kind: str
    symbol: str
    timeframe: Optional[str] = None

    def stem(self) -> str:
        safe = re.sub(r'[^A-Za-z0-9._-]+', '_', self.symbol).strip('_')
        name = f'{safe}__{self.timeframe}' if self.timeframe else safe
        net = 'demo' if self.demo else 'live'
        return f'{self.exchange}/{net}-{self.market_type}/{self.kind}/{name}'


class HistoryCache:
    """Series files under ``root``. ``fmt='auto'`` — parquet if pyarrow is installed (the ``parquet`` extra)."""

    def __init__(self, root: os.PathLike | str, fmt: Fmt = 'auto') -> None:
        if fmt not in ('auto', 'parquet', 'csv'):
            raise ValueError(f"fmt: 'auto', 'parquet' or 'csv', not {fmt!r}")
        if fmt == 'parquet' and not have_pyarrow():
            raise ImportError("parquet requires pyarrow: pip install 'bytx[parquet]'")
        self.root = Path(root)
        self.fmt: Literal['parquet', 'csv'] = 'parquet' if fmt == 'parquet' or (fmt == 'auto' and have_pyarrow()) \
            else 'csv'

    def _meta_path(self, key: CacheKey) -> Path:
        return self.root / f'{key.stem()}.meta.json'

    def load(self, key: CacheKey) -> tuple[list[dict], list[Interval]]:
        """(rows in ascending time order, covered intervals). No file / corrupt meta — empty."""
        meta_path = self._meta_path(key)
        try:
            meta = json.loads(meta_path.read_text(encoding='utf-8'))
            if meta.get('v') != META_VERSION:
                return [], []
            data = meta_path.parent / str(meta['file'])
            covered = merge((int(a), int(b)) for a, b in meta.get('covered') or [])
            rows = self._read(data, key.kind) if data.exists() else []
        except (OSError, ValueError, KeyError, TypeError):
            return [], []
        if not rows and covered and not data.exists():
            return [], []      # meta without data (file deleted) — treat as not downloaded
        return rows, covered

    def save(self, key: CacheKey, rows: list[dict], covered: Iterable[Interval]) -> Path:
        meta_path = self._meta_path(key)
        meta_path.parent.mkdir(parents=True, exist_ok=True)
        ext = 'parquet' if self.fmt == 'parquet' else 'csv'
        data = meta_path.with_name(meta_path.name.replace('.meta.json', f'.{ext}'))
        self._write(data, key.kind, rows)
        meta = {'v': META_VERSION, 'file': data.name, 'format': self.fmt, 'exchange': key.exchange,
                'market_type': key.market_type, 'demo': key.demo, 'kind': key.kind, 'symbol': key.symbol,
                'timeframe': key.timeframe, 'covered': [list(i) for i in merge(covered)]}
        _atomic_write(meta_path, json.dumps(meta, ensure_ascii=False, indent=1).encode('utf-8'))
        # format changed (pyarrow installed) — the old file is no longer needed
        for other in ('parquet', 'csv'):
            old = data.with_suffix(f'.{other}')
            if other != ext and old.exists():
                old.unlink()
        return data

    # ─── formats ──────────────────────────────────────────────────────────

    @staticmethod
    def _read(path: Path, kind: str) -> list[dict]:
        cols = COLUMNS[kind]
        if path.suffix == '.parquet':
            import pyarrow.parquet as pq  # type: ignore[import-not-found,import-untyped,unused-ignore]
            return list(pq.read_table(path).to_pylist())
        with path.open('r', encoding='utf-8', newline='') as f:
            return [{name: (typ(v) if v != '' else None) for (name, typ), v in
                     zip(cols, (row.get(name, '') for name, _ in cols))}
                    for row in csv.DictReader(f)]

    @staticmethod
    def _write(path: Path, kind: str, rows: list[dict]) -> None:
        cols = COLUMNS[kind]
        if path.suffix == '.parquet':
            import pyarrow as pa  # type: ignore[import-not-found,import-untyped,unused-ignore]
            import pyarrow.parquet as pq  # type: ignore[import-not-found,import-untyped,unused-ignore]
            types = {int: pa.int64(), float: pa.float64(), str: pa.string()}
            schema = pa.schema([(name, types[typ]) for name, typ in cols])
            table = pa.Table.from_pylist([{n: r.get(n) for n, _ in cols} for r in rows], schema=schema)
            fd, tmp = tempfile.mkstemp(dir=path.parent, prefix='.tmp-', suffix='.parquet')
            os.close(fd)
            try:
                pq.write_table(table, tmp)
                os.replace(tmp, path)
            except BaseException:
                _unlink(tmp)
                raise
            return
        fd, tmp = tempfile.mkstemp(dir=path.parent, prefix='.tmp-', suffix='.csv')
        try:
            with os.fdopen(fd, 'w', encoding='utf-8', newline='') as f:
                w = csv.writer(f)
                w.writerow([n for n, _ in cols])
                for r in rows:
                    w.writerow(['' if r.get(n) is None else _fmt(r.get(n)) for n, _ in cols])
            os.replace(tmp, path)
        except BaseException:
            _unlink(tmp)
            raise


def _fmt(v: Any) -> str:
    return repr(v) if isinstance(v, float) else str(v)      # repr of float — no precision loss


def _unlink(path: str) -> None:
    try:
        os.unlink(path)
    except OSError:
        pass


def _atomic_write(path: Path, data: bytes) -> None:
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix='.tmp-')
    try:
        with os.fdopen(fd, 'wb') as f:
            f.write(data)
        os.replace(tmp, path)
    except BaseException:
        _unlink(tmp)
        raise


__all__ = ['COLUMNS', 'CacheKey', 'Fmt', 'HistoryCache', 'Interval', 'default_cache_dir', 'have_pyarrow',
           'merge', 'subtract']
