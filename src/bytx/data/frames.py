"""Adapters from SDK models to tables: pandas and polars (extras ``pandas`` / ``polars``).

Accept a list of models (``Candle``, ``Trade``, ``FundingPayment``, ``Order``, ``Fill``, ``Position``…),
dicts or a :class:`~bytx.data.History`. The ``raw`` field (exchange response) is not carried over. If
``timestamp`` (ms) is present, a ``datetime`` column (UTC) is added. The libraries are imported only on
call: the SDK core works without them.
"""
from __future__ import annotations

import dataclasses
import importlib
from typing import Any, Iterable, Optional, Sequence

_SKIP = frozenset({'raw', 'info'})


def _require(module: str, extra: str) -> Any:
    try:
        return importlib.import_module(module)
    except ImportError as e:
        raise ImportError(f"{module} is required: pip install 'bytx[{extra}]'") from e


def records(rows: Iterable[Any]) -> tuple[list[dict], list[str]]:
    """(list of dicts, columns in model field order). The ``raw`` field is skipped."""
    out: list[dict] = []
    columns: list[str] = []
    seen: set[str] = set()
    for row in rows:
        if dataclasses.is_dataclass(row) and not isinstance(row, type):
            d = {f.name: getattr(row, f.name) for f in dataclasses.fields(row) if f.name not in _SKIP}
        elif isinstance(row, dict):
            d = {k: v for k, v in row.items() if k not in _SKIP}
        elif hasattr(row, '_asdict'):
            d = dict(row._asdict())
        else:
            raise TypeError(f'table row: SDK model (dataclass) or dict, not {type(row).__name__}')
        for k in d:
            if k not in seen:
                seen.add(k)
                columns.append(k)
        out.append(d)
    return out, columns


def _source(rows: Any, columns: Optional[Sequence[str]]) -> tuple[list[dict], list[str]]:
    cols_hint = list(columns) if columns else list(getattr(rows, 'columns', ()) or ())
    recs, cols = records(getattr(rows, 'rows', rows))
    return recs, (cols or cols_hint)


def to_pandas(rows: Any, *, columns: Optional[Sequence[str]] = None) -> Any:
    """``pandas.DataFrame`` from SDK models. Empty list — empty table with ``columns``."""
    pd = _require('pandas', 'pandas')
    recs, cols = _source(rows, columns)
    df = pd.DataFrame.from_records(recs, columns=cols)
    if 'timestamp' in df.columns:
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
    return df


def to_polars(rows: Any, *, columns: Optional[Sequence[str]] = None) -> Any:
    """``polars.DataFrame`` from SDK models. Empty list — empty table with ``columns``."""
    pl = _require('polars', 'polars')
    recs, cols = _source(rows, columns)
    if not recs:
        return pl.DataFrame({c: [] for c in cols})
    df = pl.DataFrame(recs, infer_schema_length=None)
    if 'timestamp' in df.columns:
        df = df.with_columns(pl.from_epoch(pl.col('timestamp'), time_unit='ms')
                             .dt.replace_time_zone('UTC').alias('datetime'))
    return df


__all__ = ['records', 'to_pandas', 'to_polars']
