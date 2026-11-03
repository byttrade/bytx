"""Data: history loader with an on-disk cache and pandas / polars adapters.

    from bytx.data import HistoryLoader
    h = await HistoryLoader(ex).candles('BTC/USDT:USDT', '1h', since=t0)   # or ex.history.candles(...)
    df = h.to_pandas()

pandas, polars, pyarrow are optional (extras ``pandas``, ``polars``, ``parquet``); without pyarrow
the cache is written as csv.
"""
from bytx.data.cache import HistoryCache, default_cache_dir
from bytx.data.frames import records, to_pandas, to_polars
from bytx.data.loader import History, HistoryLoader

__all__ = ['History', 'HistoryCache', 'HistoryLoader', 'default_cache_dir', 'records', 'to_pandas', 'to_polars']
