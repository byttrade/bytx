"""Exchange emulators for SDK users' tests: no network, keys or trades.

    from bytx.testing import fake_exchange, paper_exchange, drive, EXCHANGES

    ex, venue = fake_exchange('bybit', price=50_000)       # regular Exchange, ex.ccxt stubbed
    ex = paper_exchange('okx', bids=[(99, 1)], asks=[(100, 1)])   # paper on a virtual clock

* :func:`fake_exchange`: an ``Exchange`` for any supported exchange on market snapshots from the docs (tick, minimums,
  contractSize as on the exchange); orders are filled by the :class:`FakeVenue` emulator; the TP/SL path matches
  the SDK's on that exchange. A non-emulated request raises :class:`NetworkDisabled`.
* :func:`paper_exchange`: a ``PaperExchange`` (engine :mod:`bytx.paper`) on a :class:`VirtualClock`;
  the market is driven by :func:`set_book` / :func:`add_trade`, time by :func:`drive`.
* pytest plugin: ``pytest_plugins = ['bytx.testing.pytest_plugin']`` (fixtures
  ``fake_exchange``, ``paper_exchange``, ``bytx_clock``, ``bytx_offline``).

These are the same emulators the SDK itself is tested on. They do not replace real exchange behavior:
use the exchange's demo/testnet before going live.
"""
from bytx.paper import PaperConfig, PaperFees, VirtualClock
from bytx.testing.paper import DEFAULT_CONFIG, T0, add_trade, drive, paper_exchange, set_book, settle
from bytx.testing.venue import (
    CCXT_IDS,
    MARKETS_DIR,
    SYMBOLS,
    FakeVenue,
    NetworkDisabled,
    fake_exchange,
    make_exchange,
    markets,
    offline,
)

#: SDK exchange names that are emulated
EXCHANGES = tuple(CCXT_IDS)

__all__ = [
    'CCXT_IDS', 'DEFAULT_CONFIG', 'EXCHANGES', 'FakeVenue', 'MARKETS_DIR', 'NetworkDisabled', 'PaperConfig',
    'PaperFees', 'SYMBOLS', 'T0', 'VirtualClock', 'add_trade', 'drive', 'fake_exchange', 'make_exchange',
    'markets', 'offline', 'paper_exchange', 'set_book', 'settle',
]
