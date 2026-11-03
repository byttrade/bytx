"""Paper trading and recording/replay of WS streams.

    ex = create_exchange('binance', paper=True)            # live order book, virtual money
    await ex.buy('BTC/USDT', size=0.01)                    # filled against the book, taker fee

    with Recorder(live_ex, 'btc.jsonl.gz'): ...            # records ex.stream streams
    ex = replay_exchange('btc.jsonl.gz')                   # replays the recording: virtual clock
    result = await ex.replay.run(strategy)                 # same inputs → same fills

Paper does **not replace** exchange demo/testnet: they have different
liquidation models (here a simplified one, see :mod:`bytx.paper.engine`).
"""
from bytx.paper.clock import Clock, VirtualClock, WallClock
from bytx.paper.engine import LIQUIDATION_MODEL, PaperEngine, PaperFunding, PaperLiquidation, PaperRequest
from bytx.paper.exchange import PaperConfig, PaperExchange
from bytx.paper.fees import PaperFees, default_fees
from bytx.paper.record import RECORD_VERSION, Record, Recorder, iter_records, read_header
from bytx.paper.replay import Replay, ReplayResult, replay_exchange

__all__ = [
    'Clock', 'VirtualClock', 'WallClock', 'PaperEngine', 'PaperRequest', 'PaperFunding', 'PaperLiquidation',
    'LIQUIDATION_MODEL', 'PaperFees', 'default_fees', 'PaperConfig', 'PaperExchange', 'Recorder', 'Record',
    'RECORD_VERSION', 'iter_records', 'read_header', 'Replay', 'ReplayResult', 'replay_exchange',
]
