"""Portfolio across accounts: equity, unrealized and realized PnL, fees and
funding for a period — with explicit data completeness.

    async with AccountPool.from_file('accounts.json') as pool:
        report = await pool.portfolio(since=ms_week_ago)
        report.to_csv('portfolio.csv')
        if not report.complete:
            print(report.issues())

Sources are each exchange's official docs (``bytx.exchanges.<exchange>.portfolio``): the account
cash flow log (Binance ``/fapi/v1/income``, Bybit ``/v5/account/transaction-log``,
OKX ``/api/v5/account/bills(-archive)``, BingX ``/openApi/swap/v2/user/income``, KuCoin
``/api/v1/transaction-history`` + per-symbol funding (``fetch_funding_paid``),
Hyperliquid ``userFillsByTime`` + ``userFunding``) and the account equity endpoint.

Signs (the same for all exchanges): ``realized_pnl`` — + profit; ``fees`` — + paid (rebate — minus);
``funding`` — + received / − paid; ``net_pnl = realized_pnl − fees + funding``.

Completeness: ``complete=False`` with a reason in ``issues`` if the history was cut off (page limit,
window deeper than the exchange history, pagination without progress), the exchange does not return part of the data
(e.g. KuCoin — account-level fees) or a request failed. Partial sums are never
passed off as complete.
"""
from __future__ import annotations

import csv
import importlib
import io
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable, Literal, Optional, TextIO, Union

from bytx.errors import BytxError, ConfigError, NotSupported

if TYPE_CHECKING:
    from bytx.accounts import AccountPool
    from bytx.exchange import Exchange

IncomeKind = Literal['realized', 'fee', 'funding']

#: stablecoins added to the portfolio total 1:1 to USD (exchange equity in USD too);
#: VST — the virtual USDT of the BingX demo environment (balance and Get Fund Flow on VST are in VST, demo 2026-11-04)
USD_LIKE = frozenset({'USD', 'USDT', 'USDC', 'VST'})

_PACKAGES = {'binance': 'binance', 'bybit': 'bybit', 'okx': 'okx', 'bingx': 'bingx',
             'kucoinfutures': 'kucoin', 'hyperliquid': 'hyperliquid',
             'bitget': 'bitget', 'gate': 'gate', 'mexc': 'mexc', 'htx': 'htx', 'blofin': 'blofin'}


@dataclass(slots=True)
class IncomeItem:
    """A single normalized exchange cash flow record.

    ``amount`` — signed balance change (+ in / − out): for ``fee`` the paid fee is
    negative (as in exchange logs); in :class:`IncomeTotals` totals it already carries the "expense" sign.
    ``side`` — the position side ('long' / 'short') of a funding record where the exchange reports it (Bybit
    SETTLEMENT: in hedge mode a row per side); otherwise None. ``market`` — the record's market type ('spot' /
    'swap' / 'future' / 'option') where one log covers markets with the same ids (Bybit UTA: ``category``); None —
    the client's market."""

    ts: int
    kind: IncomeKind
    amount: float
    currency: str
    symbol: Optional[str] = None
    id: Optional[str] = None
    side: Optional[str] = None
    market: Optional[str] = None


@dataclass(slots=True)
class IncomeTotals:
    """Period totals in a single currency."""

    currency: str
    realized_pnl: float = 0.0
    fees: float = 0.0
    funding: float = 0.0

    @property
    def net_pnl(self) -> float:
        return self.realized_pnl - self.fees + self.funding

    def add(self, item: IncomeItem) -> None:
        if item.kind == 'realized':
            self.realized_pnl += item.amount
        elif item.kind == 'fee':
            self.fees -= item.amount
        else:
            self.funding += item.amount


@dataclass(slots=True)
class IncomeResult:
    """Exchange module response: records + completeness."""

    items: list[IncomeItem] = field(default_factory=list)
    complete: bool = True
    issues: list[str] = field(default_factory=list)
    #: components the exchange does not return at account level ('fee', 'funding', 'realized') — their totals are None
    unavailable: set = field(default_factory=set)

    def incomplete(self, reason: str) -> None:
        self.complete = False
        if reason not in self.issues:
            self.issues.append(reason)


@dataclass(slots=True)
class AccountReport:
    """Portfolio of a single account for the period ``[since, until]`` (ms, inclusive)."""

    name: str
    exchange: str
    since: int
    until: int
    demo: bool = False
    #: equity by currency: {currency: value}; 'USD' — on exchanges that compute account equity in USD (Bybit UTA, OKX)
    equity: dict[str, float] = field(default_factory=dict)
    #: unrealized PnL of open positions (sum over positions, symbol settlement currency)
    unrealized_pnl: Optional[float] = None
    totals: dict[str, IncomeTotals] = field(default_factory=dict)
    positions: list = field(default_factory=list, repr=False)
    items: list[IncomeItem] = field(default_factory=list, repr=False)
    complete: bool = True
    issues: list[str] = field(default_factory=list)
    unavailable: set = field(default_factory=set)
    error: Optional[BytxError] = None
    #: the ``symbols=`` filter (SDK symbols): log records, positions and unrealized PnL of these symbols only;
    #: equity — always the whole account. None — a report for the whole account
    symbols: Optional[list[str]] = None

    def _sum(self, attr: str) -> Optional[float]:
        kind = {'realized_pnl': 'realized', 'fees': 'fee', 'funding': 'funding'}[attr]
        if kind in self.unavailable:
            return None
        return sum(getattr(t, attr) for c, t in self.totals.items() if c in USD_LIKE)

    @property
    def realized_pnl(self) -> Optional[float]:
        """Realized PnL for the period in USD-like currencies (None — the exchange does not return it)."""
        return self._sum('realized_pnl')

    @property
    def fees(self) -> Optional[float]:
        return self._sum('fees')

    @property
    def funding(self) -> Optional[float]:
        return self._sum('funding')

    @property
    def net_pnl(self) -> Optional[float]:
        parts = (self.realized_pnl, self.fees, self.funding)
        if any(p is None for p in parts):
            return None
        realized, fees, funding = parts
        return realized - fees + funding  # type: ignore[operator]

    @property
    def total_equity(self) -> Optional[float]:
        """Equity in USD-like currencies (USDT/USDC/USD 1:1); None — not read."""
        values = [v for c, v in self.equity.items() if c in USD_LIKE]
        return sum(values) if values else None

    def incomplete(self, reason: str) -> None:
        self.complete = False
        if reason not in self.issues:
            self.issues.append(reason)


@dataclass(slots=True)
class PortfolioReport:
    """Portfolio across all pool accounts."""

    since: int
    until: int
    accounts: list[AccountReport] = field(default_factory=list)

    @property
    def complete(self) -> bool:
        return all(a.complete for a in self.accounts)

    def issues(self) -> dict[str, list[str]]:
        return {a.name: list(a.issues) for a in self.accounts if a.issues}

    def _total(self, attr: str) -> Optional[float]:
        values = [getattr(a, attr) for a in self.accounts]
        known = [v for v in values if v is not None]
        return sum(known) if known else None

    @property
    def total_equity(self) -> Optional[float]:
        return self._total('total_equity')

    @property
    def unrealized_pnl(self) -> Optional[float]:
        return self._total('unrealized_pnl')

    @property
    def realized_pnl(self) -> Optional[float]:
        return self._total('realized_pnl')

    @property
    def fees(self) -> Optional[float]:
        return self._total('fees')

    @property
    def funding(self) -> Optional[float]:
        return self._total('funding')

    def rows(self) -> list[dict[str, Any]]:
        """Rows for a table/CSV: one row per account and totals currency (equity — in the currency's row)."""
        out: list[dict[str, Any]] = []
        for a in self.accounts:
            currencies = sorted(set(a.equity) | set(a.totals)) or ['']
            for cur in currencies:
                t = a.totals.get(cur)
                out.append({
                    'account': a.name, 'exchange': a.exchange, 'demo': a.demo,
                    'since': a.since, 'until': a.until, 'currency': cur,
                    'equity': a.equity.get(cur),
                    'unrealized_pnl': a.unrealized_pnl if cur == currencies[0] else None,
                    'realized_pnl': None if 'realized' in a.unavailable else (t.realized_pnl if t else 0.0),
                    'fees': None if 'fee' in a.unavailable else (t.fees if t else 0.0),
                    'funding': None if 'funding' in a.unavailable else (t.funding if t else 0.0),
                    'complete': a.complete,
                    'issues': '; '.join(a.issues),
                    'error': str(a.error) if a.error else '',
                })
        return out

    def to_csv(self, target: Union[str, Path, TextIO, None] = None) -> str:
        """Portfolio CSV (:meth:`rows`). ``target`` — a path or an open file; returns the CSV text."""
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=CSV_FIELDS, lineterminator='\n')
        writer.writeheader()
        for row in self.rows():
            writer.writerow({k: '' if v is None else v for k, v in row.items()})
        text = buf.getvalue()
        if isinstance(target, (str, Path)):
            Path(target).write_text(text, encoding='utf-8')
        elif target is not None:
            target.write(text)
        return text


CSV_FIELDS = ['account', 'exchange', 'demo', 'since', 'until', 'currency', 'equity', 'unrealized_pnl',
              'realized_pnl', 'fees', 'funding', 'complete', 'issues', 'error']


def exchange_module(ex: 'Exchange', name: str) -> Any:
    """Exchange module ``bytx.exchanges.<exchange>.<name>``; an exchange without the module — :class:`NotSupported`."""
    package = _PACKAGES.get(ex.id)
    if package is None:
        raise NotSupported(f'{name}: exchange {ex.id} is not supported', exchange=ex.name)
    return importlib.import_module(f'bytx.exchanges.{package}.{name}')


def _window(since: Optional[int], until: Optional[int]) -> tuple[int, int]:
    now = int(time.time() * 1000)
    end = int(until) if until is not None else now
    if since is None:
        raise ConfigError('portfolio: since (ms) is required — the report period')
    start = int(since)
    if end < start:
        raise ConfigError(f'portfolio: until {end} < since {start}')
    return start, end


def market_symbol(ex: 'Exchange', market_id: Optional[str], market: Optional[str] = None) -> Optional[str]:
    """SDK (ccxt) symbol of a log record by the exchange market id ('BTCUSDT', 'BTC-USDT-SWAP', Hyperliquid coin).

    Exact id match only (no base+quote «gluing»: OKX spot 'BTC-USDT' is not the swap); an id shared by several
    markets ('BTCUSDT' of spot and USDⓈ-M) — the market of the record's ``market`` type (Bybit ``category``), without
    it — of the client's type; no market of that type on the client — the id as is. Not found — the id as is."""
    if not market_id:
        return market_id
    c = ex.ccxt
    markets = c.markets or {}
    if market_id in markets:
        return market_id
    entries = [e for e in (getattr(c, 'markets_by_id', None) or {}).get(market_id) or [] if isinstance(e, dict)]
    if entries:
        if market is not None:
            # spot vs derivatives (Bybit linear covers both swap perpetuals and future contracts: the type is by id)
            same = [e for e in entries if (e.get('type') == 'spot') == (market == 'spot')]
            fit = next((e for e in same if e.get('type') == market), same[0] if same else None)
            return (fit.get('symbol') if fit else None) or market_id
        fit = next((e for e in entries if (e.get('type') == 'spot') == (ex.market_type == 'spot')), entries[0])
        return fit.get('symbol') or market_id
    if ex.id == 'hyperliquid':
        # the native HL instrument id is the coin ('BTC'); perpetuals — 'BTC/USDC:USDC' (convert.resolve_symbol)
        from bytx.convert import resolve_symbol
        return resolve_symbol(markets, {}, market_id, ex.market_type, ex.id) or market_id
    return market_id


async def account_report(ex: 'Exchange', *, since: Optional[int], until: Optional[int] = None,
                         name: Optional[str] = None, symbols: Optional[Iterable[str]] = None,
                         keep_items: bool = False) -> AccountReport:
    """Portfolio of a single account. Errors in individual parts are not raised; they make the report
    incomplete (``complete=False``, reason in ``issues``).

    The symbol of log records (``IncomeItem.symbol``) is the SDK symbol (:func:`market_symbol`), not the exchange
    market id. ``symbols=`` (any symbol spelling: 'BTC/USDT:USDT', 'BTCUSDT') — a report for these symbols: log
    records, positions and unrealized PnL of these symbols only; equity — of the whole account."""
    start, end = _window(since, until)
    report = AccountReport(name=name or ex.name, exchange=ex.name, since=start, until=end, demo=ex.demo)
    if getattr(ex, 'is_paper', False):
        raise NotSupported('portfolio: paper account — use its journal/balance directly', exchange=ex.name)
    await ex.connect()
    module = exchange_module(ex, 'portfolio')
    syms = list(dict.fromkeys(ex.symbol(s) for s in symbols)) if symbols else None
    report.symbols = syms

    try:
        report.equity = await module.equity(ex)
    except BytxError as e:
        report.incomplete(f'equity: {e.message or e}')

    try:
        positions = await ex.positions()      # all: not every exchange reads a list of symbols in one call
        if syms is not None:
            positions = [p for p in positions if p.symbol in syms]
        report.positions = positions
        report.unrealized_pnl = sum(p.unrealized_pnl or 0.0 for p in positions)
        if any(p.unrealized_pnl is None for p in positions):
            report.incomplete('unrealized_pnl: the exchange did not return PnL for some positions')
    except BytxError as e:
        report.incomplete(f'positions: {e.message or e}')

    try:
        result: IncomeResult = await module.income(ex, start, end, symbols=syms, positions=report.positions)
    except BytxError as e:
        result = IncomeResult()
        result.unavailable = {'realized', 'fee', 'funding'}
        result.incomplete(f'income: {e.message or e}')
    for item in result.items:
        item.symbol = market_symbol(ex, item.symbol, item.market)
    if syms is not None:
        wanted = set(syms)
        result.items = [i for i in result.items if i.symbol in wanted]
    for item in result.items:
        report.totals.setdefault(item.currency, IncomeTotals(item.currency)).add(item)
    if keep_items:
        report.items = sorted(result.items, key=lambda i: i.ts)
    report.unavailable = set(result.unavailable)
    for reason in result.issues:
        report.incomplete(reason)
    if not result.complete:
        report.complete = False
    return report


async def portfolio(pool: 'AccountPool', *, since: Optional[int], until: Optional[int] = None,
                    names: Optional[Iterable[str]] = None, concurrency: int = 10,
                    keep_items: bool = False) -> PortfolioReport:
    """Portfolio across pool accounts. An account where everything failed stays in the report with ``error``
    and ``complete=False`` — the pool total is not passed off as complete."""
    start, end = _window(since, until)
    results = await pool.gather(
        lambda ex: account_report(ex, since=start, until=end, keep_items=keep_items),
        names=names, concurrency=concurrency)
    report = PortfolioReport(since=start, until=end)
    for acc_name, value in results.items():
        if isinstance(value, AccountReport):
            value.name = acc_name
            report.accounts.append(value)
        else:
            cfg = pool.config(acc_name)
            failed = AccountReport(name=acc_name, exchange=cfg.exchange, since=start, until=end, demo=cfg.demo,
                                   error=value, unavailable={'realized', 'fee', 'funding'})
            failed.incomplete(f'account not read: {value}')
            report.accounts.append(failed)
    return report


__all__ = ['AccountReport', 'CSV_FIELDS', 'IncomeItem', 'IncomeResult', 'IncomeTotals', 'PortfolioReport',
           'USD_LIKE', 'account_report', 'market_symbol', 'portfolio']
