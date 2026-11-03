"""
Bybit Position History Module

This module provides functionality to work with Bybit's position history.
Bybit has a complex timestamp system:
- Closed-PnL record timestamp = createdTime = close time (fill of the closing order)
- Position ID = transaction/order ID
- Closed positions need special matching logic
- Need to reconstruct trade_history from executions API

Pagination (review: «no paginated history loading for Bybit»):
- v5/position/closed-pnl: startTime..endTime window at most 7 days, limit <= 100,
  depth 2 years. We walk 7-day windows from newest to oldest; within a window we move
  endTime to the page's min(createdTime) (via the public ccxt fetch_positions_history
  with params.until -> endTime).
- v5/execution/list: same 7-day window, limit <= 100, cursor result.nextPageCursor.
  We use the raw endpoint WITHOUT execType (ccxt fetch_my_trades hardcodes
  execType=Trade and loses liquidation/ADL) and keep locally
  Trade/BustTrade/AdlTrade.
- One HistoryBudget per top-level call; incompleteness — warning + last_history_incomplete.
"""

import time
from decimal import Decimal, InvalidOperation

from loguru import logger
from bytx.exchanges._records.position import TradeHistory, OrderFee

from bytx.utils.history_paging import (
    DAY_MS,
    _BYBIT_LIMIT,
    _BYBIT_MAX_AGE_MS,
    _BYBIT_WINDOW_MS,
    _RECON_MAX_LOOKBACK_MS,
    HistoryBudget,
    get_history_semaphore,
    log_fetch,
    new_budget,
)

# Executions that change the position. Funding/Settle/Delivery etc. don't.
_POSITION_EXEC_TYPES = frozenset({'Trade', 'BustTrade', 'AdlTrade'})
_EPS = Decimal('1e-12')
# Close-time match tolerance (closed-pnl createdTime vs lastUpdateTimestamp).
_CLOSE_TIME_TOLERANCE_MS = 60_000
# Partial closes of one Bybit position share one avgEntryPrice; a different one means another cycle.
_SAME_ENTRY_REL = 1e-9
# The position snapshot was taken AFTER the closed-pnl record if
# lastUpdateTimestamp >= createdTime - tolerance (clock skew / rounding).
_SNAPSHOT_AFTER_EPS_MS = 1_500


def _now_ms() -> int:
    return int(time.time() * 1000)


def _dec(value) -> Decimal | None:
    if value is None or value == '':
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _int(value) -> int | None:
    try:
        return int(value) if value not in (None, '') else None
    except (TypeError, ValueError):
        return None


def _info(obj) -> dict:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj.get('info') or {}
    return getattr(obj, 'info', None) or {}


class BybitPositionHistoryMatcher:
    """
    Handles Bybit position history matching and retrieval.

    Bybit specifics:
    - Uses native fetch_positions_history() API (v5/position/closed-pnl)
    - Closed-PnL timestamp is the close time (createdTime)
    - Position ID is the transaction/order ID
    - Need careful matching for recently closed positions
    """

    def __init__(self, exchange, symbol: str, bot_id: str,
                 max_calls: int | None = None, deadline_s: float | None = None):
        """
        Args:
            exchange: CCXT exchange instance
            symbol: Trading symbol (e.g., 'BTC/USDT:USDT')
            bot_id: Bot identifier for logging
            max_calls / deadline_s: override of the default budget (40 / 25 s)
        """
        self.exchange = exchange
        self.symbol = symbol
        self.bot_id = bot_id
        self._max_calls = max_calls
        self._deadline_s = deadline_s
        self.last_history_incomplete = False

    def new_budget(self) -> HistoryBudget:
        return new_budget('bybit', self._max_calls, self._deadline_s)

    def _log(self):
        return logger.bind(bot_id=self.bot_id)

    # ------------------------------------------------------------------ closed-pnl

    async def fetch_position_history(self, since: int | None = None, until: int | None = None,
                                     budget: HistoryBudget | None = None) -> list:
        """
        Fetch closed positions (v5/position/closed-pnl) from Bybit API.

        Args:
            since: period start, ms. None — the old single call (last 7 days, up to 50).
            until: period end, ms (default — now).
            budget: shared budget of the top-level call.

        Returns:
            List of ccxt positions from newest to oldest. If incomplete —
            warning and self.last_history_incomplete = True.
        """
        self.last_history_incomplete = False
        if since is None:
            try:
                data = await self.exchange.fetch_positions_history(symbols=[self.symbol], since=since)
                data = data or []
                self._log().debug(
                    f"[{self.bot_id}]\tFetched {len(data)} closed position(s) from Bybit"
                )
                if len(data) >= 50:
                    # Without since ccxt/Bybit returns only the last 7 days and at most 50 records.
                    self.last_history_incomplete = True
                    self._log().warning(
                        f"[{self.bot_id}]\t⚠️ Bybit closed-pnl without since returned a full page "
                        f"({len(data)}); history may be incomplete"
                    )
                return data
            except Exception as e:
                self._log().error(f"[{self.bot_id}]\tError fetching Bybit position history: {e}")
                return []

        budget = budget or self.new_budget()
        items, complete = await self._fetch_closed_pnl(int(since), until, budget)
        self.last_history_incomplete = not complete
        return items

    async def _fetch_closed_pnl(self, since: int, until: int | None,
                                budget: HistoryBudget) -> tuple[list, bool]:
        """7-day windows from until (now) back to since; within a window — endTime shift."""
        now = _now_ms()
        since = max(since, now - _BYBIT_MAX_AGE_MS)
        end = int(until) if until is not None else now
        sem = get_history_semaphore('bybit')

        collected: list = []
        seen: set = set()
        complete = True
        pages = 0
        reached = end
        window_end = end

        while window_end >= since and complete:
            window_start = max(since, window_end - _BYBIT_WINDOW_MS)
            page_end = window_end
            while True:
                async with sem:
                    if not budget.take():
                        complete = False
                        break
                    try:
                        page = await self.exchange.fetch_positions_history(
                            [self.symbol], since=window_start, limit=_BYBIT_LIMIT,
                            params={'until': page_end},
                        )
                    except Exception as e:
                        complete = False
                        budget.mark_incomplete('error')
                        self._log().error(
                            f"[{self.bot_id}]\tError fetching Bybit closed-pnl page {pages + 1} "
                            f"[{window_start}..{page_end}]: {e}"
                        )
                        break
                pages += 1
                page = page or []
                for item in page:
                    inf = _info(item)
                    key = (inf.get('orderId'), inf.get('createdTime'), inf.get('closedSize'))
                    if key in seen:
                        continue
                    seen.add(key)
                    collected.append(item)

                if len(page) < _BYBIT_LIMIT:
                    reached = window_start
                    break
                created = [
                    _int(_info(p).get('createdTime')) or _int(p.get('timestamp')) for p in page
                ]
                created = [c for c in created if c is not None]
                if not created:
                    reached = window_start
                    break
                min_ct, max_ct = min(created), max(created)
                if min_ct == max_ct or min_ct >= page_end:
                    # Whole page in one ms: an inclusive shift won't advance.
                    self._log().warning(
                        f"[{self.bot_id}]\t⚠️ Bybit closed-pnl: full page within one ms ({min_ct}); "
                        f"records of that ms beyond the page may be lost"
                    )
                    next_end = min(min_ct, page_end) - 1
                else:
                    # Inclusive: same-ms records that didn't fit the page come
                    # in the next page; seen removes duplicates.
                    next_end = min_ct
                reached = next_end
                if next_end < window_start:
                    break
                page_end = next_end
            window_end = window_start - 1

        collected.sort(
            key=lambda p: _int(_info(p).get('createdTime')) or _int(p.get('timestamp')) or 0,
            reverse=True,
        )
        log_fetch(self.bot_id, 'bybit closed-pnl', self.symbol, pages, len(collected),
                  budget.elapsed_ms(), complete, budget.reason, reached)
        return collected, complete

    async def _fix_position_timestamps(self, positions: list):
        """Kept for compatibility: no-op (open time is set by reconstruct)."""
        return None

    # ------------------------------------------------------------------ executions

    async def _fetch_executions(self, start: int, end: int, budget: HistoryBudget,
                                stop_fn=None, extra: dict | None = None) -> tuple[list, bool]:
        """
        Executions for a symbol over [start, end] via raw v5/execution/list without execType.

        Windows of 7d−1ms back from end to start; within a window startTime/endTime are fixed,
        pages follow the result.nextPageCursor cursor. Dedup by execId.
        stop_fn(trades_so_far) -> True stops fetching early.

        Returns:
            (ccxt trades in ascending time order, complete)
        """
        try:
            await self.exchange.load_markets()
        except Exception:
            pass
        market = self.exchange.market(self.symbol)
        category = 'linear' if market.get('linear', True) else 'inverse'
        now = _now_ms()
        start = max(int(start), now - _BYBIT_MAX_AGE_MS)
        end = int(end)
        sem = get_history_semaphore('bybit')

        raw_ids: set = set()
        trades: list = []
        complete = True
        pages = 0
        reached = end
        stopped = False
        window_end = end

        while window_end >= start and complete and not stopped:
            window_start = max(start, window_end - _BYBIT_WINDOW_MS)
            cursor = None
            used_cursors: set = set()
            while True:
                request = {
                    'category': category,
                    'symbol': market['id'],
                    'startTime': window_start,
                    'endTime': window_end,
                    'limit': _BYBIT_LIMIT,
                }
                if extra:
                    request.update(extra)
                if cursor:
                    request['cursor'] = cursor
                async with sem:
                    if not budget.take():
                        complete = False
                        break
                    try:
                        response = await self.exchange.privateGetV5ExecutionList(request)
                    except Exception as e:
                        complete = False
                        budget.mark_incomplete('error')
                        self._log().error(
                            f"[{self.bot_id}]\tError fetching Bybit executions page {pages + 1} "
                            f"[{window_start}..{window_end}]: {e}"
                        )
                        break
                pages += 1
                result = (response or {}).get('result') or {}
                raw = result.get('list') or []
                new_raw = []
                for r in raw:
                    eid = r.get('execId')
                    if eid in raw_ids:
                        continue
                    raw_ids.add(eid)
                    new_raw.append(r)
                usable = [r for r in new_raw if r.get('execType') in _POSITION_EXEC_TYPES]
                if usable:
                    trades.extend(self.exchange.parse_trades(usable, market))
                if raw:
                    oldest = min((_int(r.get('execTime')) or window_end) for r in raw)
                    reached = min(reached, oldest)
                if stop_fn is not None and stop_fn(trades):
                    stopped = True
                    break
                next_cursor = result.get('nextPageCursor')
                if not raw or not next_cursor or next_cursor in used_cursors or not new_raw:
                    reached = min(reached, window_start)
                    break
                used_cursors.add(next_cursor)
                cursor = next_cursor
            window_end = window_start - 1

        trades.sort(key=lambda t: (t.get('timestamp') or 0, _int(_info(t).get('seq')) or 0))
        log_fetch(self.bot_id, 'bybit executions', self.symbol, pages, len(trades),
                  budget.elapsed_ms(), complete or stopped, budget.reason, reached)
        return trades, (complete or stopped)

    # ------------------------------------------------------------------ matching

    @staticmethod
    def _close_time(closed_pos) -> int | None:
        """Close time of a closed-pnl record: info.createdTime (fallback timestamp)."""
        ct = _int(_info(closed_pos).get('createdTime'))
        if ct is not None:
            return ct
        return getattr(closed_pos, 'timestamp', None) if not isinstance(closed_pos, dict) \
            else closed_pos.get('timestamp')

    @staticmethod
    def _same_entry(c, ref) -> bool:
        return bool(c.entryPrice) and abs(c.entryPrice - ref) <= abs(ref) * _SAME_ENTRY_REL

    @staticmethod
    def _series_final(position, candidates: list, start: int, include_start: bool):
        """Final record of a series of partial closes of one cycle starting at candidates[start].

        A series is consecutive (by close time) records of the position side with the same
        avgEntryPrice as candidates[start] (partial closes of one Bybit position don't
        change it); records of the other side (hedge) are skipped, a same-side record with a
        different entryPrice is a new cycle and ends the series.

        Closed volume (info.closedSize, else contracts) is accumulated: from
        candidates[start] itself (include_start — snapshot taken before it, contracts = cycle volume)
        or from the next record (snapshot after it, contracts = remainder). The series
        ends at the record where the accumulated >= position.contracts; records after that are
        a new cycle. Not reached — the last record of the series.

        Returns:
            (record, exact): exact — the accumulated equals contracts exactly (within 1e-8).
        """
        first = candidates[start]
        ref = first.entryPrice
        target = position.contracts
        if not ref or target is None:
            return first, False

        def _closed(c) -> float:
            v = _dec(_info(c).get('closedSize'))
            if v is not None:
                return float(v)
            return float(c.contracts or 0)

        acc = _closed(first) if include_start else 0.0
        last = first
        if not (include_start and acc >= target - 1e-8):
            for c in candidates[start + 1:]:
                if c.side != position.side:
                    continue
                if not BybitPositionHistoryMatcher._same_entry(c, ref):
                    break
                last = c
                acc += _closed(c)
                if acc >= target - 1e-8:
                    break
        exact = abs(acc - target) <= 1e-8
        if not include_start and last is first:
            exact = False      # no series record after it — there was no remainder
        return last, exact

    def _resolve_series(self, position, candidates: list, scored: list, pick, by_time: bool,
                        open_time):
        """Final record of its cycle's series for the chosen record pick (see find_matching_position, item 5)."""
        score, diff, idx, chosen, details = pick
        # snapshot after pick (LU >= closeTime - eps): contracts may be the remainder after
        # it; snapshot before pick — contracts are closed starting from pick itself
        snapshot_after = False
        if by_time:
            ct = self._close_time(chosen)
            if ct is not None:
                snapshot_after = position.lastUpdateTimestamp >= ct - _SNAPSHOT_AFTER_EPS_MS
        if 'contracts' not in details:
            # partial record: contracts — cycle volume (snapshot before it) or remainder (after)
            whole, whole_exact = self._series_final(position, candidates, idx, True)
            if snapshot_after:
                rest, rest_exact = self._series_final(position, candidates, idx, False)
                # snapshot after pick (incl. the tolerance zone ct - eps <= LU < ct, as in the
                # matched-volume branch) — remainder first, then cycle volume
                for rec, exact in ((rest, rest_exact), (whole, whole_exact)):
                    if exact:
                        return rec
            return whole

        # volume matched: the record is a full close unless partiality is confirmed
        ref = chosen.entryPrice
        full_idx = {s[2] for s in scored if 'contracts' in s[4]}
        confirmed = False
        if by_time:
            # the snapshot was updated no earlier than pick (LU >= ct - eps) and the position in it is open:
            # it was still alive after pick — pick can't be its final (partial TP),
            # contracts is the remainder after it. Snapshot before pick (LU < ct - eps): contracts =
            # pick volume — the record is the final.
            confirmed = snapshot_after and bool(position.contracts) and position.contracts > 0
        elif len(full_idx) > 1:
            # the next same-side record with the same entry is partial and comes before
            # any other full record
            for j in range(idx + 1, len(candidates)):
                c = candidates[j]
                if c.side != position.side:
                    continue
                if j in full_idx:
                    break
                if self._same_entry(c, ref):
                    confirmed = True
                break
        if not confirmed:
            return chosen
        rest, rest_exact = self._series_final(position, candidates, idx, False)
        return rest if rest_exact else chosen

    def find_matching_position(self, position, closed_positions: list, open_time: int | None = None):
        """
        Find matching closed position in history.

        Bybit matching strategy:
        1. Candidates whose close time is earlier than the position open (open_time /
           position.timestamp) are dropped.
        2. Try to match by ID.
        3. Side (required) + contracts + entry price; close time =
           info.createdTime (not timestamp, which reconstruction overwrites).
        4. If position has lastUpdateTimestamp, closeTime within 60 s is
           required (if there are no such candidates — fallback choice with a warning).
           Without lastUpdateTimestamp — the record with the HIGHEST score, on a tie
           the EARLIEST after open (Bybit writes closed-pnl for every
           closing order: partial TPs match side+price, the final one also matches volume).
        5. If the chosen record matched on price, it may be a partial close —
           the final of the same cycle's close series is looked up (_series_final):
           - volume did NOT match: contracts = cycle volume (accumulating from the record itself); if
             the record was chosen by time and the snapshot was taken after it (lastUpdateTimestamp >=
             closeTime - 1.5 s), contracts = remainder is also checked (accumulating from the
             next record). The interpretation where the sum equals contracts exactly is taken
             (remainder first, then cycle volume); otherwise — cycle volume. Snapshot before the record
             (LU < closeTime - 1.5 s) — cycle volume only (the snapshot couldn't see the
             remainder after the record).
           - volume matched: the record is a full close (takes priority, as in 854904e), except
             confirmed partiality: (a) chosen by time and the snapshot was taken after it
             (LU >= closeTime - 1.5 s) with contracts > 0 — the position outlived the record, which can't
             be its final; snapshot before the record (LU < closeTime - 1.5 s) — the record
             is the final; (b) without a time
             hit and with several full matches — the next same-side record with the same entry
             is partial and comes before any other full record. Then the final is the record
             at which the remainder is closed exactly; no match — the record itself.

        Known limitations (closed-pnl data is indistinguishable):
        (b) Without lastUpdateTimestamp (or with the record outside the time window) a TP for exactly the snapshot
            volume (TP 50%, equal TPs) looks the same as a full cycle close followed by a next cycle
            of the same volume with the same avgEntryPrice. The TP record itself is returned (a full
            match takes priority). With LU in the window the data is distinguishable (see item 5 (a)).
        (e) A snapshot before a full close in the tolerance zone (closeTime - 1.5 s <= LU <
            closeTime) is treated as a snapshot after: if the next cycle with the same entry
            closes contracts exactly, its final is returned.
        (c) A DCA add after a partial TP changes avgEntryPrice within the cycle
            (within 0.1%): the series ends at the entry change, the final isn't found —
            the last record with the previous entry is returned.
        (d) Snapshot after pick (LU >= ct) or without LU, and the next cycle with the same entry exactly
            closes the remainder — the next cycle's final is returned.

        Returns:
            Matched PositionModel or None
        """
        if not closed_positions:
            self._log().warning(f"[{self.bot_id}]\t⚠️ No closed positions available for matching")
            return None

        if open_time is None:
            open_time = getattr(position, 'timestamp', None)

        self._log().debug(
            f"[{self.bot_id}]\tSearching for closed position (Bybit): "
            f"id={position.id}, side={position.side}, "
            f"contracts={position.contracts}, entryPrice={position.entryPrice}, "
            f"open={open_time}, lastUpdateTimestamp={position.lastUpdateTimestamp}"
        )

        candidates = sorted(closed_positions, key=lambda x: self._close_time(x) or 0)
        if open_time:
            candidates = [c for c in candidates
                          if self._close_time(c) is None or self._close_time(c) >= open_time]

        # Priority 1: ID
        if position.id:
            for closed_pos in candidates:
                if closed_pos.id == position.id:
                    self._log().info(f"[{self.bot_id}]\t✅ Found closed position by ID: {position.id}")
                    return closed_pos

        last_update = position.lastUpdateTimestamp
        scored = []          # (score, diff, idx, pos, details)
        best = (0, None, [])
        for idx, closed_pos in enumerate(candidates):
            score = 0
            details = []
            if closed_pos.side != position.side:
                continue
            score += 2
            details.append("side")

            if closed_pos.contracts is not None and position.contracts is not None and \
                    abs(closed_pos.contracts - position.contracts) < 1e-8:
                score += 2
                details.append("contracts")

            if position.entryPrice and closed_pos.entryPrice:
                price_diff = abs(closed_pos.entryPrice - position.entryPrice) / position.entryPrice
                if price_diff <= 0.001:
                    score += 2
                    details.append("price")

            diff = None
            ct = self._close_time(closed_pos)
            if last_update and ct:
                diff = abs(ct - last_update)
                if diff <= 20000:
                    score += 2
                    details.append(f"closeTime(diff={diff}ms)")
                elif diff <= _CLOSE_TIME_TOLERANCE_MS:
                    score += 1
                    details.append(f"closeTime~(diff={diff}ms)")

            if score > best[0]:
                best = (score, closed_pos, details)
            if score >= 4:
                scored.append((score, diff, idx, closed_pos, details))

        if scored:
            # highest score, on a tie — the earliest record after open
            best_earliest = min(scored, key=lambda s: (-s[0], s[2]))
            if last_update:
                in_time = [s for s in scored if s[1] is not None and s[1] <= _CLOSE_TIME_TOLERANCE_MS]
                if in_time:
                    pick = min(in_time, key=lambda s: (-s[0], s[1], s[2]))
                else:
                    pick = best_earliest
                    self._log().warning(
                        f"[{self.bot_id}]\t⚠️ No Bybit closed-pnl within {_CLOSE_TIME_TOLERANCE_MS}ms of "
                        f"lastUpdateTimestamp={last_update}; falling back to best-scored earliest match after open"
                    )
            else:
                pick = best_earliest
            score, diff, _, chosen, details = pick
            if 'price' in details:
                # the record may be a partial close (partial TP) — the final of its cycle's series
                by_time = last_update is not None and diff is not None and diff <= _CLOSE_TIME_TOLERANCE_MS
                final = self._resolve_series(position, candidates, scored, pick, by_time, open_time)
                if final is not chosen:
                    chosen = final
                    details = details + [f"seriesFinal(closeTime={self._close_time(final)})"]
            self._log().info(
                f"[{self.bot_id}]\t✅ Found closed position with {score} points: {', '.join(details)}"
            )
            return chosen

        if best[1] is not None:
            bm = best[1]
            self._log().warning(
                f"[{self.bot_id}]\t⚠️ Best match had only {best[0]} points ({', '.join(best[2])}): "
                f"side={bm.side}, contracts={bm.contracts}, "
                f"entryPrice={bm.entryPrice}, closeTime={self._close_time(bm)}"
            )
        return None

    # ------------------------------------------------------------------ reconstruct

    async def reconstruct_position_with_trades(self, position, trades: list | None = None,
                                               open_time_hint: int | None = None,
                                               budget: HistoryBudget | None = None):
        """
        Reconstruct position with full trade history.

        1. All fills of the closing order (info.orderId of the closed-pnl record).
        2. Walk back from the first closing fill: net = closed volume; trades on the closing
           side with closedSize>0 are earlier partial closes (net += closedSize);
           trades on the opening side — open_part = execQty − closedSize (net −= open_part).
           net <= 0 — the opening boundary is found, timestamp = that trade's time.
        3. If the boundary isn't found (budget/depth/error), trade_history and timestamp
           are NOT touched, warning 'history incomplete', last_history_incomplete = True.

        PnL and fee are not recomputed: taken from closed-pnl.

        Args:
            position: PositionModel from v5/position/closed-pnl
            trades: pre-fetched executions (ccxt trades); None — fetch them here
            open_time_hint: known open time (lower bound − 1 day)
            budget: shared budget of the top-level call
        """
        try:
            if not position:
                return position

            info = _info(position)
            close_order_id = info.get('orderId')
            if not close_order_id:
                return position

            budget = budget or self.new_budget()
            close_ct = _int(info.get('createdTime')) or position.lastUpdateTimestamp or position.timestamp
            if not close_ct:
                return position
            position_side = (position.side or '').lower()
            close_side = 'sell' if position_side == 'long' else 'buy'
            open_side = 'buy' if position_side == 'long' else 'sell'

            lower = close_ct - _RECON_MAX_LOOKBACK_MS
            if open_time_hint:
                lower = max(lower, int(open_time_hint) - DAY_MS)

            # (a) all fills of the closing order
            if trades is not None:
                close_fills = [t for t in trades if t.get('order') == close_order_id]
            else:
                # One 7-day window ending just after the record's createdTime.
                fetched, _ = await self._fetch_executions(
                    close_ct + _CLOSE_TIME_TOLERANCE_MS - _BYBIT_WINDOW_MS,
                    close_ct + _CLOSE_TIME_TOLERANCE_MS, budget,
                    extra={'orderId': close_order_id},
                )
                close_fills = [t for t in fetched if t.get('order') == close_order_id]
            if not close_fills:
                self._mark_incomplete(f"close order {close_order_id} fills not found")
                return position
            close_fills.sort(key=lambda t: (t.get('timestamp') or 0, _int(_info(t).get('seq')) or 0))
            first_close_ts = close_fills[0].get('timestamp') or close_ct

            has_closed_size = all(_dec(_info(t).get('closedSize')) is not None for t in close_fills)
            if not has_closed_size:
                return await self._legacy_reconstruct(
                    position, close_fills, trades, lower, first_close_ts, budget,
                    position_side, open_side,
                )

            net0 = sum((_dec(_info(t).get('closedSize')) for t in close_fills), Decimal(0))
            record_closed = _dec(info.get('closedSize'))
            if record_closed is not None and abs(record_closed - net0) > _EPS:
                self._log().warning(
                    f"[{self.bot_id}]\t⚠️ Bybit close order {close_order_id}: fills closedSize={net0} "
                    f"!= closed-pnl closedSize={record_closed}"
                )

            def _earlier(src):
                return [
                    t for t in src
                    if t.get('order') != close_order_id
                    and lower <= (t.get('timestamp') or 0) <= first_close_ts
                ]

            def _stop(src) -> bool:
                return self._walk_back(_earlier(src), net0, close_side, open_side)[0] is not None

            if trades is not None:
                earlier = _earlier(trades)
            else:
                fetched, _ = await self._fetch_executions(lower, first_close_ts, budget, stop_fn=_stop)
                earlier = _earlier(fetched)

            open_ts, parts = self._walk_back(earlier, net0, close_side, open_side)
            if open_ts is None:
                self._mark_incomplete(
                    f"open boundary not found for close order {close_order_id} "
                    f"(searched back to {lower}, budget reason={budget.reason})"
                )
                return position

            entries = [(t, amt) for t, amt in parts]
            for t in close_fills:
                entries.append((t, _dec(_info(t).get('closedSize'))))
            entries.sort(key=lambda e: (e[0].get('timestamp') or 0, _int(_info(e[0]).get('seq')) or 0))

            trade_history = [self._to_trade_history(t, amt, position_side) for t, amt in entries]
            if hasattr(position, 'trade_history'):
                position.trade_history = trade_history
            if hasattr(position, 'timestamp'):
                position.timestamp = open_ts

            opening = sum(1 for t, _ in parts if (t.get('side') or '').lower() == open_side)
            self._log().info(
                f"[{self.bot_id}]\t✅ {opening} opening + {len(trade_history) - opening} closing "
                f"= {len(trade_history)} trades"
            )
            return position

        except Exception as e:
            self._log().exception(f"[{self.bot_id}]\tError: {e}")
            return position

    def _walk_back(self, earlier: list, net0: Decimal, close_side: str, open_side: str):
        """
        Walks the net position back from the first closing fill to zero.

        Returns:
            (open_ts | None, [(trade, amount Decimal), ...]) — position trades before the close.
        """
        net = net0
        parts = []
        ordered = sorted(
            earlier,
            key=lambda t: (t.get('timestamp') or 0, _int(_info(t).get('seq')) or 0),
            reverse=True,
        )
        for t in ordered:
            inf = _info(t)
            side = (t.get('side') or '').lower()
            qty = _dec(inf.get('execQty'))
            if qty is None:
                qty = _dec(t.get('amount')) or Decimal(0)
            closed = _dec(inf.get('closedSize')) or Decimal(0)
            if side == close_side:
                if closed > _EPS:
                    # An earlier partial close of this position.
                    net += closed
                    parts.append((t, closed))
                # closedSize=0 on the closing side — opening of the opposite hedge position: skip.
                continue
            if side != open_side:
                continue
            open_part = qty - closed
            if open_part <= _EPS:
                # Close of the opposite hedge position (or a previous cycle): not our trade.
                continue
            net -= open_part
            parts.append((t, open_part))
            if net <= _EPS:
                if net < -_EPS:
                    self._log().warning(
                        f"[{self.bot_id}]\t⚠️ Bybit reconstruct overshoot at {t.get('id')}: net={net}"
                    )
                return t.get('timestamp'), parts
        return None, parts

    async def _legacy_reconstruct(self, position, close_fills, trades, lower, first_close_ts,
                                  budget, position_side, open_side):
        """Old side-based heuristic (no closedSize in the response), no break on close."""
        self._log().warning(
            f"[{self.bot_id}]\t⚠️ Bybit executions without closedSize — falling back to side heuristic"
        )
        legacy_lower = max(lower, first_close_ts - _BYBIT_WINDOW_MS)
        close_oid = close_fills[0].get('order')
        if trades is None:
            trades, _ = await self._fetch_executions(legacy_lower, first_close_ts, budget)
        opening = sorted(
            [t for t in trades
             if t.get('order') != close_oid
             and legacy_lower <= (t.get('timestamp') or 0) <= first_close_ts
             and (t.get('side') or '').lower() == open_side],
            key=lambda t: t.get('timestamp') or 0,
        )
        history = [self._to_trade_history(t, None, position_side) for t in opening + close_fills]
        if hasattr(position, 'trade_history'):
            position.trade_history = history
        if opening and hasattr(position, 'timestamp'):
            position.timestamp = opening[0].get('timestamp', position.timestamp)
        return position

    def _mark_incomplete(self, reason: str) -> None:
        self.last_history_incomplete = True
        self._log().warning(f"[{self.bot_id}]\t⚠️ Bybit history incomplete: {reason}")

    def _to_trade_history(self, trade: dict, amount: Decimal | None, position_side: str) -> TradeHistory:
        """ccxt trade -> TradeHistory; amount — the share of the trade belonging to the position."""
        full = float(trade.get('amount') or 0)
        amt = float(amount) if amount is not None else full
        ratio = (amt / full) if full else 1.0
        price = trade.get('price', 0) or 0
        fee_obj = None
        trade_fee = trade.get('fee')
        if isinstance(trade_fee, dict) and trade_fee:
            cost = trade_fee.get('cost', 0) or 0
            fee_obj = OrderFee(cost=float(cost) * ratio, currency=trade_fee.get('currency', 'USDT') or 'USDT')
        cost = trade.get('cost')
        cost = amt * float(price) if (cost is None or ratio != 1.0) else cost
        return TradeHistory(
            exchange=str(self.exchange).lower(),
            id=trade.get('id', '') or '',
            order_id=trade.get('order', ''),
            timestamp=trade.get('timestamp', 0),
            datetime=trade.get('datetime', ''),
            symbol=trade.get('symbol', self.symbol),
            side=(trade.get('side') or '').lower(),
            price=price,
            amount=amt,
            cost=cost,
            taker_or_maker=trade.get('takerOrMaker', ''),
            type=trade.get('type', ''),
            position_side=position_side,
            fee=fee_obj,
            realized_pnl=None,
        )
