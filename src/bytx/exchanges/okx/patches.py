"""ccxt OKX patches at the INSTANCE level.

They used to be applied to the ``ccxt.okx`` class on module import, changing
ccxt behavior across the user's whole process. A public SDK must not do that, so
the patch is applied only to instances the SDK creates itself.
"""
import types

from loguru import logger


def patch_okx_instance(exchange) -> None:
    """Installs safe parse_market/parse_markets and fetch_my_liquidations on an OKX instance.

    Does nothing for other exchanges. Repeated calls are safe.
    """
    if getattr(exchange, 'id', None) not in ('okx', 'myokx', 'okxus'):
        return
    if getattr(exchange, '_bytx_okx_patched', False):
        return
    cls = type(exchange)
    original_parse_market = cls.parse_market
    original_parse_markets = cls.parse_markets

    def safe_parse_market(self, market):
        """Skips OKX markets with None base/quote (otherwise TypeError in ccxt)."""
        try:
            return original_parse_market(self, market)
        except TypeError as e:
            if 'NoneType' in str(e):
                market_id = market.get('instId', 'unknown') if isinstance(market, dict) else 'unknown'
                logger.debug(f"Skipping unparseable OKX market: {market_id} - {e}")
                return None
            raise

    def safe_parse_markets(self, markets):
        result = original_parse_markets(self, markets)
        filtered = [m for m in result if m is not None]
        skipped = len(result) - len(filtered)
        if skipped > 0:
            logger.debug(f"Filtered out {skipped} unparseable OKX market(s)")
        return filtered

    async def fetch_my_liquidations_okx(self, symbol=None, since=None, limit=None, params={}):
        """
        Fetch OKX liquidation history via /api/v5/trade/fills.

        Args:
            symbol: Unified symbol (e.g. 'BTC/USDT:USDT')
            since: Period start timestamp (milliseconds)
            limit: Max number of records (default 100, max 100)
            params: Extra parameters

        Returns:
            List[dict]: List of liquidations in the unified format
        """
        await self.load_markets()

        request = {
            'instType': 'SWAP',  # Instrument type: SWAP for perpetuals
        }

        # Add symbol if given
        if symbol:
            market = self.market(symbol)
            request['instId'] = market['id']  # e.g. 'BTC-USDT-SWAP'

        # Add the time filter
        if since is not None:
            request['begin'] = str(since)

        # OKX accepts limit from 1 to 100
        if limit is not None:
            request['limit'] = str(min(int(limit), 100))

        # Call /api/v5/trade/fills
        response = await self.request(
            path='trade/fills',
            api='private',
            method='GET',
            params=request
        )

        # Parse the response and keep liquidations only
        # subType for liquidations:
        # 104: Liquidation long
        # 105: Liquidation short
        # 106: Liquidation buy
        # 107: Liquidation sell
        # 100: Partial liquidation close long
        # 101: Partial liquidation close short
        # 102: Partial liquidation buy
        # 103: Partial liquidation sell
        liquidation_subtypes = {'100', '101', '102', '103', '104', '105', '106', '107'}

        data = self.safe_value(response, 'data', [])
        liquidations = []

        for item in data:
            sub_type = self.safe_string(item, 'subType')

            # Skip if not a liquidation
            if sub_type not in liquidation_subtypes:
                continue

            timestamp = self.safe_integer(item, 'ts')
            inst_id = self.safe_string(item, 'instId')
            market_obj = None
            symbol_parsed = None

            # Try to find the market by instId
            if inst_id and inst_id in self.markets_by_id:
                market_obj = self.markets_by_id[inst_id]
                symbol_parsed = market_obj['symbol']

            pos_side = self.safe_string(item, 'posSide')  # 'long', 'short', or 'net'
            side = self.safe_string(item, 'side')  # 'buy' or 'sell'

            # OKX fills already carry the correct side (side of the closing trade)
            # long liquidation -> side='sell'
            # short liquidation -> side='buy'

            order_id = self.safe_string(item, 'ordId')
            trade_id = self.safe_string(item, 'tradeId')

            liquidations.append({
                'info': item,
                'symbol': symbol_parsed,
                'contracts': self.safe_float(item, 'fillSz'),
                'amount': self.safe_float(item, 'fillSz'),
                'price': self.safe_float(item, 'fillPx'),
                'timestamp': timestamp,
                'datetime': self.iso8601(timestamp) if timestamp else None,
                'side': side,  # 'buy' or 'sell' - already the correct side from OKX
                'id': trade_id,
                'order': order_id,  # Keep order_id for lookup in trade_history
            })

        return liquidations

    exchange.parse_market = types.MethodType(safe_parse_market, exchange)
    exchange.parse_markets = types.MethodType(safe_parse_markets, exchange)
    exchange.fetch_my_liquidations = types.MethodType(fetch_my_liquidations_okx, exchange)
    exchange._bytx_okx_patched = True
