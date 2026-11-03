from decimal import Decimal, ROUND_DOWN, ROUND_UP, InvalidOperation
from loguru import logger

def calculate_contract_size(usdt_volume, price, market, exchange):
    """
    Converts a USDT amount into a contract size on a futures market.

    :param usdt_volume: Amount in USDT (number)
    :param price: Price of the trading pair (e.g. BTC/USDT)
    :param market: Market info from self.exchange.market(symbol)
    :param exchange: Exchange name ('binance', 'bybit', 'okx')
    :return: Contract size (float) or None if it is below the minimum
    """
    try:
        usdt_volume = float(usdt_volume)
        price = float(price)
        contract_size = float(market.get('contractSize', 1.0))
    except (TypeError, ValueError) as e:
        logger.error(f"Failed to convert inputs to float: {e}")
        return None

    lot_step = None
    min_qty = None

    try:
        match exchange.lower():
            case 'binance':
                for f in market['info'].get('filters', []):
                    if f.get('filterType') == 'LOT_SIZE':
                        lot_step = float(f['stepSize'])
                        min_qty = float(f['minQty'])
                        break

            case 'bybit':
                lot_info = market['info'].get('lotSizeFilter', {})
                lot_step = float(lot_info.get('qtyStep', 0.001))
                min_qty = float(lot_info.get('minOrderQty', 0.001))

            case 'okx':
                # OKX uses lotSz and minSz in info
                info = market.get('info', {})
                if info:
                    # lotSz - lot size (step size)
                    lot_step = float(info.get('lotSz', market['precision'].get('amount', 0.01)))
                    # minSz - minimum order size
                    min_qty = float(info.get('minSz', market['limits']['amount'].get('min', 0.01)))
                else:
                    # Fallback to the standard fields
                    lot_step = float(market['precision'].get('amount', 0.01))
                    min_qty = float(market['limits']['amount'].get('min', 0.01))

            case 'bingx':
                # BingX uses the standard precision and limits fields
                lot_step = float(market['precision'].get('amount', 0.001))
                min_qty = float(market['limits']['amount'].get('min', 0.001))

            case 'hyperliquid':
                lot_step = market.get('precision', {}).get('amount', 0)
                min_qty = market.get('limits', {}).get('amount', {}).get('min', 0)
                if lot_step and lot_step > 0:
                    step = Decimal(str(lot_step))
                else:
                    step = Decimal('0.0001')
                if min_qty:
                    min_qty = Decimal(str(min_qty))
                else:
                    min_qty = step
                lot_step = float(step)
                min_qty = float(min_qty)

            case 'kucoin futures':
                # KuCoin Futures: amount is in CONTRACTS (not base coin).
                # contractSize varies per symbol (BTC=0.001, SOL=0.1, etc.) and
                # is already factored into the general formula above.
                # precision.amount typically 1 (whole contracts), min usually 1.
                lot_step = float(market.get('precision', {}).get('amount', 1))
                min_qty = float(market.get('limits', {}).get('amount', {}).get('min', 1))

            case _:
                logger.error(f"Unknown exchange: {exchange}")
                return None

    except Exception as e:
        logger.error(f"Failed to extract lot_step/min_qty for {exchange}: {e}")
        return None

    if lot_step is None or min_qty is None:
        logger.error(f"Could not get lot_step and min_qty for {market.get('symbol')}")
        return None

    try:
        contracts = Decimal(str(usdt_volume)) / (Decimal(str(price)) * Decimal(str(contract_size)))
        lot_step_dec = Decimal(str(lot_step)).normalize()
        min_qty_dec = Decimal(str(min_qty))

        # usdt_volume already arrives as a float —
        # usually as a round-trip k*lot*price/leverage*leverage, where multiplication and
        # division shift the value DOWN by a few ULPs. The ROUND_DOWN below cuts off
        # a whole lot because of it: 2 BTC lots at price=50000.1, lev=1 became
        # 1 lot (−50 % of the grid level amount), in 7 % of combinations of a 15x15x25 sweep.
        # Snap the value to a whole number of lots if it is off by a
        # relative amount on the order of machine error (measured: ≤3e-16 with
        # a 1e-9 tolerance).
        if contracts > 0:
            contracts_snapped = contracts.quantize(lot_step_dec, rounding=ROUND_UP)
            if contracts_snapped - contracts <= contracts * Decimal('1e-9'):
                contracts = contracts_snapped

        # logger.debug(f"calculate_contract_size: usdt_volume={usdt_volume}, price={price}, contract_size={contract_size}")
        # logger.debug(f"calculate_contract_size: contracts (before rounding)={contracts}")
        # logger.debug(f"calculate_contract_size: lot_step={lot_step}, min_qty={min_qty}")

        # FIX: First try ROUND_DOWN
        contracts_down = contracts.quantize(lot_step_dec, rounding=ROUND_DOWN)
        #logger.debug(f"calculate_contract_size: contracts_down={contracts_down}")

        # FIX: If result is less than min_qty, try ROUND_UP
        if contracts_down < min_qty_dec:
            #logger.debug(f"calculate_contract_size: contracts_down ({contracts_down}) < min_qty ({min_qty_dec}), trying ROUND_UP")
            contracts_up = contracts.quantize(lot_step_dec, rounding=ROUND_UP)
            #logger.debug(f"calculate_contract_size: contracts_up={contracts_up}")
            if contracts_up >= min_qty_dec:
                #logger.debug(f"calculate_contract_size: returning contracts_up={contracts_up}")
                return float(contracts_up)
            else:
                #logger.warning(f"Calculated contract size ({contracts}) is less than the minimum allowed ({min_qty}) even after rounding up.")
                return None

        #logger.debug(f"calculate_contract_size: returning contracts_down={contracts_down}")
        return float(contracts_down)

    except (InvalidOperation, ValueError) as e:
        logger.error(f"Decimal calculation error: {e}")
        return None


def calculate_usdt_from_contracts(contracts, price, market, leverage):
    """
    Converts a contract size back into a USDT amount on a futures market.

    :param contracts: Contract size (number)
    :param price: Price of the trading pair (e.g. BTC/USDT)
    :param market: Market info from self.exchange.market(symbol)
    :param leverage: Leverage
    :return: Amount in USDT (float) or None on error
    """
    try:
        if contracts is None:
            logger.error(f"calculate_usdt_from_contracts: contracts is None")
            return None
        if price is None:
            logger.error(f"calculate_usdt_from_contracts: price is None")
            return None
        if market is None:
            logger.error(f"calculate_usdt_from_contracts: market is None")
            return None

        contracts = float(contracts)
        price = float(price)
        contract_size = float(market.get('contractSize', 1.0))
    except (TypeError, ValueError) as e:
        logger.error(f"Failed to convert inputs to float: {e} (contracts={contracts}, price={price}, market={market})")
        return None

    # Check that the number of contracts is positive
    if contracts <= 0:
        logger.warning(f"Number of contracts must be greater than zero: {contracts}")
        return None

    # Check that the price is positive
    if price <= 0:
        logger.warning(f"Price must be greater than zero: {price}")
        return None

    try:
        # Inverse formula: usdt_volume = contracts * price * contract_size
        usdt_volume = Decimal(str(contracts)) * Decimal(str(price)) * Decimal(str(contract_size))

        return float(usdt_volume/leverage)

    except (InvalidOperation, ValueError) as e:
        logger.error(f"Decimal calculation error converting contracts to USDT: {e}")
        return None
