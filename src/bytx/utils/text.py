import re

# Supported quote currencies for SWAP pairs
SUPPORTED_QUOTES = ['USDT', 'USDC']

def moderate_currency_format(pair: str) -> str:
    pair = pair.strip().upper()

    # Already in correct format: XXX/YYY:YYY
    if re.fullmatch(r'[A-Z0-9]+/[A-Z0-9]+:[A-Z0-9]+', pair):
        return pair

    # Try each supported quote currency
    for quote in SUPPORTED_QUOTES:
        # Format: BTCUSDT, ETHUSDC
        if match := re.match(rf'^([A-Z0-9]+){quote}$', pair):
            coin = match.group(1)
            return f"{coin}/{quote}:{quote}"
        # Format: BTC-USDT, ETH-USDC
        if match := re.match(rf'^([A-Z0-9]+)-{quote}$', pair):
            coin = match.group(1)
            return f"{coin}/{quote}:{quote}"
        # Format: BTC/USDT, ETH/USDC (without :USDT suffix)
        if match := re.match(rf'^([A-Z0-9]+)/{quote}$', pair):
            coin = match.group(1)
            return f"{coin}/{quote}:{quote}"

    raise ValueError(f"Invalid pair format: {pair}")

# Exchanges whose linear perps exist only in USDC (no USDT perps): a request
# for X/USDT means their X/USDC:USDC. Hyperliquid: collateral and settlement in USDC.
USDC_ONLY_SWAP_EXCHANGES = frozenset({'hyperliquid'})


def to_exchange_symbol(exchange_name: str, pair: str) -> str:
    """Swap market symbol in the form it exists under on exchange exchange_name.

    First moderate_currency_format (BTCUSDT, BTC-USDT, BTC/USDT → BTC/USDT:USDT;
    invalid pair → ValueError). For exchanges in USDC_ONLY_SWAP_EXCHANGES, USDT in
    the quote and settle currency is replaced with USDC (BTC/USDT:USDT →
    BTC/USDC:USDC); the base is untouched. Other exchanges — unchanged.
    Single conversion point for Client and calling services: the symbol a service
    compares against exchange positions/orders matches the client's symbol.
    """
    unified = moderate_currency_format(pair)
    if (exchange_name or '').strip().lower() not in USDC_ONLY_SWAP_EXCHANGES:
        return unified
    base, rest = unified.split('/', 1)
    quote, settle = rest.split(':', 1)
    quote = 'USDC' if quote == 'USDT' else quote
    settle = 'USDC' if settle == 'USDT' else settle
    return f"{base}/{quote}:{settle}"
