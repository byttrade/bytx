"""Many accounts on one server: a proxy per account, a shared rate limit per IP.

    pip install pyyaml          # for YAML; JSON works without it
    python examples/08_multi_account.py examples/accounts.example.yaml

Keys in the file go through ${ENV_VAR}; the secrets themselves are never written to the file. One
account's error (key, network) doesn't affect the rest.
"""
from __future__ import annotations

import asyncio
import sys

from bytx import AccountPool
from bytx.errors import BytxError
from bytx.types import Balance


def summary(results: dict[str, Balance | BytxError], currency: str = 'USDT') -> list[str]:
    lines = []
    for name, r in sorted(results.items()):
        if isinstance(r, BytxError):
            lines.append(f'{name}: error {type(r).__name__}: {r}')
        else:
            lines.append(f'{name}: {currency} total {r.total(currency):.2f}, free {r.free(currency):.2f}')
    return lines


async def main(path: str) -> None:
    async with AccountPool.from_file(path) as pool:
        for warning in pool.shared_ip_warnings():
            print('⚠️ ', warning)                 # several accounts behind one IP — ban risk
        balances = await pool.gather(lambda ex: ex.balance(), concurrency=10)
        print('\n'.join(summary(balances)))


if __name__ == '__main__':
    asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else 'examples/accounts.example.yaml'))
