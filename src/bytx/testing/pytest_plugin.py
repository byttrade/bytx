"""pytest plugin ``bytx.testing``: exchange emulator and virtual clock fixtures.

Enable it explicitly (the plugin does not activate on SDK install)::

    # conftest.py
    pytest_plugins = ['bytx.testing.pytest_plugin']

    async def test_stop(fake_exchange):                    # requires pytest-asyncio (or anyio)
        ex, venue = fake_exchange('okx', price=100)
        await ex.buy('BTC/USDT:USDT', size=0.01)

Fixtures: ``bytx_clock`` (VirtualClock at :data:`~bytx.testing.T0`), ``fake_exchange`` and
``paper_exchange`` (factories; the Exchanges they create are closed after the test), ``bytx_offline``
(cut network for any of your own Exchanges: ``bytx_offline(ex)``). No ``bytx_exchanges`` marker is needed:
parametrize over :data:`~bytx.testing.EXCHANGES`.
"""
from __future__ import annotations

import asyncio
import contextlib
from typing import Any, Callable, Iterator

import pytest

from bytx.paper import VirtualClock
from bytx.testing.paper import T0, paper_exchange as _paper_exchange
from bytx.testing.venue import fake_exchange as _fake_exchange, offline


def _close_all(created: list) -> None:
    """Close the test's Exchanges. Their network is cut, so there are no aiohttp sessions; a separate loop is enough."""
    for ex in created:
        with contextlib.suppress(Exception):
            asyncio.run(ex.close())


@pytest.fixture
def bytx_clock() -> VirtualClock:
    """Virtual clock (ms), starting at ``bytx.testing.T0``."""
    return VirtualClock(T0)


@pytest.fixture
def fake_exchange() -> Iterator[Callable[..., tuple[Any, Any]]]:
    """Factory of ``(Exchange, FakeVenue)``; see :func:`bytx.testing.fake_exchange`."""
    created: list = []

    def make(name: str = 'binance', **kwargs: Any) -> tuple[Any, Any]:
        ex, venue = _fake_exchange(name, **kwargs)
        created.append(ex)
        return ex, venue

    yield make
    _close_all(created)


@pytest.fixture
def paper_exchange(bytx_clock: VirtualClock) -> Iterator[Callable[..., Any]]:
    """Factory of ``PaperExchange`` on the test's shared ``bytx_clock``; see :func:`bytx.testing.paper_exchange`."""
    created: list = []

    def make(name: str = 'binance', **kwargs: Any) -> Any:
        kwargs.setdefault('clock', bytx_clock)
        ex = _paper_exchange(name, **kwargs)
        created.append(ex)
        return ex

    yield make
    _close_all(created)


@pytest.fixture
def bytx_offline() -> Callable[[Any], Any]:
    """Cut network access for your ``Exchange`` (``ex.ccxt``): any request raises ``NetworkDisabled``."""
    return offline
