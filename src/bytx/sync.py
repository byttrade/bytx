"""Synchronous SDK wrapper: blocking methods on top of a dedicated event loop in a separate thread.

    from bytx.sync import create_exchange

    with create_exchange('okx', demo=True, api_key=..., secret=..., password=...) as ex:
        print(ex.ticker('BTC/USDT:USDT').last)
        order = ex.buy('BTC/USDT:USDT', size=0.01, price=50_000, post_only=True)
        for book in ex.stream.order_book('BTC/USDT:USDT'):     # a WS stream is a regular iterator
            ...

How it works. Each sync client has its own event loop in its own (daemon) thread. The async
``Exchange`` is created and lives only in that thread: all calls (sync methods too)
are dispatched there and wait for the result, so client state is never touched by two threads at once.

* Works inside an already running loop too (Jupyter, an async app): its own loop runs in its own thread,
  no conflicts (the outer loop is blocked for the duration of the call; for async code use the async API).
* A call from the client's own thread (e.g. from an ``on_update`` callback) raises ``RuntimeError`` immediately
  instead of deadlocking.
* ``call_timeout`` (seconds): cap on waiting for a call; when it expires, the task is cancelled and
  ``TimeoutError`` is raised. The order may still have reached the exchange: reconcile by ``client_id``
  (``ex.find_order``). No cap by default (network timeouts are handled by the SDK itself).
* Ctrl+C during a call cancels the task in the client's loop and re-raises ``KeyboardInterrupt``.
* ``close()`` (or ``with``) closes the Exchange in its loop and stops the loop and thread.
  A forgotten ``close()`` is handled the same way on garbage collection or interpreter exit.

Return values: models (``Order``, ``Ticker``…) as is; SDK objects with async methods
(``ex.stream``, ``ex.algos``, ``ex.history``, trackers, subscriptions, algorithms) get the same sync
wrapper: async iterator → iterator (``next(sub)``, ``sub.next(timeout=)``), ``async with`` → ``with``,
awaitable (algorithm) → ``.wait(timeout=None)``. User callbacks run in the client's
thread: they must be fast and must not call the sync client.
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import functools
import inspect
import threading
import weakref
from typing import Any, Callable, Iterator, Optional, TypeVar

T = TypeVar('T')
_CLOSE_TIMEOUT = 15.0


class LoopThread:
    """A dedicated event loop in a daemon thread; :meth:`run` / :meth:`call` are blocking calls into it."""

    def __init__(self, name: str = 'bytx-sync') -> None:
        self.loop = asyncio.new_event_loop()
        self._ready = threading.Event()
        self._closed = False
        self._lock = threading.Lock()
        self.thread = threading.Thread(target=self._main, name=name, daemon=True)
        self.thread.start()
        self._ready.wait()

    def _main(self) -> None:
        asyncio.set_event_loop(self.loop)
        self.loop.call_soon(self._ready.set)
        try:
            self.loop.run_forever()
        finally:
            try:
                pending = [t for t in asyncio.all_tasks(self.loop) if not t.done()]
                for t in pending:
                    t.cancel()
                if pending:
                    self.loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                self.loop.run_until_complete(self.loop.shutdown_asyncgens())
            finally:
                self.loop.close()

    @property
    def closed(self) -> bool:
        return self._closed

    def in_loop_thread(self) -> bool:
        return threading.get_ident() == self.thread.ident

    def _check(self) -> None:
        if self._closed:
            raise RuntimeError('bytx.sync: client is closed')
        if self.in_loop_thread():
            raise RuntimeError("bytx.sync: blocking call from the client's own thread (callback?); "
                               'this is a deadlock; use the async API in callbacks')

    def _wait(self, fut: 'concurrent.futures.Future[T]', timeout: Optional[float]) -> T:
        try:
            return fut.result(timeout)
        except concurrent.futures.TimeoutError:
            fut.cancel()
            raise TimeoutError(f'bytx.sync: call did not finish within {timeout} s (task cancelled)') from None
        except BaseException:            # KeyboardInterrupt / SystemExit: cancel the task in the client's loop
            if not fut.done():
                fut.cancel()
            raise

    def run(self, coro: Any, timeout: Optional[float] = None) -> Any:
        """Run a coroutine (awaitable) in the client's loop and wait for the result."""
        try:
            self._check()
        except BaseException:
            if inspect.iscoroutine(coro):
                coro.close()             # avoid "never awaited"
            raise

        async def runner() -> Any:
            return await coro

        return self._wait(asyncio.run_coroutine_threadsafe(runner(), self.loop), timeout)

    def call(self, fn: Callable[..., T], *args: Any, timeout: Optional[float] = None, **kwargs: Any) -> T:
        """Run a plain function in the client's thread (Exchange state lives only there)."""
        self._check()
        fut: 'concurrent.futures.Future[T]' = concurrent.futures.Future()

        def go() -> None:
            if not fut.set_running_or_notify_cancel():
                return
            try:
                fut.set_result(fn(*args, **kwargs))
            except BaseException as e:   # noqa: BLE001 — passed to the calling thread
                fut.set_exception(e)

        self.loop.call_soon_threadsafe(go)
        return self._wait(fut, timeout)

    def stop(self, timeout: float = _CLOSE_TIMEOUT) -> None:
        """Stop the loop (remaining tasks are cancelled) and join the thread. Repeated calls are a no-op."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
        if self.in_loop_thread():
            self.loop.stop()
            return
        with contextlib.suppress(RuntimeError):       # loop already closed (interpreter exit)
            self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(timeout)


# ─── sync object wrapper ────────────────────────────────────────────────────

def _needs_proxy(value: Any) -> bool:
    """An SDK object with an async interface (stream, tracker, algorithm, loader), not a data model."""
    if value is None or isinstance(value, (str, bytes, int, float, bool, list, tuple, dict, set)):
        return False
    if hasattr(value, '__aiter__') or hasattr(value, '__aenter__') or hasattr(value, '__await__'):
        return True
    mod = getattr(type(value), '__module__', '') or ''
    if not mod.startswith('bytx'):
        return False
    return any(inspect.iscoroutinefunction(getattr(type(value), n, None)) for n in dir(type(value))
               if not n.startswith('__'))


class SyncProxy:
    """Sync view of an async object living in a :class:`LoopThread` loop (see module)."""

    __slots__ = ('_target', '_runner', '_timeout', '__weakref__')

    def __init__(self, target: Any, runner: LoopThread, timeout: Optional[float] = None) -> None:
        object.__setattr__(self, '_target', target)
        object.__setattr__(self, '_runner', runner)
        object.__setattr__(self, '_timeout', timeout)

    def _wrap(self, value: Any) -> Any:
        if inspect.iscoroutine(value):
            return self._wrap(self._runner.run(value, self._timeout))
        if _needs_proxy(value):
            return SyncProxy(value, self._runner, self._timeout)
        return value

    def __getattr__(self, name: str) -> Any:
        target, runner = self._target, self._runner
        attr = runner.call(getattr, target, name) if not runner.closed else getattr(target, name)
        if inspect.iscoroutinefunction(attr) or (callable(attr) and not isinstance(attr, type)):
            @functools.wraps(attr)
            def call(*args: Any, **kwargs: Any) -> Any:
                if inspect.iscoroutinefunction(attr):
                    return self._wrap(runner.run(attr(*args, **kwargs), self._timeout))
                return self._wrap(runner.call(attr, *args, timeout=self._timeout, **kwargs))
            return call
        return self._wrap(attr)

    def __setattr__(self, name: str, value: Any) -> None:
        self._runner.call(setattr, self._target, name, value)

    def __repr__(self) -> str:
        return f'<sync {self._target!r}>'

    def __dir__(self) -> list[str]:
        return sorted(set(dir(self._target)) | {'wait', 'next'})

    # ─── async iterator → iterator ────────────────────────────────────────
    def __iter__(self) -> Iterator[Any]:
        if not hasattr(self._target, '__aiter__'):
            raise TypeError(f'{type(self._target).__name__} is not an iterator')
        return self

    def __next__(self) -> Any:
        return self.next()

    def next(self, timeout: Optional[float] = None) -> Any:
        """Next item of the async iterator (WS stream); StopIteration at the end."""
        target = self._target
        if not hasattr(target, '__anext__'):
            target = self._runner.call(target.__aiter__)
            object.__setattr__(self, '_target', target)
        try:
            return self._wrap(self._runner.run(target.__anext__(), timeout if timeout is not None else self._timeout))
        except StopAsyncIteration:
            raise StopIteration from None

    # ─── async with → with ────────────────────────────────────────────────
    def __enter__(self) -> Any:
        if not hasattr(self._target, '__aenter__'):
            raise TypeError(f'{type(self._target).__name__} is not a context manager')
        result = self._runner.run(self._target.__aenter__(), self._timeout)
        return self if result is self._target else self._wrap(result)

    def __exit__(self, *exc: Any) -> Any:
        return self._runner.run(self._target.__aexit__(*exc), self._timeout)

    # ─── awaitable (algorithm) → wait() ───────────────────────────────────
    def wait(self, timeout: Optional[float] = None) -> Any:
        """Wait for the awaitable (``await algo`` in the async API)."""
        target = self._target
        if not hasattr(target, '__await__'):
            raise TypeError(f'{type(target).__name__} is not awaitable')

        async def waiter() -> Any:
            return await target

        return self._wrap(self._runner.run(waiter(), timeout if timeout is not None else self._timeout))


# ─── clients ────────────────────────────────────────────────────────────────

def _shutdown(runner: LoopThread, target: Any) -> None:
    """Close the object in its loop and stop the thread (close() / garbage collection / exit)."""
    if runner.closed:
        return
    try:
        if not runner.in_loop_thread():
            close = getattr(target, 'close', None)
            if close is not None and inspect.iscoroutinefunction(close):
                runner.run(close(), _CLOSE_TIMEOUT)
    except BaseException:  # noqa: BLE001 — closing is best effort, the thread is stopped anyway
        pass
    finally:
        runner.stop()


class SyncClient(SyncProxy):
    """Thread owner: creates the ``factory()`` object in its loop and closes it together with the loop.
    Any async SDK object (your own Exchange subclass, a ``bytx.testing`` emulator): :func:`wrap`."""

    __slots__ = ('_finalizer',)

    def __init__(self, factory: Callable[[], Any], *, name: str, call_timeout: Optional[float]) -> None:
        runner = LoopThread(name)
        try:
            target = runner.call(factory)
        except BaseException:
            runner.stop()
            raise
        super().__init__(target, runner, call_timeout)
        object.__setattr__(self, '_finalizer', weakref.finalize(self, _shutdown, runner, target))

    def close(self) -> None:
        """Close the client (Exchange in its loop), stop the loop and thread. Repeated calls are a no-op."""
        self._finalizer()

    @property
    def closed(self) -> bool:
        return self._runner.closed

    def __enter__(self) -> Any:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


class Exchange(SyncClient):
    """Synchronous :class:`bytx.Exchange`: same methods, blocking. ``kwargs`` as in
    :func:`bytx.create_exchange`; ``call_timeout``: cap on waiting for each call (s)."""

    __slots__ = ()

    def __init__(self, name: str, *, call_timeout: Optional[float] = None, **kwargs: Any) -> None:
        from bytx.exchange import create_exchange as _create
        super().__init__(functools.partial(_create, name, **kwargs), name=f'bytx-sync-{name}',
                         call_timeout=call_timeout)

    def __repr__(self) -> str:
        return f'<bytx.sync.Exchange {self._target!r}>'


class AccountPool(SyncClient):
    """Synchronous :class:`bytx.AccountPool` (``pool['main'].balance()``, ``pool.portfolio(...)``)."""

    __slots__ = ()

    def __init__(self, accounts: Any, *, call_timeout: Optional[float] = None, **exchange_kwargs: Any) -> None:
        from bytx.accounts import AccountPool as _Pool
        super().__init__(functools.partial(_Pool, accounts, **exchange_kwargs), name='bytx-sync-pool',
                         call_timeout=call_timeout)

    def __getitem__(self, name: str) -> Any:
        return self._wrap(self._runner.call(self._target.__getitem__, name))

    def __len__(self) -> int:
        return len(self._target)


def wrap(factory: Callable[[], Any], *, name: str = 'bytx-sync', call_timeout: Optional[float] = None) -> SyncClient:
    """Sync client over an object that ``factory()`` creates **in the client's thread** (where it
    also lives and is closed): ``wrap(lambda: bytx.testing.paper_exchange('okx'))``."""
    return SyncClient(factory, name=name, call_timeout=call_timeout)


def create_exchange(name: str, *, call_timeout: Optional[float] = None, **kwargs: Any) -> Exchange:
    """Synchronous exchange client; arguments as in :func:`bytx.create_exchange`."""
    return Exchange(name, call_timeout=call_timeout, **kwargs)


__all__ = ['AccountPool', 'Exchange', 'LoopThread', 'SyncClient', 'SyncProxy', 'create_exchange', 'wrap']
