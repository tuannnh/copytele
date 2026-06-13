"""Bridge between synchronous (FUSE) threads and an asyncio event loop.

Telethon is asyncio-based and binds its client to one event loop. FUSE
callbacks (via fusepy) arrive on arbitrary worker threads. We run a single
dedicated event loop in its own thread; all Telegram coroutines are submitted
to it with ``run_coroutine_threadsafe`` and the calling thread blocks for the
result. This keeps every Telethon call on the one loop it was created on.
"""

from __future__ import annotations

import asyncio
import threading
from concurrent.futures import Future
from typing import Any, Coroutine, TypeVar

T = TypeVar("T")


class AsyncLoop:
    def __init__(self) -> None:
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._run, name="tgfs-asyncio", daemon=True
        )
        self._started = threading.Event()

    def _run(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._started.set()
        self._loop.run_forever()

    def start(self) -> None:
        if self._thread.is_alive():
            return
        self._thread.start()
        self._started.wait()

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self._loop

    def submit(self, coro: Coroutine[Any, Any, T]) -> Future[T]:
        return asyncio.run_coroutine_threadsafe(coro, self._loop)

    def call(self, coro: Coroutine[Any, Any, T], timeout: float | None = None) -> T:
        """Submit a coroutine and block the current thread for its result."""
        return self.submit(coro).result(timeout)

    def stop(self) -> None:
        if not self._thread.is_alive():
            return
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=10)
