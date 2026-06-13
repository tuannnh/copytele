"""Blob storage backend interface + an in-memory mock for tests.

A backend stores opaque byte blobs and returns an integer handle (for the real
backend, a Telegram message id). The rest of tgfs only ever sees this small,
synchronous interface; the Telethon/asyncio machinery is hidden inside
``TelegramBackend`` (see telegram.py).
"""

from __future__ import annotations

import threading
from typing import Protocol


class Backend(Protocol):
    def upload(self, data: bytes) -> int:
        """Store a blob; return its handle (Telegram message id)."""

    def download(self, handle: int) -> bytes:
        """Fetch a blob by handle."""

    def delete(self, handle: int) -> None:
        """Delete a blob by handle (best-effort)."""

    def close(self) -> None: ...


class MemoryBackend:
    """In-memory stand-in for the Telegram backend. Deterministic, no network."""

    def __init__(self) -> None:
        self._blobs: dict[int, bytes] = {}
        self._next = 1
        self._lock = threading.Lock()
        self.uploads = 0
        self.downloads = 0
        self.deletes = 0

    def upload(self, data: bytes) -> int:
        with self._lock:
            h = self._next
            self._next += 1
            self._blobs[h] = bytes(data)
            self.uploads += 1
            return h

    def download(self, handle: int) -> bytes:
        with self._lock:
            self.downloads += 1
            try:
                return self._blobs[handle]
            except KeyError:
                raise FileNotFoundError(f"no blob with handle {handle}") from None

    def delete(self, handle: int) -> None:
        with self._lock:
            self.deletes += 1
            self._blobs.pop(handle, None)

    def close(self) -> None:
        pass
