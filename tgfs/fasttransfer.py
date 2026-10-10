"""Multi-connection MTProto part transfer (the "tgup -j N" technique).

Telethon moves a file's 512 KiB parts one at a time over a single connection,
which caps throughput far below what the account is allowed. Here a chunk's
parts are spread over N independent MTProto connections to the file's data
center, so N requests are in flight at once.

Relies on Telethon internals (``MTProtoSender``, ``_create_exported_sender``);
every entry point raises on any surprise and :class:`~tgfs.telegram.TelegramBackend`
falls back to Telethon's stock single-connection path.
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import logging
from typing import Any

from telethon import helpers, utils
from telethon.errors import FloodWaitError
from telethon.network import MTProtoSender
from telethon.tl import functions, types

log = logging.getLogger("tgfs.fast")

PART = 512 * 1024  # MTProto's maximum part size
SMALL_PART = 128 * 1024  # part size Telethon uses for small files (divides PART)
BIG_FILE = 10 * 1024 * 1024  # below this Telegram wants md5'd small-file parts
MAX_PARTS = 8000  # => 4000 MiB per document (Premium); 4000 parts free
_PART_RETRIES = 4


class SenderPool:
    """N long-lived extra connections to one data center."""

    def __init__(self, client, dc_id: int, size: int) -> None:
        self.client = client
        self.dc_id = dc_id
        self.size = size
        self.senders: list[MTProtoSender] = []
        self._lock = asyncio.Lock()

    async def _make(self) -> MTProtoSender:
        c = self.client
        if self.dc_id != c.session.dc_id:
            return await c._create_exported_sender(self.dc_id)
        # home DC: reuse the account's auth key on a fresh connection
        sender = MTProtoSender(c.session.auth_key, loggers=c._log)
        await sender.connect(
            c._connection(
                c.session.server_address, c.session.port, c.session.dc_id,
                loggers=c._log, proxy=c._proxy, local_addr=c._local_addr,
            )
        )
        return sender

    async def get(self) -> list[MTProtoSender]:
        async with self._lock:
            while len(self.senders) < self.size:
                self.senders.append(await self._make())
            return list(self.senders)

    async def reset(self) -> None:
        """Drop all connections (called after a transfer error)."""
        async with self._lock:
            dead, self.senders = self.senders, []
        for s in dead:
            try:
                await s.disconnect()
            except Exception:
                pass

    async def close(self) -> None:
        await self.reset()


async def _call(sender: MTProtoSender, request) -> Any:
    for attempt in range(_PART_RETRIES):
        try:
            return await sender.send(request)
        except FloodWaitError as ex:
            await asyncio.sleep(int(ex.seconds) + 1)
        except (ConnectionError, OSError, asyncio.TimeoutError):
            if attempt == _PART_RETRIES - 1:
                raise
            await asyncio.sleep(min(2**attempt, 8))
    raise ConnectionError("part transfer failed")


async def _run_parts(pool: SenderPool, count: int, do_part) -> None:
    """Run ``do_part(sender, index)`` for all indices, one worker per connection."""
    senders = await pool.get()
    nxt = iter(range(count))

    async def worker(sender):
        for i in nxt:  # shared iterator: each index is handed out once
            await do_part(sender, i)

    tasks = [asyncio.ensure_future(worker(s)) for s in senders]
    try:
        await asyncio.gather(*tasks)
    except BaseException:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await pool.reset()  # connections may hold half-sent state
        raise


async def upload_big(
    pool: SenderPool, data: bytes, name: str
) -> types.InputFileBig:
    """Upload ``data`` (> BIG_FILE) over the pool; return an InputFileBig."""
    count = -(-len(data) // PART)
    if count > MAX_PARTS:
        raise ValueError(f"{len(data)} bytes exceeds the {MAX_PARTS}-part limit")
    file_id = helpers.generate_random_long()
    view = memoryview(data)

    async def do_part(sender, i):
        part = bytes(view[i * PART : (i + 1) * PART])
        ok = await _call(
            sender,
            functions.upload.SaveBigFilePartRequest(file_id, i, count, part),
        )
        if not ok:
            raise RuntimeError(f"Telegram rejected upload part {i}")

    await _run_parts(pool, count, do_part)
    return types.InputFileBig(file_id, count, name)


async def upload_small(
    pool: SenderPool, data: bytes, name: str
) -> types.InputFile:
    """Upload ``data`` (<= BIG_FILE) over the pool; return an InputFile.

    Telegram wants the small-file request (``SaveFilePart``) plus the file's md5
    for files up to 10 MiB. The stock path sends such a file's parts one after
    another over one connection (~2-3 MB/s); spreading the small parts over the
    pool makes a 5 MB photo as fast as a big upload.
    """
    count = max(1, -(-len(data) // SMALL_PART))
    file_id = helpers.generate_random_long()
    view = memoryview(data)
    md5 = hashlib.md5(data).hexdigest()

    async def do_part(sender, i):
        part = bytes(view[i * SMALL_PART : (i + 1) * SMALL_PART])
        ok = await _call(
            sender, functions.upload.SaveFilePartRequest(file_id, i, part)
        )
        if not ok:
            raise RuntimeError(f"Telegram rejected upload part {i}")

    await _run_parts(pool, count, do_part)
    return types.InputFile(file_id, count, name, md5)


async def download_doc(pool: SenderPool, client, media, size: int) -> bytes:
    """Download a document's bytes over the pool."""
    dc_id, location = utils.get_input_location(media)
    if dc_id is not None and dc_id != pool.dc_id:
        raise RuntimeError("document lives on a different DC than the pool")
    if size <= 0:
        return b""
    out = bytearray(size)
    count = -(-size // PART)

    async def do_part(sender, i):
        off = i * PART
        res = await _call(
            sender, functions.upload.GetFileRequest(location, offset=off, limit=PART)
        )
        if isinstance(res, types.upload.FileCdnRedirect):
            raise RuntimeError("CDN redirect; use stock downloader")
        b = res.bytes
        want = min(PART, size - off)
        if len(b) != want:
            raise IOError(f"short part {i}: got {len(b)} of {want}")
        out[off : off + len(b)] = b

    await _run_parts(pool, count, do_part)
    return bytes(out)


def small_file(data: bytes, name: str) -> io.BytesIO:
    buf = io.BytesIO(data)
    buf.name = name
    return buf
