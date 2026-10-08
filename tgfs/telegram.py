"""Telethon (MTProto) blob backend.

Stores each content chunk as a separate document message in a dedicated private
channel. The synchronous :class:`Backend` interface is presented to the rest of
tgfs; all async Telethon calls are marshalled onto the dedicated event loop via
:class:`~tgfs.asyncbridge.AsyncLoop`.
"""

from __future__ import annotations

import asyncio
import io
import logging
from typing import Any

from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.tl.types import DocumentAttributeFilename

from .asyncbridge import AsyncLoop
from .config import Config

try:  # needs Telethon internals; stock path is the fallback
    from . import fasttransfer
except Exception:  # pragma: no cover
    fasttransfer = None

log = logging.getLogger("tgfs.telegram")

_MAX_RETRIES = 5
_CHUNK_NAME = "tgfs.bin"
MIB = 1024 * 1024
# one document: 8000 parts x 512 KiB = 4000 MiB on Premium; 4000 parts free
_LIMIT_PREMIUM = 4000 * MIB
_LIMIT_FREE = 1950 * MIB


class TelegramBackend:
    def __init__(self, cfg: Config, loop: AsyncLoop, *, connect: bool = True) -> None:
        self.cfg = cfg
        self.loop = loop
        self._client: TelegramClient | None = None
        self._entity: Any = None
        self._pools: dict[int, Any] = {}
        self.premium = False
        self.conns = 1
        if connect:
            self.loop.call(self._connect())

    # ----- lifecycle -------------------------------------------------------
    async def _connect(self) -> None:
        client = TelegramClient(
            str(self.cfg.session), self.cfg.api_id, self.cfg.api_hash
        )
        await client.connect()
        if not await client.is_user_authorized():
            raise RuntimeError(
                "Telegram session is not authorized; run `tgfs login` first"
            )
        self._client = client
        self._entity = await self._resolve_channel()
        me = await client.get_me()
        self.premium = bool(getattr(me, "premium", False))
        limit = _LIMIT_PREMIUM if self.premium else _LIMIT_FREE
        if self.cfg.chunk_size > limit:
            raise RuntimeError(
                f"chunk_size {self.cfg.chunk_size // MIB} MiB exceeds Telegram's "
                f"{limit // MIB} MiB per-file limit for this "
                f"{'Premium' if self.premium else 'free'} account"
            )
        req = self.cfg.connections
        self.conns = req if req > 0 else (8 if self.premium else 4)
        if fasttransfer is None:
            self.conns = 1
        log.info("premium=%s, %d parallel connection(s) per transfer",
                 self.premium, self.conns)
        log.info(
            "connected as %s; storage channel resolved: %s",
            getattr(me, "username", None) or me.id,
            getattr(self._entity, "title", self.cfg.channel),
        )

    async def _resolve_channel(self):
        """Resolve cfg.channel to an entity, robustly.

        A freshly-created session (e.g. from `tgfs login` in a container) has not
        cached the channel, so a bare numeric id can't be looked up — and a
        positive id is ambiguously treated as a user. We first try a direct
        resolve (works for @usernames, t.me links, and already-cached ids), then
        fall back to scanning dialogs and matching by id (in either bot-API
        ``-100…`` or raw form) or by title. Scanning also warms the entity cache.
        """
        ch = self.cfg.channel
        try:
            return await self.client.get_entity(ch)
        except (ValueError, TypeError):
            pass

        ids: set[int] = set()
        title = None
        if isinstance(ch, int):
            raw = abs(ch)
            ids.add(raw)
            s = str(raw)
            if s.startswith("100"):  # strip bot-API channel prefix
                ids.add(int(s[3:]))
        else:
            title = str(ch).lstrip("@")

        async for d in self.client.iter_dialogs():
            if not d.is_channel:
                continue
            ent = d.entity
            if title is not None:
                uname = getattr(ent, "username", None)
                if (uname and uname.lower() == title.lower()) or \
                        getattr(ent, "title", None) == title:
                    return ent
            elif ent.id in ids or d.id == ch:
                return ent

        raise RuntimeError(
            f"storage channel {ch!r} not found among this account's dialogs. "
            "Check the id with `tgfs channels`, use the -100… form (or the "
            "channel @username / title), and make sure this account is a member."
        )

    @property
    def client(self) -> TelegramClient:
        if self._client is None:
            raise RuntimeError("Telegram client not connected")
        return self._client

    def close(self) -> None:
        if self._client is None:
            return
        client = self._client
        self._client = None
        pools, self._pools = self._pools, {}

        async def _dc() -> None:
            # must run on the backend's own loop, else Telethon builds a Future
            # bound to the wrong loop ("attached to a different loop")
            for pool in pools.values():
                await pool.close()
            await client.disconnect()

        try:
            self.loop.call(_dc())
        except Exception as ex:  # cleanup must never raise
            log.warning("error during disconnect: %s", ex)

    # ----- retry helper ----------------------------------------------------
    async def _with_retry(self, what: str, coro_factory):
        attempt = 0
        while True:
            try:
                return await coro_factory()
            except FloodWaitError as ex:
                wait = int(ex.seconds) + 1
                log.warning("FloodWait on %s: sleeping %ss", what, wait)
                await asyncio.sleep(wait)
            except (ConnectionError, OSError) as ex:
                attempt += 1
                if attempt >= _MAX_RETRIES:
                    raise
                backoff = min(2**attempt, 30)
                log.warning(
                    "%s failed (%s); retry %d/%d in %ss",
                    what, ex, attempt, _MAX_RETRIES, backoff,
                )
                await asyncio.sleep(backoff)

    # ----- Backend interface ----------------------------------------------
    def upload(self, data: bytes) -> int:
        return self.loop.call(self._upload(data))

    def download(self, handle: int) -> bytes:
        return self.loop.call(self._download(handle))

    def delete(self, handle: int) -> None:
        self.loop.call(self._delete(handle))

    def _pool(self, dc_id: int):
        pool = self._pools.get(dc_id)
        if pool is None:
            pool = self._pools[dc_id] = fasttransfer.SenderPool(
                self.client, dc_id, self.conns
            )
        return pool

    async def _upload(self, data: bytes) -> int:
        async def go():
            file: Any = None
            if self.conns > 1 and len(data) > fasttransfer.BIG_FILE:
                try:
                    file = await fasttransfer.upload_big(
                        self._pool(self.client.session.dc_id), data, _CHUNK_NAME
                    )
                except Exception as ex:
                    log.warning("parallel upload failed (%s); using stock path", ex)
            if file is None:
                file = io.BytesIO(data)
                file.name = _CHUNK_NAME
            msg = await self.client.send_file(
                self._entity,
                file=file,
                force_document=True,
                attributes=[DocumentAttributeFilename(_CHUNK_NAME)],
            )
            return int(msg.id)

        return await self._with_retry("upload", go)

    async def _download(self, handle: int) -> bytes:
        async def go():
            msg = await self.client.get_messages(self._entity, ids=handle)
            if msg is None or msg.media is None:
                raise FileNotFoundError(f"blob message {handle} missing")
            doc = getattr(msg, "document", None)
            if self.conns > 1 and doc is not None and doc.size > 4 * MIB:
                try:
                    return await fasttransfer.download_doc(
                        self._pool(doc.dc_id), self.client, msg.media, doc.size
                    )
                except Exception as ex:
                    log.warning("parallel download failed (%s); using stock path", ex)
            data = await self.client.download_media(msg, file=bytes)
            assert isinstance(data, (bytes, bytearray))
            return bytes(data)

        return await self._with_retry("download", go)

    async def _delete(self, handle: int) -> None:
        async def go():
            await self.client.delete_messages(self._entity, [handle])

        await self._with_retry("delete", go)
