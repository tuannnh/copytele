"""Telethon (MTProto) blob backend.

Stores each content chunk as a separate document message in a dedicated private
channel. The synchronous :class:`Backend` interface is presented to the rest of
tgfs; all async Telethon calls are marshalled onto the dedicated event loop via
:class:`~tgfs.asyncbridge.AsyncLoop`.
"""

from __future__ import annotations

import io
import logging
import time
from typing import Any

from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.tl.types import DocumentAttributeFilename

from .asyncbridge import AsyncLoop
from .config import Config

log = logging.getLogger("tgfs.telegram")

_MAX_RETRIES = 5
_CHUNK_NAME = "tgfs.bin"


class TelegramBackend:
    def __init__(self, cfg: Config, loop: AsyncLoop, *, connect: bool = True) -> None:
        self.cfg = cfg
        self.loop = loop
        self._client: TelegramClient | None = None
        self._entity: Any = None
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
        self._entity = await client.get_entity(self.cfg.channel)
        me = await client.get_me()
        log.info(
            "connected as %s; storage channel resolved: %s",
            getattr(me, "username", None) or me.id,
            getattr(self._entity, "title", self.cfg.channel),
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

        async def _dc() -> None:
            # must run on the backend's own loop, else Telethon builds a Future
            # bound to the wrong loop ("attached to a different loop")
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
                time.sleep(wait)
            except (ConnectionError, OSError) as ex:
                attempt += 1
                if attempt >= _MAX_RETRIES:
                    raise
                backoff = min(2**attempt, 30)
                log.warning(
                    "%s failed (%s); retry %d/%d in %ss",
                    what, ex, attempt, _MAX_RETRIES, backoff,
                )
                time.sleep(backoff)

    # ----- Backend interface ----------------------------------------------
    def upload(self, data: bytes) -> int:
        return self.loop.call(self._upload(data))

    def download(self, handle: int) -> bytes:
        return self.loop.call(self._download(handle))

    def delete(self, handle: int) -> None:
        self.loop.call(self._delete(handle))

    async def _upload(self, data: bytes) -> int:
        async def go():
            buf = io.BytesIO(data)
            buf.name = _CHUNK_NAME
            msg = await self.client.send_file(
                self._entity,
                file=buf,
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
            data = await self.client.download_media(msg, file=bytes)
            assert isinstance(data, (bytes, bytearray))
            return bytes(data)

        return await self._with_retry("download", go)

    async def _delete(self, handle: int) -> None:
        async def go():
            await self.client.delete_messages(self._entity, [handle])

        await self._with_retry("delete", go)
