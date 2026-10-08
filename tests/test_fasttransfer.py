import asyncio
import os

import pytest

pytest.importorskip("telethon")
from telethon.tl import functions, types  # noqa: E402

from tgfs import fasttransfer as ft  # noqa: E402


class FakeSender:
    def __init__(self, store):
        self.store = store
        self.seen = 0

    async def send(self, req):
        await asyncio.sleep(0.001)
        self.seen += 1
        if isinstance(req, functions.upload.SaveBigFilePartRequest):
            self.store[req.file_part] = req.bytes
            return True
        off = req.offset
        return types.upload.File(types.storage.FileUnknown(), 0, self.store[off // ft.PART])


class FakePool:
    dc_id = 2

    def __init__(self, store, n=4):
        self.senders = [FakeSender(store) for _ in range(n)]

    async def get(self):
        return self.senders

    async def reset(self):
        pass


def test_parts_roundtrip_spread_over_connections(monkeypatch):
    data = os.urandom(ft.PART * 7 + 123)
    store = {}
    pool = FakePool(store)
    f = asyncio.run(ft.upload_big(pool, data, "x"))
    assert f.parts == 8
    assert sum(s.seen for s in pool.senders) == 8
    assert sum(1 for s in pool.senders if s.seen) > 1  # really parallel
    monkeypatch.setattr(ft.utils, "get_input_location", lambda m: (2, "loc"))
    got = asyncio.run(ft.download_doc(pool, None, None, len(data)))
    assert got == data
