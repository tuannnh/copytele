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
        if isinstance(req, functions.upload.SaveFilePartRequest):
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


def test_small_upload_uses_small_parts_md5_and_all_connections():
    import hashlib

    data = os.urandom(ft.SMALL_PART * 9 + 17)  # 10 parts, well under BIG_FILE
    store = {}
    pool = FakePool(store, n=4)
    f = asyncio.run(ft.upload_small(pool, data, "x"))
    assert isinstance(f, types.InputFile)  # not InputFileBig: Telegram wants md5 here
    assert f.parts == 10
    assert f.md5_checksum == hashlib.md5(data).hexdigest()
    assert b"".join(store[i] for i in range(10)) == data
    assert sum(s.seen for s in pool.senders) == 10
    assert sum(1 for s in pool.senders if s.seen) > 1  # spread over connections


def test_small_upload_tiny_and_empty():
    for n in (0, 1, ft.SMALL_PART, ft.SMALL_PART + 1):
        data = os.urandom(n)
        store = {}
        f = asyncio.run(ft.upload_small(FakePool(store), data, "x"))
        assert f.parts == max(1, -(-n // ft.SMALL_PART))
        assert b"".join(store[i] for i in range(f.parts)) == data
