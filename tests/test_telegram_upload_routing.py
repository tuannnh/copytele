import asyncio
import io
from types import SimpleNamespace

import pytest

pytest.importorskip("telethon")

from tgfs import fasttransfer as ft  # noqa: E402
from tgfs.telegram import TelegramBackend  # noqa: E402


class FakeClient:
    def __init__(self):
        self.session = SimpleNamespace(dc_id=2)
        self.sent = []

    async def send_file(self, entity, file, **kw):
        self.sent.append(file)
        return SimpleNamespace(id=100 + len(self.sent))


def _backend(conns=8):
    b = TelegramBackend(cfg=None, loop=None, connect=False)
    b._client = FakeClient()
    b._entity = object()
    b.conns = conns
    b._pool = lambda dc: "pool"
    return b


def test_small_blob_goes_through_pool_upload(monkeypatch):
    calls = []

    async def fake_small(pool, data, name):
        calls.append(("small", len(data)))
        return "INPUTFILE"

    async def fake_big(pool, data, name):
        calls.append(("big", len(data)))
        return "INPUTFILEBIG"

    monkeypatch.setattr(ft, "upload_small", fake_small)
    monkeypatch.setattr(ft, "upload_big", fake_big)
    b = _backend()
    assert asyncio.run(b._upload(b"x" * 1000)) == 101
    assert asyncio.run(b._upload(b"x" * (ft.BIG_FILE + 1))) == 102
    assert calls == [("small", 1000), ("big", ft.BIG_FILE + 1)]
    assert b._client.sent == ["INPUTFILE", "INPUTFILEBIG"]


def test_refused_small_upload_falls_back_then_is_disabled(monkeypatch):
    async def boom(pool, data, name):
        raise RuntimeError("Telegram rejected upload part 0")

    monkeypatch.setattr(ft, "upload_small", boom)
    b = _backend()
    for i in range(4):
        assert asyncio.run(b._upload(b"y" * 10)) == 101 + i  # always succeeds
    assert all(isinstance(f, io.BytesIO) for f in b._client.sent)  # stock path
    assert b._small_fast is False  # stopped trying after 3 refusals


def test_single_connection_uses_stock_path(monkeypatch):
    async def never(*a, **k):
        raise AssertionError("pool upload must not be used")

    monkeypatch.setattr(ft, "upload_small", never)
    b = _backend(conns=1)
    assert asyncio.run(b._upload(b"z" * 10)) == 101
    assert isinstance(b._client.sent[0], io.BytesIO)


def test_small_parallel_can_be_switched_off(monkeypatch):
    monkeypatch.setenv("TGFS_SMALL_PARALLEL", "0")

    async def never(*a, **k):
        raise AssertionError("small pool upload must be off")

    monkeypatch.setattr(ft, "upload_small", never)
    b = _backend()
    assert b._small_fast is False
    assert asyncio.run(b._upload(b"z" * 10)) == 101
