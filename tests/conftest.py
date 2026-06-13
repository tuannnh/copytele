import stat

import pytest

from tgfs.backend import MemoryBackend
from tgfs.meta import ROOT_INO, Meta
from tgfs.store import Store


@pytest.fixture
def meta(tmp_path):
    m = Meta(tmp_path / "meta.db")
    yield m
    m.close()


@pytest.fixture
def backend():
    return MemoryBackend()


@pytest.fixture
def store(meta, backend):
    # small chunk size so tests exercise multi-chunk paths cheaply
    return Store(meta, backend, chunk_size=16)


@pytest.fixture
def core(store, meta, tmp_path):
    from tgfs.cache import WritebackManager
    from tgfs.fuse_ops import FsCore

    wb = WritebackManager(store, meta, tmp_path / "wbcache")
    return FsCore(meta, store, wb)


@pytest.fixture
def root():
    return ROOT_INO


def mkfile(meta, parent, name, mode=0o644):
    return meta.create(parent, name, stat.S_IFREG | mode, "f")
