import io
import stat

from tgfs.meta import ROOT_INO


def _put(store, ino, data: bytes):
    store.put_file(ino, io.BytesIO(data), len(data))


def _mkfile(meta, name="f"):
    return meta.create(ROOT_INO, name, stat.S_IFREG | 0o644, "f").id


def test_roundtrip_multichunk(store, meta):
    ino = _mkfile(meta)
    data = bytes(range(256)) * 4  # 1024 bytes -> 64 chunks of 16
    _put(store, ino, data)
    assert meta.get_inode(ino).size == len(data)
    assert store.read_range(ino, 0, len(data)) == data


def test_partial_reads(store, meta):
    ino = _mkfile(meta)
    data = bytes(range(100))
    _put(store, ino, data)
    assert store.read_range(ino, 10, 5) == data[10:15]
    assert store.read_range(ino, 0, 1) == data[0:1]
    assert store.read_range(ino, 95, 100) == data[95:]  # clamps past EOF
    assert store.read_range(ino, 1000, 10) == b""


def test_empty_file(store, meta):
    ino = _mkfile(meta)
    _put(store, ino, b"")
    assert meta.get_inode(ino).size == 0
    assert meta.get_chunks(ino) == []
    assert store.read_range(ino, 0, 10) == b""


def test_dedup_identical_chunks_within_file(store, meta, backend):
    ino = _mkfile(meta)
    # 4 identical 16-byte chunks -> only ONE blob uploaded
    _put(store, ino, b"A" * 16 * 4)
    assert backend.uploads == 1
    blob = meta.blob_get(meta.get_chunk_shas(ino)[0])
    assert blob["refcount"] == 4


def test_dedup_across_files(store, meta, backend):
    a = _mkfile(meta, "a")
    b = _mkfile(meta, "b")
    payload = b"hello world 1234" * 3  # 48 bytes -> 3 chunks
    _put(store, a, payload)
    n = backend.uploads
    _put(store, b, payload)  # identical content -> no new uploads
    assert backend.uploads == n
    # both files readable & correct
    assert store.read_range(a, 0, 48) == payload
    assert store.read_range(b, 0, 48) == payload


def test_overwrite_gcs_orphaned_blobs(store, meta, backend):
    ino = _mkfile(meta)
    _put(store, ino, b"X" * 16 * 3)  # 3 chunks, 1 unique blob (refcount 3)
    sha_old = meta.get_chunk_shas(ino)[0]
    _put(store, ino, b"Y" * 16 * 3)  # fully different
    assert backend.deletes == 1  # old blob removed from Telegram
    assert meta.blob_get(sha_old) is None
    assert store.read_range(ino, 0, 48) == b"Y" * 48


def test_overwrite_partial_keeps_shared_blobs(store, meta, backend):
    ino = _mkfile(meta)
    # chunk0='AAAA...'(16), chunk1='BBBB...'(16)
    _put(store, ino, b"A" * 16 + b"B" * 16)
    deletes0, uploads0 = backend.deletes, backend.uploads
    # keep chunk0, change chunk1 -> exactly one delete + one upload
    _put(store, ino, b"A" * 16 + b"C" * 16)
    assert backend.uploads == uploads0 + 1
    assert backend.deletes == deletes0 + 1
    assert store.read_range(ino, 0, 32) == b"A" * 16 + b"C" * 16


def test_free_file_releases_blobs(store, meta, backend):
    ino = _mkfile(meta)
    _put(store, ino, b"Z" * 16 * 2)
    store.free_file(ino)
    assert backend.deletes == 1
    assert meta.get_chunks(ino) == []
