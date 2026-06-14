# copytele — copyparty backed by Telegram storage

Run the [copyparty](https://github.com/9001/copyparty) file server, but store the
actual file data in **Telegram** (via the MTProto API) instead of on local disk.

This is done **without modifying copyparty**. A small FUSE filesystem (`tgfs`)
presents a Telegram channel as an ordinary POSIX directory; copyparty runs on top
of the mountpoint and never knows the difference. Because the mount is fully
POSIX-compliant, *all* copyparty features work (browse, resumable chunked upload,
download, rename, dedup, thumbnails, search, shares, …).

```
        copyparty (unmodified, pip-installed)
   --hist -> LOCAL disk (index, thumbs, up2k snapshots)   [fast metadata]
   volume -> ./mnt  (the FUSE mountpoint)                 [user files]
                          |
                   tgfs FUSE daemon (pyfuse3)
              /            |               \
   SQLite metadata   local cache       Telethon (MTProto) -> private channel
   (dir tree,        (read LRU +        (1 message/document per content chunk,
    inode + chunk     writeback temp)    content-addressed + deduplicated)
    map, blobs)
```

The SQLite index is the **source of truth** for the filesystem tree. Telegram
only ever holds opaque, content-addressed blobs.

## How it works

- **Chunking + dedup.** Each file is split into fixed-size chunks (default
  32 MiB). Every chunk is sha256-hashed; identical chunks share one refcounted
  Telegram blob, so duplicate data is stored once. Overwrites only re-upload the
  chunks that actually changed; orphaned blobs are deleted from the channel.
- **Full POSIX writes on immutable blobs.** Telegram blobs can't be edited in
  place, so on the first write-open of a file its content is materialized into a
  local temp file. Reads/writes/truncates hit that temp file; on close (or
  fsync) it is re-chunked and pushed back to Telegram. Random writes, in-place
  edits, append, and `truncate` all work.
- **Read cache.** A bounded on-disk LRU cache (keyed by sha256) avoids
  re-downloading hot chunks.
- **copyparty metadata stays local.** copyparty's own SQLite index, thumbnails
  and upload snapshots are kept on local disk via `--hist`, so feature-heavy
  operations don't thrash the network.

## Requirements

- Linux with FUSE3 (`/dev/fuse`, `fusermount3`).
- Python 3.11+.
- A Telegram account + API credentials from <https://my.telegram.org>.
- A dedicated **private channel** to hold the blobs.

## Setup

### 1. System build deps for pyfuse3

`pyfuse3` compiles against libfuse3. On a normal machine:

```bash
sudo apt install libfuse3-dev pkg-config gcc
```

No root? See `scripts/build_pyfuse3_nosudo.sh`, which fetches the dev headers +
`pkgconf` into a local prefix and builds pyfuse3 against them (this repo was
developed that way).

### 2. Python environment

```bash
uv venv .venv                       # or: python -m venv .venv
uv pip install --python .venv -e ".[server,dev]"
```

### 3. Configure + authorize

```bash
cp config.example.toml config.toml      # fill in api_id, api_hash, channel
.venv/bin/python -m tgfs.main login     # one-time: authorize the Telethon session
.venv/bin/python -m tgfs.main channels  # list channels to find your channel id
.venv/bin/python -m tgfs.main smoke     # round-trip a blob -> proves it works
```

### 4. Run

```bash
scripts/run.sh                          # mounts tgfs + starts copyparty on :3923
# open http://127.0.0.1:3923
```

Everything you upload lands in your Telegram channel as chunked blobs; everything
you download is reassembled from them.

## Docker deployment

A multi-arch image (amd64 + arm64) is built and pushed to GHCR on every commit
by `.github/workflows/docker.yml` — tagged `latest` on the default branch, plus
`vX.Y.Z` on release tags and a `sha-…` tag per commit. The image bundles tgfs +
copyparty + ffmpeg/Pillow/mutagen (thumbnails & tags) + `cryptg` (faster MTProto).

> **The container mounts a FUSE filesystem**, so it needs `/dev/fuse`, the
> `SYS_ADMIN` capability, and an unconfined AppArmor profile. These are already
> set in `docker-compose.yml`.

### One-time: authorize the session

The Telethon session must be created interactively (it asks for your phone number
and login code). Do this once into the persistent volume:

```bash
docker volume create copytele-data
docker run -it --rm \
    -e TGFS_API_ID=123456 -e TGFS_API_HASH=your_hash \
    -e TGFS_CHANNEL=-1001234567890 \
    -v copytele-data:/data \
    ghcr.io/OWNER/copytele:latest login
```

Find your channel id first with the `channels` subcommand (same form, ending in
`channels` instead of `login`).

### Deploy (Portainer or compose)

Edit `docker-compose.yml` (set `OWNER` and the `TGFS_*` values), then in Portainer:
**Stacks → Add stack → Web editor**, paste it, and deploy. Or from a shell:

```bash
docker compose up -d
```

copyparty comes up on port `3923` with all feature flags enabled
(`-e2dsa -e2ts --dedup --xff-src lan`).

### Behind an HTTPS reverse proxy

copyparty only honors forwarded headers from trusted proxy IPs; the image default
`--xff-src lan` trusts private-range proxies (Docker networks, your LAN). Your
proxy **must forward these headers**, or uploads fail with a `cors-check 403`
(copyparty sees plain `http` and the browser's `https://` Origin no longer
matches):

```nginx
location / {
    proxy_pass http://copytele:3923;
    proxy_set_header Host              $host;
    proxy_set_header X-Forwarded-For   $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;   # <- the critical one
    client_max_body_size 0;                       # allow large uploads
}
```

Caddy does this automatically (`reverse_proxy copytele:3923`). If you still see
cors-check rejections, you can instead whitelist your origin explicitly by adding
`--acao https://files.example.com` to the `command:`.

### Notes

- GHCR packages are **private by default** — either make the package public in
  your GitHub *Packages* settings, or add the registry credentials in Portainer.
- Everything under `/data` (session, `meta.db`, cache, copyparty hist) persists in
  the `copytele-data` volume. **Back up `meta.db`** — it's the only map from your
  files to their Telegram chunks.
- If `mount` fails on an unusual host, also add `security_opt: ["seccomp=unconfined"]`.

## Commands

| command | purpose |
|---|---|
| `tgfs login` | interactively authorize/create the Telethon session file |
| `tgfs channels` | list channels you can post to (to find the channel id) |
| `tgfs smoke` | upload→download→delete one blob; connectivity proof |
| `tgfs mount [MNT]` | mount the filesystem (foreground) |

## Testing

```bash
.venv/bin/python -m pytest                      # unit + POSIX core (mock backend, no network)
.venv/bin/python scripts/mount_smoke.py         # real kernel FUSE mount, mock backend
.venv/bin/python scripts/copyparty_e2e.py       # copyparty over a real tgfs mount, HTTP up/down
```

The test suite uses an in-memory backend, so it is fast, deterministic, and needs
no Telegram credentials. `scripts/mount_smoke.py` and `scripts/copyparty_e2e.py`
mount a real FUSE filesystem and verify behavior through actual syscalls / HTTP.

## Parity is behavioral, not free

True feature parity is achieved by making the mount fully POSIX. The cost is
**performance, not capability**:

- In-place edits and thumbnail generation must download the whole file first.
- The first read of a cold file fetches its chunks from Telegram (then cached).
- Telegram rate limits (`FloodWait`) are honored with backoff; bursts are slower.

Mitigations: the local read cache, keeping copyparty's index/thumbs on local
disk (`--hist`), and content-addressed dedup.

## Limits & notes

- Telegram allows ~2 GB/file (4 GB premium); chunking (default 32 MiB) keeps
  every blob well under that, so file size is effectively unbounded.
- Don't point two tgfs instances at the same channel + metadata DB simultaneously.
- Secrets (`config.toml`, `*.session`) are gitignored. Keep them safe — the
  session file is full account access.
- `tgfs` uses **pyfuse3** (libfuse3). fusepy was the original plan but only
  speaks the libfuse2 ABI, unavailable on libfuse3-only hosts.

## Layout

```
tgfs/
  config.py       config.toml + env loading
  asyncbridge.py  asyncio loop thread <-> sync FUSE threads
  telegram.py     Telethon MTProto blob backend (+ FloodWait/retry)
  backend.py      Backend interface + in-memory mock (for tests)
  meta.py         SQLite metadata index (tree, inodes, chunks, blobs)
  store.py        chunking + content-addressed dedup
  cache.py        read LRU cache + writeback manager
  fuse_ops.py     synchronous, binding-agnostic FsCore (all POSIX semantics)
  mount.py        pyfuse3 adapter + mount entrypoint
  main.py         CLI (login / channels / smoke / mount)
scripts/
  run.sh              mount tgfs + launch copyparty
  mount_smoke.py      real-kernel FUSE smoke test (mock backend)
  copyparty_e2e.py    copyparty-over-tgfs HTTP end-to-end (mock backend)
```
