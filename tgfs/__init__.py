"""tgfs — a Telegram-backed FUSE filesystem.

Stores file content as chunked blobs in a private Telegram channel (via the
MTProto API / Telethon) while keeping the directory tree and chunk map in a
local SQLite index. Mount it, then run an unmodified file server (copyparty)
on top of the mountpoint.
"""

__version__ = "0.1.0"
