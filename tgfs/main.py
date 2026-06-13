"""tgfs command-line entrypoint.

Subcommands:
  login     interactively create/authorize the Telethon session file
  channels  list channels you can post to (to find the storage channel id)
  smoke     round-trip a blob through the configured channel (connectivity proof)
  mount     mount the Telegram-backed filesystem (see fuse_ops/M4)
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

from . import config as cfgmod


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def cmd_login(args) -> int:
    from telethon import TelegramClient

    cfg = cfgmod.load(args.config)
    client = TelegramClient(str(cfg.session), cfg.api_id, cfg.api_hash)
    # start() prompts for phone number, login code, and 2FA password on stdin
    client.start()
    me = client.loop.run_until_complete(client.get_me())
    print(f"authorized as {getattr(me, 'username', None) or me.id}")
    print(f"session saved to {cfg.session}")
    client.disconnect()
    return 0


def cmd_channels(args) -> int:
    from telethon import TelegramClient

    cfg = cfgmod.load(args.config)
    client = TelegramClient(str(cfg.session), cfg.api_id, cfg.api_hash)

    async def _run():
        await client.connect()
        try:
            if not await client.is_user_authorized():
                print("not authorized; run `tgfs login` first", file=sys.stderr)
                return None
            out = []
            async for dialog in client.iter_dialogs():
                if dialog.is_channel:
                    out.append((dialog.id, getattr(dialog.entity, "title", "?")))
            return out
        finally:
            await client.disconnect()

    rows = client.loop.run_until_complete(_run())
    if rows is None:
        return 1
    print(f"{'id':>16}  title")
    for cid, title in rows:
        print(f"{cid:>16}  {title}")
    print("\nset the chosen id as `channel` in config.toml")
    return 0


def cmd_smoke(args) -> int:
    import hashlib

    from .asyncbridge import AsyncLoop
    from .telegram import TelegramBackend

    cfg = cfgmod.load(args.config)
    loop = AsyncLoop()
    loop.start()
    backend = TelegramBackend(cfg, loop)
    try:
        payload = b"tgfs smoke test " + os.urandom(4096)
        want = hashlib.sha256(payload).hexdigest()
        print(f"uploading {len(payload)} bytes (sha256={want[:12]}...)")
        handle = backend.upload(payload)
        print(f"  -> stored as message id {handle}")
        got = backend.download(handle)
        ok = hashlib.sha256(got).hexdigest() == want
        print(f"download round-trip: {'OK' if ok else 'MISMATCH'} ({len(got)} bytes)")
        backend.delete(handle)
        print("deleted test message")
        return 0 if ok else 2
    finally:
        backend.close()
        loop.stop()


def cmd_mount(args) -> int:
    from .mount import mount

    cfg = cfgmod.load(args.config)
    point = args.mountpoint or str(cfg.mount)
    return mount(cfg, point, foreground=not args.background, debug=args.verbose)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="tgfs", description=__doc__)
    p.add_argument("-c", "--config", default="config.toml", help="path to config.toml")
    p.add_argument("-v", "--verbose", action="store_true")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("login", help="authorize the Telethon session").set_defaults(
        func=cmd_login
    )
    sub.add_parser("channels", help="list channels you can post to").set_defaults(
        func=cmd_channels
    )
    sub.add_parser("smoke", help="round-trip a blob through the channel").set_defaults(
        func=cmd_smoke
    )
    pm = sub.add_parser("mount", help="mount the filesystem")
    pm.add_argument("mountpoint", nargs="?", help="override config mountpoint")
    pm.add_argument("-b", "--background", action="store_true", help="daemonize")
    pm.set_defaults(func=cmd_mount)

    args = p.parse_args(argv)
    _setup_logging(args.verbose)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
