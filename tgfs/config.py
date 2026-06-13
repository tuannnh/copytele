"""Configuration loading for tgfs.

Reads ``config.toml`` (TOML) and applies environment-variable overrides of the
form ``TGFS_<KEY>``. Secrets live only in config.toml / the environment and are
never committed (config.toml is gitignored).
"""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_UNITS = {
    "": 1,
    "b": 1,
    "k": 1000,
    "kb": 1000,
    "kib": 1024,
    "m": 1000**2,
    "mb": 1000**2,
    "mib": 1024**2,
    "g": 1000**3,
    "gb": 1000**3,
    "gib": 1024**3,
}


def parse_size(v: Any) -> int:
    """Parse a human size like '32MiB' / '5GiB' / 1048576 into bytes."""
    if isinstance(v, int):
        return v
    s = str(v).strip().lower().replace(" ", "")
    num = s
    unit = ""
    for i, ch in enumerate(s):
        if not (ch.isdigit() or ch == "."):
            num, unit = s[:i], s[i:]
            break
    if unit not in _UNITS:
        raise ValueError(f"unknown size unit in {v!r}")
    return int(float(num) * _UNITS[unit])


@dataclass
class Config:
    api_id: int
    api_hash: str
    session: Path
    channel: int | str
    chunk_size: int
    cache_cap: int
    meta_db: Path
    cache_dir: Path
    mount: Path
    root: Path  # directory the config lives in; relative paths resolve against it

    def resolve(self, p: str | Path) -> Path:
        p = Path(p)
        return p if p.is_absolute() else (self.root / p)


def _env(key: str) -> str | None:
    return os.environ.get(f"TGFS_{key.upper()}")


def load(path: str | Path = "config.toml") -> Config:
    path = Path(path).resolve()
    data: dict[str, Any] = {}
    if path.exists():
        with open(path, "rb") as f:
            data = tomllib.load(f)
    elif not (_env("API_ID") and _env("API_HASH")):
        raise FileNotFoundError(
            f"{path} not found and TGFS_API_ID/TGFS_API_HASH not set; "
            "copy config.example.toml to config.toml and fill it in"
        )

    tg = data.get("telegram", {})
    st = data.get("storage", {})
    pa = data.get("paths", {})
    root = path.parent

    def pick(section: dict, key: str, default: Any = None) -> Any:
        ev = _env(key)
        if ev is not None:
            return ev
        return section.get(key, default)

    channel: int | str = pick(tg, "channel")
    if isinstance(channel, str) and channel.lstrip("-").isdigit():
        channel = int(channel)

    cfg = Config(
        api_id=int(pick(tg, "api_id")),
        api_hash=str(pick(tg, "api_hash")),
        session=root / str(pick(tg, "session", "tgfs.session")),
        channel=channel,
        chunk_size=parse_size(pick(st, "chunk_size", "32MiB")),
        cache_cap=parse_size(pick(st, "cache_cap", "5GiB")),
        meta_db=root / str(pick(pa, "meta_db", "data/meta.db")),
        cache_dir=root / str(pick(pa, "cache_dir", "data/cache")),
        mount=root / str(pick(pa, "mount", "mnt")),
        root=root,
    )
    return cfg
