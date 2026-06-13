#!/usr/bin/env bash
# Mount tgfs (Telegram-backed FUSE) and launch copyparty on top of it.
#
# copyparty serves the mountpoint as an ordinary directory; file *data* lives in
# your Telegram channel, while copyparty's own index/thumbnails are kept on local
# disk (./hist) for speed. On exit the mountpoint is unmounted.
#
# Prereqs (one-time):
#   cp config.example.toml config.toml   # fill in api_id/api_hash/channel
#   .venv/bin/python -m tgfs.main login   # authorize the Telethon session
#   .venv/bin/python -m tgfs.main smoke   # verify connectivity
#
# Usage:  scripts/run.sh [copyparty args...]
set -euo pipefail
cd "$(dirname "$0")/.."

PY=.venv/bin/python
MNT="$(${PY} -c 'import tgfs.config as c; print(c.load().mount)')"
HIST="${TGFS_HIST:-$PWD/hist}"
PORT="${TGFS_PORT:-3923}"

mkdir -p "$MNT" "$HIST"

cleanup() {
    echo "[run] unmounting $MNT"
    fusermount3 -u "$MNT" 2>/dev/null || fusermount3 -uz "$MNT" 2>/dev/null || true
    [[ -n "${MOUNT_PID:-}" ]] && kill "$MOUNT_PID" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "[run] mounting tgfs at $MNT"
${PY} -m tgfs.main mount "$MNT" &
MOUNT_PID=$!

# wait for the mount to become live
for _ in $(seq 1 50); do
    if mountpoint -q "$MNT" 2>/dev/null || mount | grep -q "fsname=tgfs.*$MNT\|$MNT type fuse"; then
        break
    fi
    sleep 0.2
done
echo "[run] mount ready; starting copyparty on :$PORT"

# data on Telegram (the mount); copyparty index/thumbs on local disk (--hist).
# -v "$MNT::A" shares the whole mount with full perms; adjust to taste.
exec ${PY} -m copyparty \
    -p "$PORT" \
    -v "$MNT::A" \
    --hist "$HIST" \
    "$@"
