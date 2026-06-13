#!/usr/bin/env bash
# Container entrypoint: mount tgfs (Telegram-backed FUSE), then run copyparty on
# top of it. Any arguments are passed through to copyparty (the Dockerfile CMD
# supplies the default feature flags).
#
# Maintenance subcommands run tgfs directly instead of serving:
#   docker run -it --rm <env+vol> IMAGE login      # authorize the session
#   docker run -it --rm <env+vol> IMAGE channels   # list channel ids
#   docker run --rm     <env+vol> IMAGE smoke      # connectivity test
set -euo pipefail

case "${1:-}" in
    login | channels | smoke)
        exec python -m tgfs.main "$@"
        ;;
esac

MNT="${TGFS_MOUNT:-/mnt/tgfs}"
HIST="${TGFS_HIST:-/data/hist}"
PORT="${TGFS_PORT:-3923}"

mkdir -p "$MNT" "$HIST" "${TGFS_CACHE_DIR:-/data/cache}" \
    "$(dirname "${TGFS_META_DB:-/data/meta.db}")"

MOUNT_PID=""
CP_PID=""
cleanup() {
    [ -n "$CP_PID" ] && kill "$CP_PID" 2>/dev/null || true
    echo "[entrypoint] unmounting $MNT"
    fusermount3 -u "$MNT" 2>/dev/null || fusermount3 -uz "$MNT" 2>/dev/null || true
    [ -n "$MOUNT_PID" ] && kill "$MOUNT_PID" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

if [ ! -f "${TGFS_SESSION:-/data/tgfs.session}" ]; then
    echo "[entrypoint] ERROR: no Telethon session at ${TGFS_SESSION:-/data/tgfs.session}"
    echo "             run the image once interactively to authorize, e.g.:"
    echo "             docker run -it --rm -e TGFS_API_ID -e TGFS_API_HASH \\"
    echo "                 -v copytele-data:/data <image> login"
    exit 1
fi

is_mounted() { awk -v m="$MNT" '$2==m{f=1} END{exit !f}' /proc/mounts; }

echo "[entrypoint] mounting tgfs at $MNT"
python -m tgfs.main mount "$MNT" &
MOUNT_PID=$!

# wait for the mount to come live (or fail fast if the mount process dies)
for _ in $(seq 1 100); do
    if is_mounted; then break; fi
    if ! kill -0 "$MOUNT_PID" 2>/dev/null; then
        echo "[entrypoint] ERROR: tgfs mount process exited during startup"
        exit 1
    fi
    sleep 0.2
done
is_mounted || { echo "[entrypoint] ERROR: mount not ready"; exit 1; }
echo "[entrypoint] mount ready; starting copyparty on :$PORT"

# data on Telegram (the mount); copyparty index/thumbs on local disk (--hist)
python -m copyparty \
    -i 0.0.0.0 -p "$PORT" \
    -v "$MNT::A" \
    --hist "$HIST" \
    "$@" &
CP_PID=$!
wait "$CP_PID"
