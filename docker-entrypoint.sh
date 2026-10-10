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
    if [ -n "$MOUNT_PID" ]; then
        kill "$MOUNT_PID" 2>/dev/null || true
        # let tgfs finish its final writeback flush to Telegram
        wait "$MOUNT_PID" 2>/dev/null || true
    fi
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

# Uploads are written to a local cache first and sent to Telegram a little later.
# If the container was killed before that finished, push the leftovers now --
# mounting would delete them as stale. If it fails, stop (files stay in the cache);
# set TGFS_RECOVER_FORCE=1 to start anyway and give them up.
echo "[entrypoint] checking for unflushed uploads from a previous run"
if ! python -m tgfs.main recover; then
    if [ "${TGFS_RECOVER_FORCE:-0}" = "1" ]; then
        echo "[entrypoint] WARNING: recovery failed, continuing (TGFS_RECOVER_FORCE=1)"
    else
        echo "[entrypoint] ERROR: could not upload unflushed files; leaving them in"
        echo "             ${TGFS_CACHE_DIR:-/data/cache}/wb. Fix connectivity and restart,"
        echo "             or set TGFS_RECOVER_FORCE=1 to discard them."
        exit 1
    fi
fi

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

# Login: set CP_USER + CP_PASS to require a password for everything (web UI and
# WebDAV; any username works with the right password, so clients such as the
# iPhone Files app can use either). Without them the share is open to anyone
# who can reach the port -- fine on a LAN, not behind a public hostname.
CP_USER="${CP_USER:-}"
CP_PASS="${CP_PASS:-}"
if [ -n "$CP_USER" ] || [ -n "$CP_PASS" ]; then
    if [ -z "$CP_USER" ] || [ -z "$CP_PASS" ]; then
        echo "[entrypoint] ERROR: set both CP_USER and CP_PASS (or neither)"
        exit 1
    fi
    echo "[entrypoint] login enabled for user '$CP_USER'"
    AUTH_ARGS=(-a "$CP_USER:$CP_PASS" -v "$MNT::rwmda,$CP_USER" --dav-auth)
else
    echo "[entrypoint] WARNING: CP_USER/CP_PASS not set -- no login, anonymous full access"
    AUTH_ARGS=(-v "$MNT::A")
fi

# Inbox: uploads into /<CP_INBOX>/ (default "iphone") are sorted by file type into
# /photos, /videos and /files; uploads anywhere else are left where they are.
# CP_INBOX="" turns sorting off.
export CP_INBOX="${CP_INBOX-iphone}"
HOOK_ARGS=()
if [ -n "$CP_INBOX" ]; then
    echo "[entrypoint] sorting uploads in /$CP_INBOX/ into /photos /videos /files"
    HOOK_ARGS=(--xbu "j,c1,/usr/local/share/tgfs/hooks/sort-uploads.py")
fi

# data on Telegram (the mount); copyparty index/thumbs on local disk (--hist)
python -m copyparty \
    -i 0.0.0.0 -p "$PORT" \
    "${AUTH_ARGS[@]}" \
    "${HOOK_ARGS[@]}" \
    --hist "$HIST" \
    "$@" &
CP_PID=$!
wait "$CP_PID"
