#!/usr/bin/env bash
set -euo pipefail

# Folders đảm bảo tồn tại (vẫn được mount từ host)
mkdir -p "${DATA_ROOT:-/app/database}" /app/logs/logs

# Log banner nhỏ
echo "[BOOT] PAGE=${PAGE_NAME:-} | TAG=${ACCOUNT_TAG:-} | MITM=${MITM_PORT:-8899} | DATA_ROOT=${DATA_ROOT:-/app/database}"

# Base args
ARGS=( \
  "--group-url" "${GROUP_URL:-https://www.facebook.com/thoibao.de}" \
  "--page-name" "${PAGE_NAME:-thoibaode}" \
  "--account-tag" "${ACCOUNT_TAG:-}" \
  "--data-root" "${DATA_ROOT:-/app/database}" \
  "--cookies-path" "${COOKIE_PATH:-}" \
  "--mitm-port" "${MITM_PORT:-8899}" \
)

# Proxy (optional)
if [[ -n "${PROXY_HOST:-}" ]]; then
  ARGS+=("--proxy-host" "${PROXY_HOST}")
fi
if [[ -n "${PROXY_PORT:-}" ]]; then
  ARGS+=("--proxy-port" "${PROXY_PORT}")
fi
if [[ -n "${PROXY_USER:-}" ]]; then
  ARGS+=("--proxy-user" "${PROXY_USER}")
fi
if [[ -n "${PROXY_PASS:-}" ]]; then
  ARGS+=("--proxy-pass" "${PROXY_PASS}")
fi

# Keep last (hook config)
if [[ -n "${KEEP_LAST:-}" ]]; then
  ARGS+=("--keep-last" "${KEEP_LAST}")
fi

# Page limit (test)
if [[ -n "${PAGE_LIMIT:-}" ]]; then
  ARGS+=("--page-limit" "${PAGE_LIMIT}")
fi

# Flags boolean
if [[ "${RESUME:-false}" == "true" ]]; then
  ARGS+=("--resume")
fi

# Headless: mặc định true; nếu HEADLESS=false thì bật --no-headless
if [[ "${HEADLESS:-true}" == "true" ]]; then
  ARGS+=("--headless")
else
  ARGS+=("--no-headless")
fi

# Backfill mode (optional)
if [[ "${BACKFILL:-false}" == "true" ]]; then
  ARGS+=("--backfill")
  if [[ -n "${FROM_MONTH:-}" ]]; then ARGS+=("--from-month" "${FROM_MONTH}"); fi
  if [[ -n "${TO_MONTH:-}" ]]; then ARGS+=("--to-month" "${TO_MONTH}"); fi
  if [[ -n "${YEAR:-}" ]]; then ARGS+=("--year" "${YEAR}"); fi
fi

# Chromium flags via env
export CHROME_BIN=/usr/bin/chromium
export CHROMIUM_FLAGS="${CHROMIUM_FLAGS:-"--no-sandbox --disable-dev-shm-usage --disable-gpu --headless=new"}"

# Run
exec python /app/post/v2/main.py "${ARGS[@]}"
