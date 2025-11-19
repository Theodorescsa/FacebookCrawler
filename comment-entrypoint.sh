#!/usr/bin/env bash
set -euo pipefail

mkdir -p "${DATA_ROOT:-/app/database}" /app/logs/logs

echo "[BOOT] PAGE=${PAGE_NAME:-} | TAG=${ACCOUNT_TAG:-} | MITM=${MITM_PORT:-8899} | DATA_ROOT=${DATA_ROOT:-/app/database}"

ARGS=( \
    "--page-name" "${PAGE_NAME:-thoibaode}" \
    "--sheet-name" "${SHEET_NAME:-Sheet_3}" \
    "--cookies-path" "${COOKIE_PATH:-}" \
    "--mitm-port" "${MITM_PORT:-8899}" \
    "--input-excel" "${INPUT_EXCEL:-/app/database/post/page/thoibaode/thoibao-de-last-split-sheet3.xlsx}" \
)

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

# Headless: mặc định true; nếu HEADLESS=false thì bật --no-headless
if [[ "${HEADLESS:-true}" == "true" ]]; then
  ARGS+=("--headless")
else
  ARGS+=("--no-headless")
fi

# Chromium flags via env
export CHROME_BIN=/usr/bin/chromium
# NOTE: avoid nested quotes inside parameter expansion
export CHROMIUM_FLAGS="${CHROMIUM_FLAGS:---no-sandbox --disable-dev-shm-usage --disable-gpu --headless=new}"

# Run
exec python /app/comment/v2/crawler_from_excel_to_ndjson.py "${ARGS[@]}"
