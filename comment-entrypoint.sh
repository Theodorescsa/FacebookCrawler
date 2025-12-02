#!/usr/bin/env bash
set -euo pipefail

mkdir -p "${DATA_ROOT:-/app/database}" /app/logs/logs

echo "[BOOT] PAGE=${PAGE_NAME:-} | INPUT_EXCEL=${INPUT_EXCEL:-} | COOKIE_PATH=${COOKIE_PATH:-} | DATA_ROOT=${DATA_ROOT:-/app/database} | HEADLESS=${HEADLESS:-true}"

# Chromium flags via env
export CHROME_BIN=/usr/bin/chromium
export CHROMIUM_FLAGS="${CHROMIUM_FLAGS:---no-sandbox --disable-dev-shm-usage --disable-gpu --headless=new}"

ARGS=(
  "--page-name" "${PAGE_NAME:-thoibaode}"
  "--input-excel" "${INPUT_EXCEL:-/app/database/post/page/thoibaode/thoibao-de-last.xlsx}"
  "--cookies-path" "${COOKIE_PATH:-/app/database/facebookaccount/authen_tranhoangdinhnam/cookies.json}"
  "--data-root" "${DATA_ROOT:-/app/database}"
)

# Headless flag
if [[ "${HEADLESS:-true}" == "true" ]]; then
  ARGS+=("--headless")
else
  ARGS+=("--no-headless")
fi

echo "[BOOT] Running: python /app/comment/v3/main_batch.py ${ARGS[*]}"

exec python /app/comment/v3/main_batch.py "${ARGS[@]}"
