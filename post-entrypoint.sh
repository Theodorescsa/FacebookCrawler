#!/usr/bin/env bash
set -euo pipefail

# Tạo sẵn thư mục DB & logs
mkdir -p "${DATA_ROOT:-/app/database}" /app/logs

# Log banner nhỏ cho dễ debug
echo "[BOOT] PAGE=${PAGE_NAME:-} | TAG=${ACCOUNT_TAG:-} | TIMEOUT=${TIMEOUT:-0} | DATE=${CRAWL_DATE:-today}"

# Khởi tạo các tham số cơ bản
ARGS=(
  "--page-name"    "${PAGE_NAME:-thoibaode}"
  "--account-tag"  "${ACCOUNT_TAG:-}"
  "--data-root"    "${DATA_ROOT:-/app/database}"
  "--cookies-path" "${COOKIE_PATH:-}"
)

# --- Xử lý GROUP_URLS (List) ---
# Mặc định fallback nếu không có ENV
DEFAULT_URL="https://www.facebook.com/thoibao.de#"
TARGET_URLS="${GROUP_URLS:-$DEFAULT_URL}"

# Quan trọng: Không để quote quanh ${TARGET_URLS} để bash tách chuỗi thành các arguments riêng lẻ
# python argparse nargs='+' cần: --group-urls url1 url2 url3
ARGS+=( "--group-urls" ${TARGET_URLS} )

# --- Xử lý TIMEOUT ---
if [[ -n "${TIMEOUT:-}" ]]; then
  ARGS+=("--timeout" "${TIMEOUT}")
fi

# --- Xử lý KEEP_LAST ---
if [[ -n "${KEEP_LAST:-}" ]]; then
  ARGS+=("--keep-last" "${KEEP_LAST}")
fi

# --- Xử lý PAGE_LIMIT ---
if [[ -n "${PAGE_LIMIT:-}" ]]; then
  ARGS+=("--page-limit" "${PAGE_LIMIT}")
fi

# --- Xử lý HEADLESS ---
if [[ "${HEADLESS:-true}" == "true" ]]; then
  ARGS+=("--headless")
else
  ARGS+=("--no-headless")
fi

# --- Xử lý CRAWL_DATE ---
if [[ -n "${CRAWL_DATE:-}" ]]; then
  ARGS+=("--date" "${CRAWL_DATE}")
fi

# Config cho Chromium
export CHROME_BIN="${CHROME_BIN:-/usr/bin/chromium}"
export CHROMIUM_FLAGS="${CHROMIUM_FLAGS:---no-sandbox --disable-dev-shm-usage --disable-gpu --headless=new}"

# In ra lệnh thực thi để debug (optional)
echo "[EXEC] Running: python -m post.v3.cli ${ARGS[*]}"

# Run
exec python -m post.v3.cli "${ARGS[@]}"