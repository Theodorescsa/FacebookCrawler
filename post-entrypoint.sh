#!/usr/bin/env bash
set -euo pipefail

# Tạo sẵn thư mục DB & logs (vẫn mount từ host được bình thường)
mkdir -p "${DATA_ROOT:-/app/database}" /app/logs/logs

# Log banner nhỏ cho dễ debug
echo "[BOOT] PAGE=${PAGE_NAME:-} | TAG=${ACCOUNT_TAG:-} | DATA_ROOT=${DATA_ROOT:-/app/database} | DATE=${CRAWL_DATE:-today}"

# Base args đúng theo post/v3/cli.py
ARGS=(
  "--group-url"    "${GROUP_URL:-https://www.facebook.com/1024013528523184}"
  "--page-name"    "${PAGE_NAME:-thoibaode}"
  "--account-tag"  "${ACCOUNT_TAG:-}"
  "--data-root"    "${DATA_ROOT:-/app/database}"
  "--cookies-path" "${COOKIE_PATH:-}"
)

# KEEP_LAST (optional) – nếu không set thì dùng default trong code (350)
if [[ -n "${KEEP_LAST:-}" ]]; then
  ARGS+=("--keep-last" "${KEEP_LAST}")
fi

# Giới hạn số lượt scroll (optional)
if [[ -n "${PAGE_LIMIT:-}" ]]; then
  ARGS+=("--page-limit" "${PAGE_LIMIT}")
fi

# Headless: mặc định true; nếu HEADLESS=false thì dùng --no-headless
if [[ "${HEADLESS:-true}" == "true" ]]; then
  ARGS+=("--headless")
else
  ARGS+=("--no-headless")
fi

# Crawl theo ngày cụ thể (optional)
# Ví dụ: CRAWL_DATE=2025-12-01 → '--date 2025-12-01'
if [[ -n "${CRAWL_DATE:-}" ]]; then
  ARGS+=("--date" "${CRAWL_DATE}")
fi

# Chromium flags via env – create_chrome sẽ đọc CHROME_BIN/CHROMIUM_FLAGS
export CHROME_BIN="${CHROME_BIN:-/usr/bin/chromium}"
# Lưu ý: dùng cú pháp ${VAR:---no-sandbox ...} để không bị lỗi quote
export CHROMIUM_FLAGS="${CHROMIUM_FLAGS:---no-sandbox --disable-dev-shm-usage --disable-gpu --headless=new}"

# Run: quan trọng là chạy dạng MODULE để relative import trong post/v3/cli.py chạy được
exec python -m post.v3.cli "${ARGS[@]}"
