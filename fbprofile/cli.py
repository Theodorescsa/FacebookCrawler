# post/v3/cli.py
import os
import sys
import signal
import time
from pathlib import Path
from datetime import datetime, date
from typing import Set

from logs.loging_config import logger
from util.startdriverproxy import bootstrap_auth

from .config import PROJECT_ROOT, env
from .browser.driver import create_chrome, make_headless
from .browser.hooks import install_early_hook
from .browser.navigation import go_to_date
from .browser.scroll import crawl_scroll_loop, set_stop_flag
# Import hàm mới tạo
from .browser.get_profile_info import scrape_full_profile_info
from .browser.login import fb_login
from .storage.paths import compute_paths
from .storage.checkpoint import save_checkpoint
from . import pipeline

def add_common_args(ap):
    ap.add_argument("--group-url", type=str,
                    default=env("GROUP_URL", "https://www.facebook.com/profile.php?id=61571413447387"))
    ap.add_argument("--page-name", type=str,
                    default=env("PAGE_NAME", "thoibaode"))
    ap.add_argument("--account-tag", type=str,
                    default=env("ACCOUNT_TAG", ""),
                    help="Nhãn để tách dữ liệu theo account (vd: acc01)")
    ap.add_argument("--data-root", type=str,
                    default=env("DATA_ROOT", str(PROJECT_ROOT / "database")),
                    help="Thư mục gốc database")
    ap.add_argument("--cookies-path", type=str,
                    default=env("COOKIE_PATH", ""),
                    help="Đường dẫn file cookies.json")
    ap.add_argument("--keep-last", type=int,
                    default=env("KEEP_LAST", 350, int),
                    help="Số bản ghi GQL giữ lại trong window.__gqlReqs")
    ap.add_argument("--headless", action="store_true",
                    help="Chạy headless (ẩn Chrome)")
    ap.add_argument("--no-headless", action="store_true",
                    help="Force non-headless")
    ap.add_argument("--page-limit", type=int,
                    default=env("PAGE_LIMIT", None, int),
                    help="Giới hạn lượt scroll (None = 10000)")
    ap.add_argument("--date", type=str,
                    help="YYYY-MM-DD (ngày cần crawl, mặc định = hôm nay)")
    # Thêm cờ để bật/tắt quét info (optional)
    ap.add_argument("--skip-info", action="store_true",
                    help="Bỏ qua bước quét thông tin profile")


def _handle_sigterm(sig, frame):
    logger.warning("[SIGNAL] Nhận tín hiệu dừng, sẽ thoát an toàn sau bước hiện tại.")
    set_stop_flag()


signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)

def _run_single_session(
    *,
    args,
    group_url: str,
    target_date: date,
    out_ndjson: Path,
    profile_info_path: Path, # Thêm tham số đường dẫn file info
    keep_last: int,
    seen_ids: Set[str],
    cookies: str,
):
    """
    Chạy 1 phiên Chrome: open, [scrape_profile], go_to_date, crawl_scroll_loop, quit.
    """
    d = create_chrome(headless=make_headless(args))
    if cookies and os.path.exists(cookies):
        try:
            bootstrap_auth(d, cookies)
            logger.info("[AUTH] bootstrap_auth OK with cookies %s", cookies)
        except Exception as e:
            logger.error("[AUTH] bootstrap_auth FAILED: %s", e)
    else:
        logger.warning("[AUTH] Không tìm thấy cookies: %s", cookies)
    try:
        d.execute_cdp_cmd("Network.enable", {})
        d.execute_cdp_cmd("Network.setCacheDisabled", {"cacheDisabled": True})
    except Exception as e:
        logger.warning("[CDP] Cannot enable network/disable cache: %s", e)

    try:
        install_early_hook(d, keep_last=keep_last)
        logger.info("[HOOK] install_early_hook OK (keep_last=%s)", keep_last)
    except Exception as e:
        logger.error("[HOOK] install_early_hook FAILED: %s", e)

    try:
        try:
            scrape_full_profile_info(d, group_url, profile_info_path)
        except Exception as e:
            logger.error(f"Lỗi khi crawl profile info: {e}")

        d.get(group_url)
        time.sleep(1.5)
        
        if "group" not in group_url:
            go_to_date(d, target_date)

        stopped_due_to_stall = crawl_scroll_loop(
            d,
            group_url=group_url,
            out_path=out_ndjson,
            seen_ids=seen_ids,
            keep_last=keep_last,
            max_scrolls=args.page_limit or 10000,
        )
    finally:
        try:
            d.quit()
        except Exception:
            pass

    return stopped_due_to_stall

def main(argv=None):
    import argparse

    print(">>> ĐANG KHỞI CHẠY SCRIPT...")

    ap = argparse.ArgumentParser("FB Post Crawler (single-day, clean)")
    add_common_args(ap)
    args = ap.parse_args(argv)

    # Đã sửa lại PROJECT_ROOT đúng như cuộc hội thoại trước
    print(f">>> DEBUG: PROJECT_ROOT hiện tại là: {PROJECT_ROOT}")

    group_url = args.group_url.strip()
    page_name = args.page_name.strip()
    account_tag = (args.account_tag or "").strip()
    data_root = Path(args.data_root).resolve()
    keep_last = int(args.keep_last)
    cookies = args.cookies_path.strip()
    print("cookies: ", cookies)
    
    # Tính toán đường dẫn
    database_path, out_ndjson, raw_dumps_dir, checkpoint = compute_paths(
        data_root, page_name, account_tag
    )
    
    # Định nghĩa đường dẫn file info profile
    profile_info_path = database_path / "profile_info.json"

    logger.info(
        "[BOOT] PAGE=%s | TAG=%s | DATA_ROOT=%s",
        page_name,
        account_tag or "-",
        data_root,
    )
    logger.info(
        "[PATH] DB=%s | OUT=%s | INFO=%s", database_path, out_ndjson, profile_info_path
    )

    if args.date:
        try:
            target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError:
            raise SystemExit("--date phải ở format YYYY-MM-DD, ví dụ: 2024-12-01")
    else:
        target_date = date.today()

    logger.info("====== CRAWL NGÀY %s ======", target_date.isoformat())

    seen_ids: Set[str] = set()

    MAX_STALL_RETRIES = 3
    stall_retry_count = 0
    current_target_date = target_date

    while True:
        logger.info(
            "[SESSION] Bắt đầu phiên crawl (retry #%d) với target_date=%s",
            stall_retry_count,
            current_target_date.isoformat(),
        )

        stopped_due_to_stall = _run_single_session(
            args=args,
            group_url=group_url,
            target_date=current_target_date,
            out_ndjson=out_ndjson,
            profile_info_path=profile_info_path, # Truyền tham số mới vào
            keep_last=keep_last,
            seen_ids=seen_ids,
            cookies=cookies,
        )

        # nếu không phải stall -> kết thúc luôn
        if not stopped_due_to_stall:
            logger.info("[SESSION] Dừng không phải do stall, kết thúc crawler.")
            break

        stall_retry_count += 1
        logger.info(
            "[SESSION] Bị stall lần thứ %d (max=%d).",
            stall_retry_count,
            MAX_STALL_RETRIES,
        )

        if stall_retry_count >= MAX_STALL_RETRIES:
            logger.info(
                "[SESSION] Đã đạt số lần stall tối đa (%d), dừng hẳn.",
                MAX_STALL_RETRIES,
            )
            break

        if pipeline.EARLIEST_CREATED_TS is None:
            logger.warning(
                "[SESSION] EARLIEST_CREATED_TS chưa có, không thể tính ngày mới. Dừng."
            )
            break

        new_date = datetime.fromtimestamp(
            pipeline.EARLIEST_CREATED_TS
        ).date()
        logger.info(
            "[SESSION] Sau stall, đặt target_date mới = %s (from created_time=%d).",
            new_date.isoformat(),
            pipeline.EARLIEST_CREATED_TS,
        )
        current_target_date = new_date

    if pipeline.LATEST_CREATED_TS is not None:
        save_checkpoint(checkpoint, pipeline.LATEST_CREATED_TS)

    logger.info(
        "[DONE] Finished crawl for %s. Total unique posts: %d",
        target_date,
        len(seen_ids),
    )


if __name__ == "__main__":
    main()