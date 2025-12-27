# post/v3/cli.py
import os
import sys
import signal
import time
import threading
from pathlib import Path
from datetime import datetime, date
from typing import Set, List

from logs.loging_config import logger
from util.startdriverproxy import bootstrap_auth

from .config import PROJECT_ROOT, env
from .browser.driver import create_chrome, make_headless
from .browser.hooks import install_early_hook
from .browser.navigation import go_to_date

# --- SỬA IMPORT TẠI ĐÂY ---
# Import thêm reset_stop_flag
from .browser.scroll import crawl_scroll_loop, set_stop_flag, reset_stop_flag

from .storage.paths import compute_paths
from .storage.checkpoint import save_checkpoint
from . import pipeline

def add_common_args(ap):
    ap.add_argument("--group-urls", type=str, nargs='+',
                    default=[env("GROUP_URL", "https://www.facebook.com/le.t.khoa.1#")],
                    help="Danh sách URL cần crawl")
    ap.add_argument("--timeout", type=int,
                    default=env("CRAWL_TIMEOUT", 0, int),
                    help="Timeout (phút) cho mỗi URL")
    ap.add_argument("--page-name", type=str, default=env("PAGE_NAME", "thoibaode"))
    ap.add_argument("--account-tag", type=str, default=env("ACCOUNT_TAG", ""))
    ap.add_argument("--data-root", type=str, default=env("DATA_ROOT", str(PROJECT_ROOT / "database")))
    ap.add_argument("--cookies-path", type=str, default=env("COOKIE_PATH", ""))
    ap.add_argument("--keep-last", type=int, default=env("KEEP_LAST", 350, int))
    ap.add_argument("--headless", action="store_true")
    ap.add_argument("--no-headless", action="store_true")
    ap.add_argument("--page-limit", type=int, default=env("PAGE_LIMIT", None, int))
    ap.add_argument("--date", type=str, help="YYYY-MM-DD")

IS_TIMEOUT_TRIGGERED = False

def _handle_sigterm(sig, frame):
    if not IS_TIMEOUT_TRIGGERED:
        logger.warning("[SIGNAL] Nhận tín hiệu dừng hệ thống.")
    set_stop_flag()

signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)

def _run_single_session(
    *,
    args,
    group_url: str,
    target_date: date,
    out_ndjson: Path,
    keep_last: int,
    seen_ids: Set[str],
    cookies: str,
):
    d = create_chrome(headless=make_headless(args))

    if cookies and os.path.exists(cookies):
        try:
            if not bootstrap_auth(d, cookies):
                logger.error("[AUTH] Fail")
                d.quit()
                return False
            logger.info("[AUTH] OK")
        except Exception as e:
            logger.error(f"[AUTH] Error: {e}")
    
    try:
        d.execute_cdp_cmd("Network.enable", {})
        d.execute_cdp_cmd("Network.setCacheDisabled", {"cacheDisabled": True})
        install_early_hook(d, keep_last=keep_last)
    except Exception:
        pass

    stopped_due_to_stall = False
    try:
        logger.info(f"[NAV] Đang truy cập: {group_url}")
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
    except Exception as e:
        logger.error(f"[SESSION] Error: {e}")
    finally:
        try:
            d.quit()
        except:
            pass

    return stopped_due_to_stall

def process_url(url, args, global_seen_ids_map):
    global IS_TIMEOUT_TRIGGERED
    
    # --- CẬP NHẬT: GỌI HÀM RESET CHÍNH CHỦ ---
    reset_stop_flag()
    IS_TIMEOUT_TRIGGERED = False
    # ------------------------------------------

    group_url = url.strip()
    page_name = args.page_name.strip()
    account_tag = (args.account_tag or "").strip()
    data_root = Path(args.data_root).resolve()
    keep_last = int(args.keep_last)
    cookies = args.cookies_path.strip()
    timeout_mins = args.timeout

    database_path, out_ndjson, raw_dumps_dir, checkpoint = compute_paths(
        data_root, page_name, account_tag
    )

    if args.date:
        try:
            target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError:
            raise SystemExit("Date format error")
    else:
        target_date = date.today()

    timer = None
    if timeout_mins > 0:
        def on_timeout():
            global IS_TIMEOUT_TRIGGERED
            logger.warning(f"[TIMEOUT] Hết giờ ({timeout_mins}p) cho {group_url}")
            IS_TIMEOUT_TRIGGERED = True
            set_stop_flag() 

        timer = threading.Timer(timeout_mins * 60, on_timeout)
        timer.start()
        logger.info(f"[TIMER] Đã đặt hẹn giờ {timeout_mins} phút")

    logger.info(f"====== CRAWL URL: {group_url} | DATE: {target_date} ======")
    seen_ids: Set[str] = set()
    MAX_STALL_RETRIES = 3
    stall_retry_count = 0
    current_target_date = target_date

    try:
        while True:
            if IS_TIMEOUT_TRIGGERED:
                break

            stopped_due_to_stall = _run_single_session(
                args=args,
                group_url=group_url,
                target_date=current_target_date,
                out_ndjson=out_ndjson,
                keep_last=keep_last,
                seen_ids=seen_ids,
                cookies=cookies,
            )

            if IS_TIMEOUT_TRIGGERED:
                break
            
            if not stopped_due_to_stall:
                logger.info("[SESSION] Xong (không phải stall).")
                break

            stall_retry_count += 1
            if stall_retry_count >= MAX_STALL_RETRIES:
                break

            if pipeline.EARLIEST_CREATED_TS is None:
                break

            new_date = datetime.fromtimestamp(pipeline.EARLIEST_CREATED_TS).date()
            current_target_date = new_date
            
            # Reset flag again before retry loop just in case
            reset_stop_flag()
            
    finally:
        if timer:
            timer.cancel()
        if pipeline.LATEST_CREATED_TS is not None:
            save_checkpoint(checkpoint, pipeline.LATEST_CREATED_TS)
        logger.info(f"[DONE] URL: {group_url}. Unique posts: {len(seen_ids)}")

def main(argv=None):
    import argparse
    ap = argparse.ArgumentParser()
    add_common_args(ap)
    args = ap.parse_args(argv)

    urls = args.group_urls
    if not urls:
        logger.error("No URLs provided")
        return

    logger.info(f"[BATCH] Xử lý {len(urls)} URLs. Timeout: {args.timeout}m")

    for i, url in enumerate(urls, 1):
        logger.info(f"\n{'='*10} PROCESSING URL {i}/{len(urls)} {'='*10}")
        try:
            process_url(url, args, None)
        except Exception as e:
            logger.exception(f"[BATCH] Error URL {url}: {e}")
        
        if i < len(urls):
            time.sleep(5)

if __name__ == "__main__":
    main()