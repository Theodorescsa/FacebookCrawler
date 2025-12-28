# post/v3/cli.py
import os
import sys
import signal
import time
import threading
from pathlib import Path
from datetime import datetime, date
from typing import Set, List, Optional

from logs.loging_config import logger
from util.startdriverproxy import bootstrap_auth

from .config import PROJECT_ROOT, env
from .browser.driver import create_chrome, make_headless
from .browser.hooks import install_early_hook
from .browser.navigation import go_to_date
from .browser.scroll import crawl_scroll_loop, set_stop_flag, reset_stop_flag
from .storage.paths import compute_paths
from .storage.checkpoint import save_checkpoint
from . import pipeline

# --- CẤU HÌNH BATCH ---
BATCH_SIZE = 3  # Số lượng URL chạy trước khi reset driver

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

# Hàm khởi tạo Driver và Đăng nhập (Dùng chung cho cả batch)
def init_driver_and_login(args):
    logger.info("[DRIVER] Đang khởi tạo Chrome driver mới cho batch...")
    d = create_chrome(headless=make_headless(args))
    
    cookies = args.cookies_path.strip()
    if cookies and os.path.exists(cookies):
        try:
            if not bootstrap_auth(d, cookies):
                logger.error("[AUTH] Đăng nhập thất bại. Đóng driver.")
                d.quit()
                return None
            logger.info("[AUTH] Đăng nhập OK")
        except Exception as e:
            logger.error(f"[AUTH] Lỗi khi đăng nhập: {e}")
            d.quit()
            return None
    
    # Cài đặt Network
    try:
        d.execute_cdp_cmd("Network.enable", {})
        d.execute_cdp_cmd("Network.setCacheDisabled", {"cacheDisabled": True})
        install_early_hook(d, keep_last=int(args.keep_last))
    except Exception:
        pass
        
    return d

def _run_single_session(
    *,
    driver, # NHẬN DRIVER TỪ BÊN NGOÀI
    args,
    group_url: str,
    target_date: date,
    out_ndjson: Path,
    keep_last: int,
    seen_ids: Set[str],
):
    # Không tạo driver ở đây nữa
    
    stopped_due_to_stall = False
    try:
        logger.info(f"[NAV] Đang truy cập: {group_url}")
        driver.get(group_url)
        time.sleep(1.5)
        
        if "group" not in group_url:
            go_to_date(driver, target_date)

        stopped_due_to_stall = crawl_scroll_loop(
            driver,
            group_url=group_url,
            out_path=out_ndjson,
            seen_ids=seen_ids,
            keep_last=keep_last,
            max_scrolls=args.page_limit or 10000,
        )
    except Exception as e:
        logger.error(f"[SESSION] Error: {e}")
        # Không quit driver ở đây để còn dùng cho link tiếp theo
    
    return stopped_due_to_stall

def process_url(url, args, driver):
    global IS_TIMEOUT_TRIGGERED
    
    reset_stop_flag()
    IS_TIMEOUT_TRIGGERED = False

    group_url = url.strip()
    page_name = args.page_name.strip()
    account_tag = (args.account_tag or "").strip()
    data_root = Path(args.data_root).resolve()
    keep_last = int(args.keep_last)
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
            
            # Nếu driver bị None (do lỗi init trước đó), break luôn
            if driver is None:
                logger.error("[SESSION] Driver không tồn tại, bỏ qua URL này.")
                break

            stopped_due_to_stall = _run_single_session(
                driver=driver, # Truyền driver vào
                args=args,
                group_url=group_url,
                target_date=current_target_date,
                out_ndjson=out_ndjson,
                keep_last=keep_last,
                seen_ids=seen_ids,
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

    logger.info(f"[BATCH] Xử lý {len(urls)} URLs. Timeout: {args.timeout}m. Batch Size: {BATCH_SIZE}")

    current_driver = None

    for i, url in enumerate(urls):
        # Logic quản lý Batch Driver
        # Nếu là URL đầu tiên HOẶC đã chạy đủ số lượng batch -> Reset driver
        if i % BATCH_SIZE == 0:
            if current_driver:
                logger.info(f"[BATCH] Đã chạy xong {BATCH_SIZE} URL, đang restart driver...")
                try:
                    current_driver.quit()
                except:
                    pass
                current_driver = None
            
            # Khởi tạo driver mới và đăng nhập
            current_driver = init_driver_and_login(args)
            if not current_driver:
                logger.error("[BATCH] Không thể khởi tạo driver. Dừng toàn bộ batch hiện tại.")
                # Tùy chọn: continue để thử lại ở batch sau hoặc return để dừng hẳn
                continue 

        logger.info(f"\n{'='*10} PROCESSING URL {i+1}/{len(urls)} {'='*10}")
        try:
            process_url(url, args, current_driver)
        except Exception as e:
            logger.exception(f"[BATCH] Error URL {url}: {e}")
            # Nếu gặp lỗi nghiêm trọng (ví dụ driver crash), có thể cần gán current_driver = None để vòng lặp sau tạo lại
            try:
                # Kiểm tra xem driver còn sống không
                current_driver.title 
            except:
                logger.warning("[BATCH] Driver có vẻ đã chết, sẽ tạo lại ở URL kế tiếp.")
                current_driver = None
        
        if i < len(urls) - 1:
            time.sleep(5)

    # Dọn dẹp cuối cùng
    if current_driver:
        logger.info("[BATCH] Hoàn tất danh sách. Đóng driver.")
        try:
            current_driver.quit()
        except:
            pass

if __name__ == "__main__":
    main()