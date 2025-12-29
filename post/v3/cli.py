# post/v3/cli.py
import os
import sys
import signal
import time
import threading
import re
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from datetime import datetime, date
from typing import Set, List, Optional

from logs.loging_config import logger
from util.startdriverproxy import bootstrap_auth

from .config import PROJECT_ROOT, env
from .browser.driver import create_chrome, make_headless
from .browser.hooks import install_early_hook
from .browser.navigation import go_to_date
from .browser.scroll import crawl_scroll_loop, set_stop_flag, reset_stop_flag
from .browser.morelogin_client import close_profile, open_profile
from .browser.driver_morelogin import create_chrome_attach

from .storage.paths import compute_paths
from .storage.checkpoint import save_checkpoint
from . import pipeline

# --- CẤU HÌNH BATCH ---
BATCH_SIZE = 3 

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
    ap.add_argument("--morelogin-profile-id", type=str, required=True,
                    help="Profile ID của MoreLogin")
IS_TIMEOUT_TRIGGERED = False

def _handle_sigterm(sig, frame):
    if not IS_TIMEOUT_TRIGGERED:
        logger.warning("[SIGNAL] Nhận tín hiệu dừng hệ thống.")
    set_stop_flag()

signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)

# --- HÀM TIỆN ÍCH: Tách ID từ URL ---
def extract_id_from_url(url: str) -> str:
    try:
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        if 'id' in query:
            return query['id'][0] # profile.php?id=123 -> 123
        
        path_parts = [p for p in parsed.path.split('/') if p]
        if not path_parts:
            return "unknown"
            
        candidate = path_parts[-1]
        # Xử lý sạch ký tự lạ
        clean_name = re.sub(r'[^a-zA-Z0-9._-]', '', candidate)
        return clean_name if clean_name else "unknown"
    except Exception:
        return "unknown"

# --- HÀM KHỞI TẠO DRIVER ---
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
    
    try:
        d.execute_cdp_cmd("Network.enable", {})
        d.execute_cdp_cmd("Network.setCacheDisabled", {"cacheDisabled": True})
        install_early_hook(d, keep_last=int(args.keep_last))
    except Exception:
        pass
        
    return d
def init_driver_and_login(args):
    """
    Start MoreLogin profile → attach Selenium → setup CDP hooks
    """
    profile_id = args.morelogin_profile_id

    logger.info("[DRIVER] Starting MoreLogin profile %s", profile_id)

    debug_port = None
    try:
        debug_port = open_profile(
            profile_id,
            headless=getattr(args, "headless", False),
            cdp_evasion=True,
        )

        driver = create_chrome_attach(debug_port)

        # ---- CDP setup (optional nhưng nên có) ----
        try:
            driver.execute_cdp_cmd("Network.enable", {})
            driver.execute_cdp_cmd(
                "Network.setCacheDisabled",
                {"cacheDisabled": True}
            )

            if hasattr(args, "keep_last"):
                install_early_hook(
                    driver,
                    keep_last=int(args.keep_last)
                )
        except Exception as e:
            logger.debug("[DRIVER] CDP hook skipped: %s", e)

        logger.info(
            "[DRIVER] Attach MoreLogin OK | profile=%s | port=%s",
            profile_id,
            debug_port,
        )
        return driver

    except Exception as e:
        logger.error("[DRIVER] Failed to init driver: %s", e)

        # nếu đã start profile mà attach fail → close ngay
        if debug_port:
            close_profile(profile_id)

        raise
def _run_single_session(*, driver, args, group_url: str, target_date: date, out_ndjson: Path, keep_last: int, seen_ids: Set[str]):
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
    
    return stopped_due_to_stall

def process_url(url, args, driver):
    global IS_TIMEOUT_TRIGGERED
    
    reset_stop_flag()
    IS_TIMEOUT_TRIGGERED = False

    group_url = url.strip()
    
    # --- LOGIC TẠO TÊN FOLDER RIÊNG BIỆT ---
    base_project = args.page_name.strip()
    unique_id = extract_id_from_url(group_url)
    page_name = f"{base_project}_{unique_id}"
    # ---------------------------------------

    account_tag = (args.account_tag or "").strip()
    data_root = Path(args.data_root).resolve()
    keep_last = int(args.keep_last)
    timeout_mins = args.timeout

    # Path sẽ được tính toán dựa trên page_name mới
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

    logger.info(f"====== CRAWL URL: {group_url} | FOLDER: {page_name} ======")
    seen_ids: Set[str] = set()
    MAX_STALL_RETRIES = 3
    stall_retry_count = 0
    current_target_date = target_date

    try:
        while True:
            if IS_TIMEOUT_TRIGGERED:
                break
            
            if driver is None:
                logger.error("[SESSION] Driver không tồn tại, bỏ qua URL này.")
                break

            stopped_due_to_stall = _run_single_session(
                driver=driver,
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
        logger.info(f"[DONE] URL: {group_url}. Output: {out_ndjson}")

def main(argv=None):
    import argparse
    ap = argparse.ArgumentParser()
    add_common_args(ap)
    args = ap.parse_args(argv)

    urls = args.group_urls
    if not urls:
        logger.error("No URLs provided")
        return

    profile_id = args.morelogin_profile_id
    logger.info(f"[MAIN] Bắt đầu xử lý {len(urls)} URLs. Chế độ: Restart Driver sau mỗi URL.")

    # --- VÒNG LẶP XỬ LÝ TỪNG URL ---
    for i, url in enumerate(urls):
        logger.info(f"\n{'='*10} PROCESSING URL {i+1}/{len(urls)} {'='*10}")
        logger.info(f"Target: {url}")

        current_driver = None
        
        try:
            # 1. KHỞI TẠO DRIVER (Mới cho mỗi URL)
            current_driver = init_driver_and_login(args)
            
            if not current_driver:
                logger.error(f"[MAIN] Không thể khởi tạo driver cho URL {i+1}. Bỏ qua.")
                continue

            # 2. XỬ LÝ URL
            process_url(url, args, current_driver)

        except Exception as e:
            logger.exception(f"[MAIN] Lỗi không mong muốn tại URL {url}: {e}")

        finally:
            # 3. DỌN DẸP (Quan trọng nhất để giải phóng RAM)
            logger.info("[MAIN] Đang đóng driver và profile để giải phóng RAM...")
            
            # 3a. Đóng Selenium Driver
            if current_driver:
                try:
                    current_driver.quit()
                except Exception:
                    pass
            
            # 3b. Gọi API MoreLogin để stop profile (Giải phóng process ngầm)
            try:
                close_profile(profile_id)
            except Exception as e:
                logger.warning(f"Lỗi khi đóng profile MoreLogin: {e}")

            # 3c. Nghỉ một chút trước khi mở lại để tránh lỗi Port
            if i < len(urls) - 1:
                logger.info("Nghỉ 5s trước khi sang URL tiếp theo...")
                time.sleep(5)

    logger.info("[MAIN] HOÀN TẤT TOÀN BỘ DANH SÁCH.")

if __name__ == "__main__":
    main()