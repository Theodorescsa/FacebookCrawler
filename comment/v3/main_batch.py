# comment/v3/main_batch.py

import sys
import re
import urllib.parse
from pathlib import Path
import time
import pandas as pd
import argparse

# ----- Imports -----
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Import các module MoreLogin gốc của bạn
try:
    # Thử import theo cấu trúc package
    from comment.v3.morelogin_client import open_profile, close_profile
    from comment.v3.driver_morelogin import create_chrome_attach
except ImportError:
    # Fallback nếu chạy tại chỗ
    from morelogin_client import open_profile, close_profile
    from driver_morelogin import create_chrome_attach

from crawl_comments import crawl_comments_for_post
from logs.loging_config import logger
from hook import install_early_hook 

def safe_filename_from_url(url: str, max_len: int = 100) -> str:
    try:
        parsed = urllib.parse.urlparse(url)
        base = parsed._replace(query="", fragment="").geturl()
        name = re.sub(r"[^A-Za-z0-9]+", "_", base).strip("_")
        if len(name) > max_len:
            name = name[-max_len:]
        return name or "post"
    except Exception:
        return "unknown_post"

def parse_args():
    parser = argparse.ArgumentParser(description="Batch crawl comments using MoreLogin.")
    parser.add_argument("--page-name", dest="page_name", default="thoibaode", help="Tên fanpage (dùng làm tên folder output).")
    parser.add_argument("--input-excel", dest="input_excel", required=True, help="Đường dẫn file Excel chứa cột 'link'.")
    parser.add_argument("--profile-id", dest="profile_id", required=True, help="ID của Profile trong MoreLogin.")
    parser.add_argument("--data-root", type=str, default=str(PROJECT_ROOT / "database"))
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()

    EXCEL_PATH = args.input_excel
    PROFILE_ID = args.profile_id
    FANPAGE_NAME = args.page_name
    DATA_ROOT = Path(args.data_root)
    BASE_DIR = DATA_ROOT / "comment" / "page"
    OUT_DIR = (BASE_DIR / FANPAGE_NAME).resolve()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("[BATCH] START | EXCEL=%s | PROFILE_ID=%s | OUT_DIR=%s", EXCEL_PATH, PROFILE_ID, OUT_DIR)

    # 1. Đọc Excel
    try:
        df = pd.read_excel(EXCEL_PATH)
    except Exception as e:
        logger.exception("[ERROR] Không đọc được file Excel: %s", e)
        sys.exit(1)

    if "link" not in df.columns:
        logger.error("[ERROR] Excel thiếu cột 'link'")
        sys.exit(1)

    links = df["link"].dropna().astype(str).tolist()
    logger.info("[BATCH] Tổng link cần crawl: %s", len(links))

    driver = None
    
    # --- BẮT ĐẦU QUY TRÌNH DRIVER 1 LẦN ---
    try:
        # 2. Mở Profile MoreLogin -> Lấy Debug Port
        logger.info("[INIT] Opening MoreLogin profile %s...", PROFILE_ID)
        try:
            # headless=False để thấy trình duyệt, cdp_evasion=True để chống detect
            debug_port = open_profile(PROFILE_ID, headless=False, cdp_evasion=True)
            logger.info("[INIT] Profile started on port: %s", debug_port)
        except Exception as e:
            logger.error("[INIT] Failed to open profile: %s", e)
            sys.exit(1)

        # 3. Attach Selenium vào Port đó
        logger.info("[INIT] Attaching Selenium to port %s...", debug_port)
        driver = create_chrome_attach(debug_port)
        
        if not driver:
            logger.error("[INIT] Failed to attach driver. Exiting.")
            sys.exit(1)

        logger.info("[INIT] Driver attached successfully.")

        # 4. Cấu hình CDP & Hooks (Làm 1 lần cho session)
        try:
            driver.execute_cdp_cmd("Network.enable", {})
            driver.execute_cdp_cmd("Network.setCacheDisabled", {"cacheDisabled": True})
            install_early_hook(driver) # Inject script bắt GQL
            logger.info("[INIT] Hooks & Network CDP enabled.")
        except Exception as e:
            logger.warning("[INIT] Warning setting up CDP/Hooks: %s", e)

        # 5. Duyệt từng link
        for idx, url in enumerate(links, start=1):
            fname = safe_filename_from_url(url) + ".ndjson"
            out_path = OUT_DIR / fname

            logger.info("---------------------------------------------------------------")
            logger.info("[BATCH] (%s/%s) Processing: %s", idx, len(links), url)
            
            try:
                # Gọi crawl - Truyền driver vào
                rows = crawl_comments_for_post(
                    driver=driver,
                    page_url=url,
                    # Không cần profile_id ở đây nữa vì driver đã có rồi
                    max_rounds=200,      
                    sleep_between_rounds=1.5,
                    out_path=str(out_path),
                )

                if not rows:
                    logger.warning("[BATCH] ⚠️ Empty result for: %s", url)
                else:
                    logger.info("[BATCH] ✅ Success. Saved %s items to %s", len(rows), fname)

            except KeyboardInterrupt:
                logger.info("[BATCH] User stopped process.")
                break 
            except Exception as e:
                logger.error("[BATCH] ❌ Error crawling link %s: %s", url, e)
                continue
            
            time.sleep(2) 

    finally:
        # 6. Dọn dẹp cuối cùng
        if driver:
            logger.info("[CLEANUP] Quitting Selenium driver...")
            try:
                driver.quit()
            except Exception:
                pass
        
        logger.info("[CLEANUP] Closing MoreLogin profile %s...", PROFILE_ID)
        close_profile(PROFILE_ID)
        logger.info("[BATCH] FINISHED.")