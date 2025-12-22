# simple_screenshot_capture.py
import re
import sys
import time
import json
from datetime import datetime
from pathlib import Path
from typing import Dict

import pandas as pd
from selenium.common.exceptions import TimeoutException, WebDriverException

from mss import mss
from PIL import Image
import os

from hook import install_early_hook

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
from util.export_utils.export_fb_session import start_driver

# ===================== CONFIG =====================
EXCEL_FILE = r"E:\NCS\fb-selenium\util\output.xlsx"
SHEET_NAME = "original_data"

COL_LINK = "facebook_link"
COL_UID = "author_id"

OUT_DIR = Path("evidence_screenshots")
OUT_DIR.mkdir(exist_ok=True)

NDJSON_FILE = OUT_DIR / "results.ndjson"

HEADLESS = False
WAIT_AFTER_LOAD = 5.0  # Đợi trang load xong
SLEEP_BETWEEN_POSTS = 1.5

# ===================== UTILS =====================
def now_iso():
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

def append_ndjson_f(f, obj: Dict):
    f.write(json.dumps(obj, ensure_ascii=False) + "\n")
    f.flush()
    os.fsync(f.fileno())

def norm_text(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    s = re.sub(r"\s+", " ", s)
    return s

def norm_uid(x) -> str:
    """Chuẩn hoá uid từ Excel"""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return "unknown"
    s = str(x).strip()
    if re.fullmatch(r"\d+\.0", s):
        s = s[:-2]
    if "e+" in s.lower():
        try:
            s = str(int(float(s)))
        except Exception:
            pass
    return s

def wait_doc_ready(driver, timeout=15):
    """Đợi trang load xong"""
    end_time = time.time() + timeout
    while time.time() < end_time:
        try:
            state = driver.execute_script("return document.readyState")
            if state == "complete":
                return True
        except Exception:
            pass
        time.sleep(0.3)
    return False

def bring_browser_to_front(driver, delay=0.15):
    """Đưa browser lên foreground"""
    try:
        driver.switch_to.window(driver.current_window_handle)
    except Exception:
        pass
    try:
        driver.execute_script("window.focus();")
    except Exception:
        pass
    time.sleep(delay)

def screenshot_entire_screen(out_path: Path, monitor_index=1, retries=3):
    """Chụp toàn bộ màn hình"""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    
    last_err = None
    for i in range(retries):
        try:
            with mss() as sct:
                monitors = sct.monitors
                if monitor_index < 1 or monitor_index >= len(monitors):
                    monitor_index = 1
                monitor = monitors[monitor_index]
                img = sct.grab(monitor)
                
                pil = Image.frombytes("RGB", img.size, img.rgb)
                pil.save(str(out_path), format="PNG", compress_level=1)
                return True
        except Exception as e:
            last_err = e
            time.sleep(0.25)
    
    print(f"[ERROR] Screenshot failed: {last_err}")
    return False

def scroll_window_to_top(driver):
    """Scroll về đầu trang"""
    try:
        driver.execute_script("""
            window.scrollTo(0, 0);
            document.documentElement.scrollTop = 0;
            document.body.scrollTop = 0;
        """)
        time.sleep(0.2)
    except Exception as e:
        print(f"[WARN] Scroll to top failed: {e}")

# ===================== MAIN FLOW =====================
def capture_comment_screenshot(
    driver,
    url: str,
    uid: str,
    seq_no: int,
) -> Dict:
    """
    Truy cập link comment và chụp màn hình
    Return record dict (ok/fail + image_name + error)
    """
    rec = {
        "ts": now_iso(),
        "uid": uid,
        "seq_no": seq_no,
        "url": url,
        "status": "fail",
        "image_name": None,
        "image_path": None,
        "error": None,
    }
    
    print(f"\n=== [uid={uid} #{seq_no}] Open: {url}")
    
    try:
        # Truy cập link
        driver.get(url)
        
        # Đợi trang load
        wait_doc_ready(driver, timeout=20)
        time.sleep(WAIT_AFTER_LOAD)
        
        # Đưa browser lên foreground
        bring_browser_to_front(driver, delay=0.2)
        
        # Tạo folder theo uid
        uid_dir = OUT_DIR / f"uid_{uid}"
        uid_dir.mkdir(parents=True, exist_ok=True)
        
        # Tên file ảnh
        ts_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        image_name = f"uid_{uid}_{seq_no}_{ts_str}.png"
        out_path = uid_dir / image_name
        
        # Chụp màn hình
        success = screenshot_entire_screen(out_path, monitor_index=1)
        
        if success:
            print(f"[OK] Screenshot saved: {out_path}")
            rec["status"] = "ok"
            rec["image_name"] = image_name
            rec["image_path"] = str(out_path.as_posix())
        else:
            rec["error"] = "screenshot_failed"
            print("[FAIL] Could not capture screenshot")
        
        return rec
        
    except (TimeoutException, WebDriverException) as e:
        rec["error"] = f"{type(e).__name__}: {e}"
        print(f"[ERROR] {rec['error']}")
        return rec
    except Exception as e:
        rec["error"] = f"Exception: {e}"
        print(f"[ERROR] {rec['error']}")
        return rec

def main():
    # Đọc Excel
    df = pd.read_excel(EXCEL_FILE, sheet_name=SHEET_NAME)
    
    # Kiểm tra cột
    for col in (COL_LINK, COL_UID):
        if col not in df.columns:
            raise ValueError(f"Missing column '{col}' in sheet '{SHEET_NAME}'")
    
    # Khởi động driver
    driver = start_driver("Profile 40")
    install_early_hook(driver)
    
    ok = 0
    fail = 0
    uid_counter: Dict[str, int] = {}
    
    # Mở NDJSON file để ghi kết quả
    NDJSON_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(NDJSON_FILE, "a", encoding="utf-8", buffering=1) as nd_f:
        try:
            for idx, row in df.iterrows():
                url = norm_text(row.get(COL_LINK))
                uid = norm_uid(row.get(COL_UID))
                
                # Bỏ qua các dòng không hợp lệ
                if not url or url.lower() == "nan":
                    print(f"[SKIP] Row {idx}: Invalid URL")
                    continue
                
                # Đếm số thứ tự cho mỗi uid
                uid_counter[uid] = uid_counter.get(uid, 0) + 1
                seq_no = uid_counter[uid]
                
                # Chụp màn hình
                rec = capture_comment_screenshot(
                    driver,
                    url=url,
                    uid=uid,
                    seq_no=seq_no,
                )
                
                # Ghi kết quả realtime
                append_ndjson_f(nd_f, rec)
                
                if rec["status"] == "ok":
                    ok += 1
                else:
                    fail += 1
                
                # Nghỉ giữa các post
                time.sleep(SLEEP_BETWEEN_POSTS)
                
        finally:
            print(f"\n{'='*50}")
            print(f"DONE!")
            print(f"Success: {ok}")
            print(f"Failed: {fail}")
            print(f"Results saved to: {NDJSON_FILE}")
            print(f"{'='*50}")
            try:
                driver.quit()
            except Exception:
                pass


if __name__ == "__main__":
    main()