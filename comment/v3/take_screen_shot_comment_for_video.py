# evidence_capture.py
import re, sys
import time
import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

from mss import mss
from PIL import Image
import os

from hook import install_early_hook
from automation import (
    open_reel_comments_if_present,
    set_sort_to_all_comments_unified,
    click_view_more_if_any,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
from util.export_utils.export_fb_session import start_driver

# ===================== CONFIG =====================
EXCEL_FILE = r"E:\NCS\fb-selenium\util\output.xlsx"
SHEET_NAME = "videos_only"

COL_LINK = "facebook_link"
COL_AUTHOR = "author_comment"
COL_COMMENT = "comment_content"
COL_UID = "author_id"  

OUT_DIR = Path("evidence_screenshots")
OUT_DIR.mkdir(exist_ok=True)

NDJSON_FILE = OUT_DIR / "results.ndjson"

HEADLESS = False
MAX_ROUNDS = 200
SLEEP_BETWEEN_ROUNDS = 1.0

SCROLL_STEP = 360

SCROLL_PAUSE = 0.25
WAIT_AFTER_OPEN_COMMENTS = 1.0
def clean_for_match(s: str) -> str:
    """Loại bỏ dấu câu và đưa về chữ thường để so sánh chính xác hơn"""
    if not s: return ""
    # Chuyển về chữ thường
    s = s.lower()
    # Loại bỏ các ký tự không phải chữ và số (bao gồm cả dấu câu)
    s = re.sub(r"[^\w\s\u00C0-\u1EF9]", "", s) 
    # Chuẩn hóa khoảng trắng
    s = " ".join(s.split())
    return s
def append_ndjson_f(f, obj: Dict):
    f.write(json.dumps(obj, ensure_ascii=False) + "\n")
    f.flush()
    os.fsync(f.fileno())  # ✅ đảm bảo ghi xuống đĩa ngay

def bring_browser_to_front(driver, delay=0.15):
    try:
        driver.switch_to.window(driver.current_window_handle)
    except Exception:
        pass
    try:
        driver.execute_script("window.focus();")
    except Exception:
        pass
    time.sleep(delay)

# ----------------- screenshot helpers -----------------
def screenshot_entire_screen(out_path: Path, monitor_index=1, retries=3):
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

                # ✅ giảm compress để khỏi “đơ”
                pil.save(str(out_path), format="PNG", compress_level=1)
                return
        except Exception as e:
            last_err = e
            time.sleep(0.25)

    raise last_err


def screenshot_full_viewport(driver, out_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    driver.save_screenshot(str(out_path))

def scroll_window_to_top(driver):
    driver.execute_script("""
        window.scrollTo(0, 0);
        document.documentElement.scrollTop = 0;
        document.body.scrollTop = 0;
    """)
    time.sleep(0.15)

# ===================== UTILS =====================
def now_iso():
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

def append_ndjson(path: Path, obj: Dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")
        f.flush()

def norm_text(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    s = re.sub(r"\s+", " ", s)
    return s

def norm_uid(x) -> str:
    """
    Excel hay đọc uid thành float (vd 123.0). Chuẩn hoá về string sạch.
    """
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

def safe_filename(s: str, max_len: int = 120) -> str:
    s = norm_text(s)
    s = re.sub(r"[\\/:*?\"<>|]+", "_", s)
    s = re.sub(r"\s+", " ", s).strip()
    if len(s) > max_len:
        s = s[:max_len].rstrip()
    return s

def sha1_short(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:10]

def wait_doc_ready(driver, timeout=15):
    WebDriverWait(driver, timeout).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
    )

# ===================== MODAL KILLER =====================
def close_annoying_modals(driver, timeout=1.2):
    """
    Đóng các modal kiểu:
    - Thống kê / Số liệu thống kê / Insights
    - Reactions / Cảm xúc
    - Dialog bất kỳ có nút Close/Đóng
    """
    try:
        # 1) ESC trước (nhanh + ít hại)
        try:
            from selenium.webdriver.common.keys import Keys
            driver.switch_to.active_element.send_keys(Keys.ESCAPE)
        except Exception:
            pass

        end = time.time() + timeout
        while time.time() < end:
            dialogs = driver.find_elements(By.CSS_SELECTOR, "div[role='dialog']")
            if not dialogs:
                break

            closed_any = False
            for d in dialogs[:4]:
                txt = ""
                try:
                    txt = (d.text or "")[:300].lower()
                except Exception:
                    pass

                # Ưu tiên kill mấy modal thống kê/insights
                must_close = any(k in txt for k in [
                    "thống kê", "số liệu", "insights", "analytics", "cảm xúc", "reactions"
                ])

                # Tìm nút close/đóng
                btns = []
                try:
                    btns = d.find_elements(By.CSS_SELECTOR, "[aria-label='Close'],[aria-label='Đóng'],div[aria-label='Close'],div[aria-label='Đóng']")
                except Exception:
                    btns = []

                if btns:
                    try:
                        btns[0].click()
                        closed_any = True
                        time.sleep(0.15)
                        continue
                    except Exception:
                        pass

                # fallback: click X dạng role=button
                if must_close:
                    try:
                        xbtn = d.find_elements(By.CSS_SELECTOR, "div[role='button'][aria-label],span[role='button'][aria-label]")
                        if xbtn:
                            xbtn[0].click()
                            closed_any = True
                            time.sleep(0.15)
                            continue
                    except Exception:
                        pass

            if not closed_any:
                break

    except Exception:
        pass

# ===================== CORE FIXES =====================
def find_comment_scroll_container(driver, timeout=10):
    article_sel = (
        "div[role='article'][aria-label*='Bình luận'],"
        "div[role='article'][aria-label*='Comment by'],"
        "div[role='article'][aria-label^='Comment by']"
    )

    WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, article_sel))
    )
    anchor = driver.find_element(By.CSS_SELECTOR, article_sel)

    js = """
    const art = arguments[0];

    function canScroll(el){
      if (!el) return false;
      const sh = el.scrollHeight || 0;
      const ch = el.clientHeight || 0;
      if (sh <= ch + 20) return false;

      const before = el.scrollTop;
      el.scrollTop = before + 120;
      const after = el.scrollTop;
      el.scrollTop = before;
      return after !== before;
    }

    let el = art.parentElement;
    let steps = 0;

    while (el && steps < 120){
      if (canScroll(el)) return el;
      el = el.parentElement;
      steps++;
    }
    return null;
    """
    container = driver.execute_script(js, anchor)
    if not container:
        raise RuntimeError("Cannot find a scrollable comment container (tested scrollTop).")
    return container

def scroll_comment_to_center(driver, container, element, max_try=4):
    js = """
    const container = arguments[0];
    const el = arguments[1];

    function isScrollable(x){
      if (!x) return false;
      const sh = x.scrollHeight || 0;
      const ch = x.clientHeight || 0;
      if (sh <= ch + 20) return false;
      const before = x.scrollTop;
      x.scrollTop = before + 120;
      const after = x.scrollTop;
      x.scrollTop = before;
      return after !== before;
    }

    let sp = el.parentElement;
    let guard = 0;
    while (sp && sp !== container && guard < 80){
      if (isScrollable(sp)) break;
      sp = sp.parentElement;
      guard++;
    }
    if (!sp || sp === container) {
      sp = container;
    }

    const spRect = sp.getBoundingClientRect();
    const elRect = el.getBoundingClientRect();
    const cur = sp.scrollTop;
    const offsetTop = (elRect.top - spRect.top) + cur;
    const target = offsetTop - (sp.clientHeight / 2) + (elRect.height / 2);

    sp.scrollTop = Math.max(0, target);

    try{
      const evt = new WheelEvent('wheel', {deltaY: 220, bubbles: true, cancelable: true});
      sp.dispatchEvent(evt);
    } catch(e){}

    const spRect2 = sp.getBoundingClientRect();
    const elRect2 = el.getBoundingClientRect();
    const margin = 16;

    const fullyVisible =
      elRect2.top >= spRect2.top + margin &&
      elRect2.bottom <= spRect2.bottom - margin;

    return { ok: fullyVisible, spTag: sp.tagName, spCls: sp.className };
    """
    last = None
    for _ in range(max_try):
        last = driver.execute_script(js, container, element)
        time.sleep(0.15)
        if last and last.get("ok"):
            return True
    return False

def get_visible_comment_nodes(driver, container) -> List[Dict]:
    js = """
    const c = arguments[0];

    function txt(el){
      return (el && el.innerText) ? el.innerText.trim() : "";
    }

    const nodes = Array.from(c.querySelectorAll("div[role='article']")).slice(0, 250);

    const items = [];
    for (const n of nodes){
      let author = "";
      const a = n.querySelector("a[role='link'], a[href*='facebook.com']");
      if (a) author = txt(a);

      let content = "";
      const candidates = n.querySelectorAll("div[dir='auto'], span[dir='auto']");
      if (candidates && candidates.length){
        let best = "";
        for (const x of candidates){
          const t = txt(x);
          if (t.length > best.length) best = t;
        }
        content = best;
      } else {
        content = txt(n);
      }

      if (!content) continue;

      items.push({ author, content, el: n });
    }
    return items;
    """
    raw = driver.execute_script(js, container)
    items = []
    for it in raw:
        items.append(
            {
                "author": norm_text(it.get("author", "")),
                "content": norm_text(it.get("content", "")),
                "element": it.get("el"),
            }
        )
    return items

def match_comment(items: List[Dict], target_author: str, target_content: str) -> Optional[Dict]:
    target_author_clean = clean_for_match(target_author)
    target_content_clean = clean_for_match(target_content)

    best = None
    max_overlap = 0

    for it in items:
        found_author_clean = clean_for_match(it["author"])
        found_content_clean = clean_for_match(it["content"])

        # 1. Kiểm tra tác giả (nếu có target_author)
        author_match = True
        if target_author_clean:
            # Facebook có thể hiển thị "Trần Long" hoặc "Tran Long" hoặc chỉ một phần
            author_match = (target_author_clean in found_author_clean) or (found_author_clean in target_author_clean)

        # 2. Kiểm tra nội dung (Trọng tâm)
        # Kiểm tra xem nội dung tìm thấy có chứa nội dung mục tiêu không hoặc ngược lại
        content_match = False
        if target_content_clean in found_content_clean or found_content_clean in target_content_clean:
            content_match = True
        
        # 3. Nếu không khớp hoàn toàn, thử khớp theo tỷ lệ từ (phòng trường hợp text bị cắt bớt)
        if not content_match:
            words_target = set(target_content_clean.split())
            words_found = set(found_content_clean.split())
            if words_target:
                overlap = len(words_target.intersection(words_found)) / len(words_target)
                if overlap > 0.7: # Khớp trên 70% số từ là chấp nhận
                    content_match = True

        if author_match and content_match:
            return it

    return None
def is_element_in_middle_band(driver, scroll_parent, el, top_ratio=0.25, bot_ratio=0.75):
    js = """
    const sp = arguments[0];
    const el = arguments[1];
    const r1 = sp.getBoundingClientRect();
    const r2 = el.getBoundingClientRect();
    const h = r1.height;

    const topBand = r1.top + h * arguments[2];
    const botBand = r1.top + h * arguments[3];

    const mid = (r2.top + r2.bottom) / 2;
    const visible = (r2.bottom > r1.top + 10) && (r2.top < r1.bottom - 10);

    return { visible, midOK: (mid >= topBand && mid <= botBand) };
    """
    return driver.execute_script(js, scroll_parent, el, float(top_ratio), float(bot_ratio))

def center_and_screenshot(driver, container, element, out_path: Path, tries=4):
    for _ in range(tries):
        scroll_comment_to_center(driver, container, element, max_try=3)
        time.sleep(0.08)

        stat = is_element_in_middle_band(driver, container, element, 0.28, 0.72)
        if stat.get("visible") and stat.get("midOK"):
            bring_browser_to_front(driver, delay=0.05)
            screenshot_entire_screen(out_path, monitor_index=1)
            return True

        time.sleep(0.08)

    bring_browser_to_front(driver, delay=0.05)
    screenshot_entire_screen(out_path, monitor_index=1)
    return False

# ===================== MAIN FLOW =====================
def run_for_one_post(
    driver,
    url: str,
    target_author: str,
    target_comment: str,
    uid: str,
    seq_no: int,
) -> Dict:
    """
    Return record dict (ok/fail + image_name + error)
    """
    rec = {
        "ts": now_iso(),
        "uid": uid,
        "seq_no": seq_no,
        "url": url,
        "author": target_author,
        "comment": target_comment,
        "status": "fail",
        "image_name": None,
        "image_path": None,
        "error": None,
    }

    print(f"\n=== [uid={uid} #{seq_no}] Open: {url}")

    try:
        driver.get(url)
        wait_doc_ready(driver, timeout=20)

        # # ✅ kill cái modal thống kê/insights tự bật
        # close_annoying_modals(driver, timeout=1.5)
        # try:
        #     close_fb_login_popup(driver)
        # except Exception:
        #     pass

        try:
            open_reel_comments_if_present(driver)
            time.sleep(WAIT_AFTER_OPEN_COMMENTS)
        except Exception as e:
            print("[WARN] open_reel_comments_if_present failed:", e)

        # close_annoying_modals(driver, timeout=1.0)

        try:
            set_sort_to_all_comments_unified(driver)
            time.sleep(0.5)
        except Exception as e:
            print("[WARN] set_sort_to_all_comments_unified failed:", e)

        try:
            click_view_more_if_any(driver, max_clicks=2)
            time.sleep(0.5)
        except Exception as e:
            print("[WARN] click_view_more_if_any init failed:", e)

        # close_annoying_modals(driver, timeout=1.0)

        container = find_comment_scroll_container(driver, timeout=12)

        for round_i in range(MAX_ROUNDS):
            close_annoying_modals(driver, timeout=0.6)

            items = get_visible_comment_nodes(driver, container)
            found = match_comment(items, target_author, target_comment)

            if found:
                print(f"[OK] Found at round {round_i}")

                try:
                    ok_center = scroll_comment_to_center(driver, container, found["element"], max_try=5)
                    if not ok_center:
                        print("[WARN] Could not perfectly center comment, still screenshot anyway.")
                    time.sleep(0.35)
                except Exception as e:
                    print("[WARN] scroll element into container failed:", e)

                # ✅ output theo uid folder
                uid_dir = OUT_DIR / f"uid_{uid}"
                uid_dir.mkdir(parents=True, exist_ok=True)

                ts_str = datetime.now().strftime("%Y%m%d_%H%M%S")
                image_name = f"uid_{uid}_{seq_no}_{ts_str}.png"
                out = uid_dir / image_name

                # ✅ giữ nguyên cái bạn yêu cầu
                scroll_window_to_top(driver)
                time.sleep(0.5)

                ok_mid = center_and_screenshot(driver, container, found["element"], out, tries=5)
                print("[SAVE]", out, "midOK=", ok_mid)

                rec["status"] = "ok"
                rec["image_name"] = image_name
                rec["image_path"] = str(out.as_posix())
                rec["error"] = None
                return rec

            try:
                click_view_more_if_any(driver, max_clicks=1)
            except Exception:
                pass

            time.sleep(SCROLL_PAUSE)

        rec["error"] = "not_found_after_max_rounds"
        return rec

    except (TimeoutException, WebDriverException, Exception) as e:
        rec["error"] = f"{type(e).__name__}: {e}"
        return rec

def main():
    df = pd.read_excel(EXCEL_FILE, sheet_name=SHEET_NAME)

    for col in (COL_LINK, COL_COMMENT, COL_UID):
        if col not in df.columns:
            raise ValueError(f"Missing column '{col}' in sheet '{SHEET_NAME}'")

    driver = start_driver("Profile 40")
    install_early_hook(driver)

    ok = 0
    fail = 0
    uid_counter: Dict[str, int] = {}

    # ✅ mở NDJSON 1 lần
    NDJSON_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(NDJSON_FILE, "a", encoding="utf-8", buffering=1) as nd_f:
        try:
            for _, row in df.iterrows():
                url = norm_text(row.get(COL_LINK))
                comment = norm_text(row.get(COL_COMMENT))
                author = norm_text(row.get(COL_AUTHOR, ""))
                uid = norm_uid(row.get(COL_UID))

                if not url or url.lower() == "nan":
                    continue
                if not comment:
                    continue

                uid_counter[uid] = uid_counter.get(uid, 0) + 1
                seq_no = uid_counter[uid]

                rec = run_for_one_post(
                    driver,
                    url=url,
                    target_author=author,
                    target_comment=comment,
                    uid=uid,
                    seq_no=seq_no,
                )

                # ✅ ghi realtime
                append_ndjson_f(nd_f, rec)

                if rec["status"] == "ok":
                    ok += 1
                else:
                    fail += 1

                time.sleep(SLEEP_BETWEEN_ROUNDS)

        finally:
            print(f"\nDONE. OK={ok} FAIL={fail}")
            print("[NDJSON]", NDJSON_FILE)
            try:
                driver.quit()
            except Exception:
                pass


if __name__ == "__main__":
    main()
