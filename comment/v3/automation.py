from utils import _button_xpaths_for_texts, _menuitem_xpaths_for_texts, _visible
import json, time, urllib.parse, sys, re
from selenium.webdriver.common.by import By
from collections import deque
from pathlib import Path
from selenium.common.exceptions import (
    TimeoutException, ElementClickInterceptedException, StaleElementReferenceException
)
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import NoSuchElementException
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
from logs.loging_config import logger

# ====== Generic helpers ======
def _wait(d, timeout=10):
    return WebDriverWait(d, timeout)

def scroll_into_view(d, el, block="center"):
    d.execute_script(f"arguments[0].scrollIntoView({{block:'{block}'}});", el)

def safe_click(d, el, sleep_after=0.25):
    """Scroll → try click (handles intercept/stale)."""
    try:
        scroll_into_view(d, el, "center")
        time.sleep(0.15)
        el.click()
        time.sleep(sleep_after)
        return True
    except (ElementClickInterceptedException, StaleElementReferenceException):
        try:
            d.execute_script("arguments[0].click()", el)
            time.sleep(sleep_after)
            return True
        except Exception:
            return False

def wait_first_xpath_anywhere(driver, xpaths, timeout=10):
    end = time.time() + timeout
    last_err = None
    while time.time() < end:
        for xp in xpaths:
            try:
                el = driver.find_element(By.XPATH, xp)
                if _visible(el):
                    return el
            except Exception as e:
                last_err = e
        time.sleep(0.1)
    if last_err:
        raise last_err
    raise TimeoutException("No element matched any xpath in time.")
def open_sort_menu_unified(driver, timeout=10):
    """
    Globally find and click the Sort button ("Phù hợp nhất"/"Most relevant") regardless of surface.
    """
    SORT_TEXTS = [
    "Phù hợp nhất", "Most relevant",  # add more locales if needed
    ]
    xpaths = _button_xpaths_for_texts(SORT_TEXTS)
    btn = wait_first_xpath_anywhere(driver, xpaths, timeout=timeout)
    if not safe_click(driver, btn):
        raise ElementClickInterceptedException("Không click được nút Sort (global).")
    # small pause for menu mount/animation
    time.sleep(1)
    return True

def choose_all_comments_unified(driver, timeout=10):
    """
    After the sort menu opens, pick the 'All comments' option. Search at <body>-level.
    """
    ALL_COMMENTS_TEXTS = [
    "Tất cả bình luận",  # VI
    "All comments",      # EN
    ]

    xpaths = _menuitem_xpaths_for_texts(ALL_COMMENTS_TEXTS)
    opt = wait_first_xpath_anywhere(driver, xpaths, timeout=timeout)
    if not safe_click(driver, opt):
        raise ElementClickInterceptedException("Không click được option 'All comments' (global).")
    time.sleep(1)
    return True
def open_reel_comments_if_present(driver, wait_after=0.6, timeout=6.0):
    """
    Mở panel bình luận cho Reel nếu có.
    Ưu tiên click 'thật' bằng CDP (Input.dispatchMouseEvent) vào đúng tâm nút,
    fallback JS click + click overlay anh em.
    Trả về True nếu đã mở (hoặc đang mở sẵn), False nếu không thấy nút.
    """
    # ===== helpers =====
    def _is_expanded(el):
        try:
            return (el.get_attribute("aria-expanded") or "").lower() == "true"
        except:
            return False

    def _scroll_into_view(el):
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        time.sleep(0.2)

    def _hard_click_center(el):
        # dùng CDP để bắn mouse events vào tọa độ tuyệt đối của phần tử
        rect = driver.execute_script("""
            const r = arguments[0].getBoundingClientRect();
            return {x: r.left + r.width/2, y: r.top + r.height/2,
                    left:r.left, top:r.top, width:r.width, height:r.height};
        """, el)
        if not rect: 
            return False
        # bring to front (đề phòng tab chưa active)
        try:
            driver.execute_cdp_cmd("Page.bringToFront", {})
        except Exception:
            pass

        # chuyển tọa độ viewport → absolute (Chrome DevTools dùng coords viewport)
        x = rect["x"]
        y = rect["y"]

        try:
            driver.execute_cdp_cmd("Input.dispatchMouseEvent", {
                "type": "mouseMoved", "x": x, "y": y, "buttons": 1
            })
            driver.execute_cdp_cmd("Input.dispatchMouseEvent", {
                "type": "mousePressed", "x": x, "y": y, "button": "left", "clickCount": 1
            })
            driver.execute_cdp_cmd("Input.dispatchMouseEvent", {
                "type": "mouseReleased", "x": x, "y": y, "button": "left", "clickCount": 1
            })
            return True
        except Exception:
            return False

    def _js_click(el):
        try:
            driver.execute_script("arguments[0].click();", el)
            return True
        except Exception:
            return False

    def _click_overlay_sibling(el):
        try:
            ov = el.find_element(By.XPATH, "following-sibling::div[@role='none'][1]")
            driver.execute_script("arguments[0].click();", ov)
            return True
        except NoSuchElementException:
            return False
        except Exception:
            return False

    def _opened():
        # mở thành công khi:
        # 1) nút có aria-expanded=true, hoặc
        # 2) xuất hiện container có aria-label chứa 'Bình luận' / 'Comments', hoặc
        # 3) buffer GraphQL tăng
        return True

    # baseline GraphQL buffer để theo dõi có phát request không
    try:
        baseline = driver.execute_script("return (window.__gqlReqs||[]).length") or 0
    except Exception:
        baseline = 0

    # ===== chọn nút ứng viên =====
    XPATHS = [
        # Nút Reel “Bình luận” theo aria-label (VI + EN)
        "//div[@role='button' and @aria-label='Bình luận']",
        "//div[@role='button' and (@aria-label='Comments' or contains(@aria-label,'Comment'))]",

        # Nút có icon bong bóng chat (path bạn gửi) → leo ancestor button
        "//svg[.//path[starts-with(@d,'M12 .5C18.351')]]/ancestor::*[@role='button'][1]",

        # Fallback: nút hiển thị số bình luận kèm icon → leo ancestor button
        "//span[normalize-space(text()) and number(.)=number(.)]/ancestor::*[@role='button'][1]",
    ]

    cand = None
    for xp in XPATHS:
        els = driver.find_elements(By.XPATH, xp)
        if els:
            cand = els[0]; break
    if not cand:
        return False  # không thấy nút → coi như không phải Reel hoặc layout khác

    # Nếu đang expanded thì coi như OK
    if _is_expanded(cand):
        return True

    # Thử theo thứ tự: scroll → hard click (CDP) → JS click → click overlay
    _scroll_into_view(cand)
    clicked = _hard_click_center(cand)
    if not clicked:
        clicked = _js_click(cand)
    if not clicked:
        clicked = _click_overlay_sibling(cand)

    # chờ load một nhịp
    time.sleep(wait_after)

    # Kiểm tra đã mở/đã bắn request chưa
    try:
        now = driver.execute_script("return (window.__gqlReqs||[]).length") or 0
    except Exception:
        now = baseline
    if _is_expanded(cand) or now > baseline:
        return True

    # Thử lần 2 (một số layout cần 2 click mới mở panel)
    _scroll_into_view(cand)
    _hard_click_center(cand)
    time.sleep(wait_after)

    try:
        now2 = driver.execute_script("return (window.__gqlReqs||[]).length") or 0
    except Exception:
        now2 = baseline

    return _is_expanded(cand) or now2 > baseline


def set_sort_to_all_comments_unified(driver, max_retry=2):
    """
    Public API — robust for Post/Video/Reel:
    1) Find global Sort button (any surface), click.
    2) Select 'All comments' from the body-mounted menu.
    """
    logger.info("[SORT] Set to 'All comments' (unified)…")
    last_err = None
    for _ in range(max_retry):
        try:
            open_sort_menu_unified(driver, timeout=10)
            choose_all_comments_unified(driver, timeout=10)
            return True
        except Exception as e:
            last_err = e
            time.sleep(0.6)
    if last_err:
        raise last_err
    return False

def click_view_more_if_any(driver, max_clicks=1):
    """
    Click các nút load thêm bình luận / phản hồi:
      - Xem thêm bình luận
      - Xem thêm phản hồi
      - Xem tất cả 12 phản hồi
      - Xem phản hồi khác
      - View more comments / replies
      - See more replies / See all replies
    """
    xps = [
        # VI: div role=button chứa text
        "//div[@role='button'][contains(.,'Xem thêm bình luận') "
        " or contains(.,'Xem thêm phản hồi') "
        " or contains(.,'Xem tất cả') "
        " or contains(.,'Xem phản hồi khác')]",

        # VI: span text → leo ancestor div[@role='button']
        "//span[contains(.,'Xem thêm bình luận') "
        " or contains(.,'Xem thêm phản hồi') "
        " or contains(.,'Xem tất cả') "
        " or contains(.,'Xem phản hồi khác')]/ancestor::div[@role='button']",

        # EN: div role=button
        "//div[@role='button'][contains(.,'View more comments') "
        " or contains(.,'View more replies') "
        " or contains(.,'See more replies') "
        " or contains(.,'See all replies')]",

        # EN: span → ancestor div role=button
        "//span[contains(.,'View more comments') "
        " or contains(.,'View more replies') "
        " or contains(.,'See more replies') "
        " or contains(.,'See all replies')]/ancestor::div[@role='button']",
    ]

    clicks = 0
    for xp in xps:
        # nếu đủ max_clicks thì dừng luôn
        if clicks >= max_clicks:
            break
        try:
            buttons = driver.find_elements(By.XPATH, xp)
        except Exception:
            continue

        for b in buttons:
            if clicks >= max_clicks:
                break
            try:
                driver.execute_script(
                    "arguments[0].scrollIntoView({block:'center'});", b
                )
                time.sleep(0.15)
                b.click()
                clicks += 1
                time.sleep(0.35)
            except Exception:
                # fallback JS click
                try:
                    driver.execute_script("arguments[0].click();", b)
                    clicks += 1
                    time.sleep(0.35)
                except Exception:
                    pass

    return clicks


def scroll_to_last_comment(driver):
    js = r"""
    (function(){
      const cands = Array.from(document.querySelectorAll("div[role='article'][aria-label]"));
      let nodes = cands.filter(n => /Bình luận/i.test(n.getAttribute('aria-label')||""));
      if (nodes.length === 0) nodes = cands.filter(n => /(Comment|Comments)/i.test(n.getAttribute('aria-label')||""));
      if (nodes.length === 0) return false;
      nodes[nodes.length - 1].scrollIntoView({behavior: 'instant', block: 'center'});
      window.scrollBy(0, Math.floor(window.innerHeight*0.1));
      return true;
    })();
    """
    return bool(driver.execute_script(js))