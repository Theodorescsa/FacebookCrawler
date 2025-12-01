import time, os, json
from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    TimeoutException
)
def append_ndjson_texts(out_path: str, texts, page_no: int, cursor_val: str | None):
    """
    Ghi mỗi comment 1 dòng NDJSON:
    {"text": "...", "page": 3, "idx": 17, "cursor": "..."}

    - Không dedup ở đây (để đơn giản và giữ đủ dữ liệu thô).
    - Dùng encoding UTF-8, ensure_ascii=False để giữ tiếng Việt.
    """
    if not texts:
        return 0
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    wrote = 0
    with open(out_path, "a", encoding="utf-8") as f:
        for i, t in enumerate(texts):
            # Nếu batch_texts là chuỗi thuần:
            if isinstance(t, str):
                obj = {"text": t, "page": page_no, "idx": i}
            else:
                # Phòng khi sau này bạn đổi parser trả dict (text, author,...)
                obj = dict(t)
                obj.setdefault("page", page_no)
                obj.setdefault("idx", i)

                # 🔹 ƯU TIÊN map text từ các field quen thuộc:
                if "text" not in obj:
                    if "content" in obj:
                        # dùng cho output của extract_full_posts_from_resptext / extract_replies_from_depth_resp
                        obj["text"] = obj.get("content")
                    elif "body" in obj:
                        obj["text"] = obj.get("body")

            if cursor_val:
                obj["cursor"] = cursor_val

            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
            wrote += 1
    return wrote

def _button_xpaths_for_texts(texts):
    # We’ll try (1) role=button with span text; (2) role=button with innerText; (3) aria-label
    # Plus some FB-specific wrappers.
    xps = []
    for t in texts:
        # exact span match
        xps += [
            f"//div[@role='button'][.//span[normalize-space()='{t}']]",
            f"//div[@role='button'][normalize-space(.)='{t}']",
            f"//div[@role='button'][contains(., '{t}')]",
            f"//*[@role='button' and @aria-label='{t}']",
            f"//*[@role='button' and contains(@aria-label, '{t}')]",
        ]
        # menus sometimes live in composite buttons
        xps += [
            f"//span[normalize-space()='{t}']/ancestor::*[@role='button'][1]",
        ]
    # A couple of generic fallbacks frequently seen on Reels/Video surfaces
    xps += [
        # sort pills near comment header
        "//div[@role='button'][.//span[contains(., 'bình luận')] and .//span[contains(., 'hợp')]]",
        # last visible button inside the last dialog as fallback
        "(//div[@role='dialog'])[last()]//div[@role='button'][.//span][last()]",
    ]
    return xps

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

def _visible(elem):
    try:
        return elem.is_displayed() and elem.is_enabled()
    except Exception:
        return False
    
def _menuitem_xpaths_for_texts(texts):
    # FB mounts menus at body-level; include role=menuitem/option/button, and listbox options.
    xps = []
    for t in texts:
        xps += [
            # common menu containers
            f"(//div[@role='menu'] | //div[@role='listbox'] | //div[@role='dialog'] | //div[@role='presentation'] | /html/body)"
            f"//div[@role='menuitem' or @role='option' or @role='button'][.//span[normalize-space()='{t}']]",
            f"(//div[@role='menu'] | //div[@role='listbox'] | //div[@role='dialog'] | //div[@role='presentation'] | /html/body)"
            f"//div[@role='menuitem' or @role='option' or @role='button'][contains(., '{t}')]",
            # sometimes text is directly on a span/div
            f"(//div[@role='menu'] | //div[@role='listbox'] | /html/body)//*[normalize-space()='{t}']/ancestor::*[@role='menuitem' or @role='option' or @role='button'][1]",
        ]
    # Fallback: last open menuitem in the last menu
    xps += [
        "(//div[@role='menu'])[last()]//div[@role='menuitem' or @role='option' or @role='button'][last()]"
    ]
    return xps