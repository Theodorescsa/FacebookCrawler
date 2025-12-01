# crawl_comments.py

import time
import json
from pathlib import Path
import sys

from utils import append_ndjson_texts

# ----- Import nội bộ -----
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from driver import create_chrome
from util.startdriverproxy import bootstrap_auth
from hook import install_early_hook
from automation import (
    open_reel_comments_if_present,
    set_sort_to_all_comments_unified,
    scroll_to_last_comment,
    click_view_more_if_any,
)
from extract import (
    extract_full_posts_from_resptext,
    extract_replies_from_depth_resp,
)
from logs.loging_config import logger


def _pull_new_gql_reqs(driver, last_len: int):
    """
    Lấy list request GraphQL mới từ window.__gqlReqs kể từ last_len.
    Trả về (new_reqs, new_len).
    """
    try:
        buf = driver.execute_script("return (window.__gqlReqs || []);")
    except Exception:
        return [], last_len

    if not isinstance(buf, list):
        return [], last_len

    if last_len < 0 or last_len > len(buf):
        last_len = 0

    new_reqs = buf[last_len:]
    return new_reqs, len(buf)


def _extract_comments_from_requests(reqs, by_id: dict, out_path: str, round_idx: int):
    """
    Đi qua list request, parse comment/reply, gom vào by_id (anti-dup).
    by_id: map raw_comment_id -> row
    Đồng thời append NDJSON ra file out_path cho từng batch.
    """
    added = 0

    for rec in reqs:
        resp_text = rec.get("responseText")
        if not resp_text or not isinstance(resp_text, str):
            continue

        # 1) Comment chính
        rows, end_cursor, total, _ = extract_full_posts_from_resptext(resp_text)

        if rows:
            # out_path = "comments.ndjson" cũ -> dùng biến truyền vào
            append_ndjson_texts(out_path, rows, page_no=round_idx, cursor_val=end_cursor)

            for row in rows:
                cid = row.get("raw_comment_id") or row.get("id")
                if not cid:
                    continue
                if cid not in by_id:
                    by_id[cid] = row
                    added += 1
                else:
                    by_id[cid].update({k: v for k, v in row.items() if v not in (None, "", [], {})})

        # 2) Reply depth-1 (nếu payload dạng reply)
        replies, next_token = extract_replies_from_depth_resp(resp_text)

        if replies:
            append_ndjson_texts(
                out_path,
                replies,
                page_no=round_idx,
                cursor_val=next_token or end_cursor,
            )

            for r in replies:
                cid = r.get("raw_comment_id") or r.get("id")
                if not cid:
                    continue
                if cid not in by_id:
                    by_id[cid] = r
                    added += 1
                else:
                    by_id[cid].update({k: v for k, v in r.items() if v not in (None, "", [], {})})

    return added


def crawl_comments_for_post(
    page_url: str,
    cookies_path: str,
    max_rounds: int = 1000,
    sleep_between_rounds: float = 1.5,
    headless: bool = False,
    out_path: str = "comments.ndjson",   # 👈 thêm default path
):
    """
    Mở 1 bài viết Facebook, scroll + click “Xem thêm bình luận”, 
    hứng GraphQL trong window.__gqlReqs, parse comment + reply.

    Trả về: list[row_dict]
    """
    logger.info("[CRAWLER] Start crawl comments for: %s", page_url)

    driver = create_chrome(headless=headless)

    try:
        # Auth theo cookies có sẵn
        bootstrap_auth(driver, cookies_path)

        # bật Network (optional, chủ yếu để bạn debug)
        try:
            driver.execute_cdp_cmd("Network.enable", {})
            driver.execute_cdp_cmd("Network.setCacheDisabled", {"cacheDisabled": True})
        except Exception as e:
            logger.warning("[CDP] Cannot enable network/disable cache: %s", e)

        # gắn hook (trước khi load page)
        try:
            install_early_hook(driver)
            logger.info("[HOOK] install_early_hook OK")
        except Exception as e:
            logger.error("[HOOK] install_early_hook FAILED: %s", e)

        # mở bài viết
        driver.get(page_url)
        time.sleep(2.5)

        # Nếu là Reel / Video dạng mới -> mở panel bình luận (nếu có)
        if "reel" in page_url:
            try:
                opened = open_reel_comments_if_present(driver)
                logger.info("[CRAWLER] open_reel_comments_if_present -> %s", opened)
            except Exception as e:
                logger.warning("[CRAWLER] open_reel_comments_if_present error: %s", e)

        # Set sort = All comments (nếu tìm được nút)
        try:
            set_sort_to_all_comments_unified(driver)
            logger.info("[CRAWLER] Set sort = All comments OK")
        except Exception as e:
            logger.warning("[CRAWLER] Cannot set sort to All comments: %s", e)

        # Biến trạng thái crawl
        last_len = 0       # số lượng req trước đó trong window.__gqlReqs
        by_id = {}         # map comment_id -> row
        rounds_no_new = 0  # số vòng liền không thêm comment mới

        for r in range(max_rounds):
            logger.info("[CRAWLER] Round %s/%s", r + 1, max_rounds)
            round_idx = r  # dùng cho page_no NDJSON

            # 1) Scroll xuống cuối block bình luận
            try:
                scrolled = scroll_to_last_comment(driver)
                logger.info("[CRAWLER] scroll_to_last_comment -> %s", scrolled)
            except Exception as e:
                logger.warning("[CRAWLER] scroll_to_last_comment error: %s", e)
                scrolled = False

            # 2) Click "Xem thêm bình luận / phản hồi" nếu có
            try:
                clicked = click_view_more_if_any(driver, max_clicks=3)
                logger.info("[CRAWLER] click_view_more_if_any -> clicked=%s", clicked)
            except Exception as e:
                logger.warning("[CRAWLER] click_view_more_if_any error: %s", e)
                clicked = 0

            # 3) Cho FB thời gian load & bắn GraphQL
            time.sleep(sleep_between_rounds)

            # 4) Lấy request GraphQL mới
            new_reqs, last_len = _pull_new_gql_reqs(driver, last_len)
            logger.info("[CRAWLER] New gql reqs this round: %s", len(new_reqs))

            # 5) Parse comment từ responseText + append NDJSON
            added = _extract_comments_from_requests(
                new_reqs,
                by_id=by_id,
                out_path=out_path,
                round_idx=round_idx,
            )
            logger.info("[CRAWLER] New comments/replies added: %s (total=%s)", added, len(by_id))

            if added == 0:
                rounds_no_new += 1
            else:
                rounds_no_new = 0

            if rounds_no_new >= 2 and clicked == 0 and not scrolled:
                logger.info("[CRAWLER] No more new comments for 2 rounds — stop.")
                break


        rows = list(by_id.values())
        logger.info("[CRAWLER] Done. Total unique comments+replies: %s", len(rows))
        return rows

    finally:
        try:
            driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    PAGE_URL = "https://www.facebook.com/...."  # link bài viết
    COOKIES_PATH = r"E:\NCS\fb-selenium\database\facebookaccount\authen_tranhoangdinhnam\cookies.json"

    rows = crawl_comments_for_post(
        page_url=PAGE_URL,
        cookies_path=COOKIES_PATH,
        max_rounds=50,
        sleep_between_rounds=1.5,
        headless=False,
    )

    # Ví dụ: dump ra JSON để kiểm tra
    out_path = Path("comments_dump.json")
    out_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Saved {len(rows)} rows to {out_path}")
