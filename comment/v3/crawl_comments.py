# crawl_comments.py

import time
import json
from pathlib import Path
import sys

# ----- Import nội bộ -----
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utils import append_ndjson_texts
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
    """
    added = 0
    for rec in reqs:
        resp_text = rec.get("responseText")
        if not resp_text or not isinstance(resp_text, str):
            continue

        # 1) Comment chính
        rows, end_cursor, total, _ = extract_full_posts_from_resptext(resp_text)
        if rows:
            append_ndjson_texts(out_path, rows, page_no=round_idx, cursor_val=end_cursor)
            for row in rows:
                cid = row.get("raw_comment_id") or row.get("id")
                if not cid: continue
                if cid not in by_id:
                    by_id[cid] = row
                    added += 1
                else:
                    by_id[cid].update({k: v for k, v in row.items() if v not in (None, "", [], {})})

        # 2) Reply depth-1
        replies, next_token = extract_replies_from_depth_resp(resp_text)
        if replies:
            append_ndjson_texts(out_path, replies, page_no=round_idx, cursor_val=next_token or end_cursor)
            for r in replies:
                cid = r.get("raw_comment_id") or r.get("id")
                if not cid: continue
                if cid not in by_id:
                    by_id[cid] = r
                    added += 1
                else:
                    by_id[cid].update({k: v for k, v in r.items() if v not in (None, "", [], {})})
    return added


def crawl_comments_for_post(
    driver,                      # 👈 Nhận driver từ bên ngoài
    page_url: str,
    max_rounds: int = 1000,
    sleep_between_rounds: float = 1.5,
    out_path: str = "comments.ndjson",
):
    """
    Dùng driver có sẵn để crawl comment. 
    KHÔNG quit driver, KHÔNG close profile.
    """
    logger.info("[CRAWLER] Start processing link: %s", page_url)

    try:
        # Mở bài viết
        driver.get(page_url)
        time.sleep(2.5)

        # Xử lý Reel/Video mode
        try:
            opened = open_reel_comments_if_present(driver)
            logger.info("[CRAWLER] open_reel_comments_if_present -> %s", opened)
        except Exception as e:
            logger.warning("[CRAWLER] open_reel_comments_if_present error: %s", e)

        # Set sort = All comments
        try:
            set_sort_to_all_comments_unified(driver)
            logger.info("[CRAWLER] Set sort = All comments OK")
        except Exception as e:
            logger.warning("[CRAWLER] Cannot set sort to All comments: %s", e)

        # Biến trạng thái crawl
        last_len = 0
        by_id = {}
        rounds_no_new = 0

        for r in range(max_rounds):
            logger.info("[CRAWLER] Round %s/%s", r + 1, max_rounds)
            round_idx = r

            # Scroll
            try:
                scrolled = scroll_to_last_comment(driver)
            except Exception:
                scrolled = False
            
            # Click view more
            try:
                clicked = click_view_more_if_any(driver, max_clicks=3)
            except Exception:
                clicked = 0

            time.sleep(sleep_between_rounds)

            # Pull GQL
            new_reqs, last_len = _pull_new_gql_reqs(driver, last_len)
            logger.info("[CRAWLER] New gql reqs: %s", len(new_reqs))

            # Extract
            added = _extract_comments_from_requests(
                new_reqs,
                by_id=by_id,
                out_path=out_path,
                round_idx=round_idx,
            )
            logger.info("[CRAWLER] Added: %s (Total unique: %s)", added, len(by_id))

            if added == 0:
                rounds_no_new += 1
            else:
                rounds_no_new = 0

            # Điều kiện dừng
            if rounds_no_new >= 2 and clicked == 0 and not scrolled:
                logger.info("[CRAWLER] No more new comments -> Stop.")
                break

        rows = list(by_id.values())
        logger.info("[CRAWLER] Done link. Total items: %s", len(rows))
        return rows

    except Exception as e:
        logger.error("[CRAWLER] Runtime error processing link %s: %s", page_url, e)
        return []

    # KHÔNG finally quit driver ở đây để giữ session cho link tiếp theo