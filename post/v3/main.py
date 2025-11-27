# =========================
# MAIN (multi-container friendly)
# =========================

import os, sys, time, signal
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime, date
from utils import parse_fb_graphql_payload, append_ndjson

# --- your project imports ---
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from logs.loging_config import logger
from util.startdriverproxy import bootstrap_auth, start_driver_with_proxy
from util.export_utils.export_fb_session import start_driver

from automation import CLEANUP_JS, go_to_date, install_early_hook
from get_info import _best_primary_key, coalesce_posts, collect_post_summaries


# ---------------------------
# Helpers: env + args
# ---------------------------
def env(key: str, default: Optional[str] = None, cast=str):
    v = os.environ.get(key, default)
    if v is None:
        return None
    if cast is bool:
        return str(v).lower() in ("1", "true", "yes", "y", "on")
    try:
        return cast(v)
    except Exception:
        return default


def add_common_args(ap):
    ap.add_argument(
        "--group-url",
        type=str,
        default=env("GROUP_URL", "https://www.facebook.com/thoibao.de"),
    )
    ap.add_argument(
        "--page-name",
        type=str,
        default=env("PAGE_NAME", "thoibaode"),
    )
    ap.add_argument(
        "--account-tag",
        type=str,
        default=env("ACCOUNT_TAG", ""),
        help="Nhãn để tách dữ liệu theo account (vd: acc01)",
    )
    ap.add_argument(
        "--data-root",
        type=str,
        default=env("DATA_ROOT", str(PROJECT_ROOT / "database")),
        help="Thư mục gốc database (trong container nên mount vào /app/database)",
    )
    ap.add_argument(
        "--cookies-path",
        type=str,
        default=env("COOKIE_PATH", ""),
        help="Đường dẫn file cookies.json trong container",
    )
    ap.add_argument(
        "--keep-last",
        type=int,
        default=env("KEEP_LAST", 350, int),
    )
    ap.add_argument(
        "--mitm-port",
        type=int,
        default=env("MITM_PORT", 8899, int),
    )
    ap.add_argument(
        "--proxy-host",
        type=str,
        default=env("PROXY_HOST", ""),
    )
    ap.add_argument(
        "--proxy-port",
        type=int,
        default=env("PROXY_PORT", 0, int),
    )
    ap.add_argument(
        "--proxy-user",
        type=str,
        default=env("PROXY_USER", ""),
    )
    ap.add_argument(
        "--proxy-pass",
        type=str,
        default=env("PROXY_PASS", ""),
    )

    ap.add_argument(
        "--headless",
        action="store_true",
        help="Chạy headless (ưu tiên nếu set)",
    )
    ap.add_argument(
        "--no-headless",
        action="store_true",
        help="Tắt headless nếu muốn xem trình duyệt",
    )

    ap.add_argument(
        "--resume",
        action="store_true",
        help="(tạm chưa dùng) Tiếp tục từ cursor trong checkpoint",
    )
    ap.add_argument(
        "--page-limit",
        type=int,
        default=env("PAGE_LIMIT", None, int),
        help="Giới hạn số lượt scroll để test (None = không giới hạn).",
    )

    # ✅ Chỉ chọn MỘT ngày để crawl
    ap.add_argument(
        "--date",
        type=str,
        help="YYYY-MM-DD (ngày cần crawl, mặc định = hôm nay)",
    )


def compute_paths(data_root: Path, page_name: str, account_tag: str):
    """
    Trả về bộ đường dẫn đã tách biệt:
      database/post/page/<page_name>[/ACC_<account_tag>]
      + posts_all.ndjson, checkpoint.json, raw_dump_posts/
    """
    base = data_root / "post" / "page" / page_name
    if account_tag:
        base = base / f"ACC_{account_tag}"

    out_ndjson = base / "posts_all.ndjson"
    raw_dump_dir = base / "raw_dump_posts"
    checkpoint = base / "checkpoint.json"

    base.mkdir(parents=True, exist_ok=True)
    raw_dump_dir.mkdir(parents=True, exist_ok=True)
    out_ndjson.parent.mkdir(parents=True, exist_ok=True)
    return base, out_ndjson, raw_dump_dir, checkpoint


def make_headless(args) -> bool:
    # Ưu tiên flag CLI; nếu không set, mặc định headless=true trong container
    if args.headless:
        return True
    if args.no_headless:
        return False
    return True


def flush_gql_recs(driver) -> List[Dict[str, Any]]:
    """
    Lấy toàn bộ các rec trong window.__gqlReqs rồi clear buffer.
    Mỗi rec có {kind, url, method, headers, body, responseText}
    """
    try:
        recs = driver.execute_script(
            """
            const q = window.__gqlReqs || [];
            window.__gqlReqs = [];
            return q;
        """
        )
        if not isinstance(recs, list):
            return []
        return recs
    except Exception:
        return []


def process_single_gql_rec(
    rec, group_url, seen_ids, out_path, log_prefix=""
) -> int:
    """
    Xử lý 1 bản ghi GraphQL:
      - parse JSON
      - collect_post_summaries
      - coalesce_posts
      - dedup theo _best_primary_key
      - append_ndjson ngay
    """
    text = rec.get("responseText")
    if not text:
        return 0

    payload = parse_fb_graphql_payload(text)
    if payload is None:
        logger.debug("[GQL%s] responseText parse fail (no JSON payload)", log_prefix)
        return 0

    raw_items: List[Dict[str, Any]] = []

    # Nếu payload là list thì xử lý từng cái, nếu dict thì xử lý luôn
    if isinstance(payload, dict):
        collect_post_summaries(payload, raw_items, group_url)
    elif isinstance(payload, list):
        for obj in payload:
            collect_post_summaries(obj, raw_items, group_url)

    if not raw_items:
        logger.debug("[GQL%s] collect_post_summaries -> 0 items", log_prefix)
        return 0

    logger.debug(
        "[GQL%s] collect_post_summaries -> %d items", log_prefix, len(raw_items)
    )

    # Nếu cần filter thêm loại post thì xử lý ở đây, tạm thời giữ nguyên:
    feed_items = raw_items

    if not feed_items:
        sample = raw_items[0]
        logger.debug(
            "[GQL%s] NO feed_items, sample: id=%s rid=%s link=%s",
            log_prefix,
            sample.get("id"),
            sample.get("rid"),
            sample.get("link"),
        )
        return 0

    page_posts = coalesce_posts(feed_items)
    logger.debug("[GQL%s] coalesce_posts -> %d items", log_prefix, len(page_posts))

    if not page_posts:
        return 0

    written_this_round = set()
    fresh = []
    for p in page_posts:
        pk = _best_primary_key(p)
        if pk and (pk not in seen_ids) and (pk not in written_this_round):
            fresh.append(p)
            written_this_round.add(pk)

    if not fresh:
        logger.debug("[GQL%s] no fresh posts after dedup", log_prefix)
        return 0

    append_ndjson(fresh, str(out_path))

    for p in fresh:
        pk = _best_primary_key(p)
        if pk:
            seen_ids.add(pk)

    logger.info("[GQL%s] wrote %d fresh posts", log_prefix, len(fresh))
    return len(fresh)


# ---------------------------
# MAIN LOOP HELPERS
# ---------------------------

_SHOULD_STOP = False


def _handle_sigterm(sig, frame):
    global _SHOULD_STOP
    _SHOULD_STOP = True
    logger.warning("[SIGNAL] Nhận tín hiệu dừng, sẽ thoát an toàn sau bước hiện tại.")


signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)


def crawl_scroll_loop(
    d,
    group_url: str,
    out_path,
    seen_ids: set,
    keep_last: int,
    max_scrolls: int = 10000,
):
    """
    Vòng lặp scroll + bắt /api/graphql cho MỘT NGÀY (đã được go_to_date nhảy tới).
    """
    MAX_SCROLLS = max_scrolls or 10000
    CLEANUP_EVERY = 25
    STALL_THRESHOLD = 8

    DOM_KEEP = 40
    if keep_last:
        DOM_KEEP = max(30, min(keep_last, 60))

    prev_height = None
    stall_count = 0

    for i in range(MAX_SCROLLS):
        if _SHOULD_STOP:
            logger.info("[STOP] Received stop flag, breaking scroll loop.")
            break

        # Scroll nhẹ ~0.9 viewport
        try:
            d.execute_script(
                "window.scrollBy(0, Math.floor(window.innerHeight * 0.9));"
            )
        except Exception as e:
            logger.warning("[SCROLL] execute_script error: %s", e)
            break

        time.sleep(1.0)  # cho FB bắn request

        # Flush GraphQL log và xử lý từng rec ngay lập tức
        recs = flush_gql_recs(d)
        if recs:
            logger.debug("[GQL] loop #%d: flush %d recs", i, len(recs))
            total_new_from_batch = 0
            for idx, rec in enumerate(recs):
                num_new = process_single_gql_rec(
                    rec,
                    group_url=group_url,
                    seen_ids=seen_ids,
                    out_path=out_path,
                    log_prefix=f"#{i}/{idx}",
                )
                total_new_from_batch += num_new

            if total_new_from_batch:
                logger.info(
                    "[GQL] #%d: collected %d new posts (total_seen=%d)",
                    i,
                    total_new_from_batch,
                    len(seen_ids),
                )
        else:
            logger.debug("[GQL] loop #%d: no recs", i)

        # 🧹 Thỉnh thoảng dọn DOM để tránh phình to
        if i > 0 and (i % CLEANUP_EVERY == 0):
            try:
                d.execute_script(CLEANUP_JS, DOM_KEEP)
                logger.debug(
                    "[CLEANUP] loop #%d: executed CLEANUP_JS keep=%d",
                    i,
                    DOM_KEEP,
                )
            except Exception as e:
                logger.warning(
                    "[CLEANUP] error running CLEANUP_JS at loop %d: %s",
                    i,
                    e,
                )

        # 🚦 Stall detection: nếu không còn load thêm
        try:
            cur_height = d.execute_script("return document.body.scrollHeight;")
        except Exception as e:
            logger.warning("[SCROLL] get scrollHeight error: %s", e)
            break

        if prev_height is None:
            prev_height = cur_height
        else:
            if cur_height <= prev_height and not recs:
                stall_count += 1
                logger.info(
                    "[STALL] loop=%d stall_count=%d (height=%s)",
                    i,
                    stall_count,
                    cur_height,
                )
                if stall_count >= STALL_THRESHOLD:
                    logger.info(
                        "[STOP] Reach stall threshold (%d), break scroll loop.",
                        STALL_THRESHOLD,
                    )
                    break
            else:
                stall_count = 0
                prev_height = cur_height

    logger.info(
        "[DONE] Crawl loop finished. Total unique posts seen: %d", len(seen_ids)
    )


# ---------------------------
# MAIN
# ---------------------------
if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser("FB Post Crawler (single-day mode)")
    add_common_args(ap)
    args = ap.parse_args()

    GROUP_URL = args.group_url.strip()
    PAGE_NAME = args.page_name.strip()
    ACCOUNT_TAG = (args.account_tag or "").strip()
    DATA_ROOT = Path(args.data_root).resolve()
    KEEP_LAST = int(args.keep_last)
    COOKIES     = args.cookies_path.strip()

    DATABASE_PATH, OUT_NDJSON, RAW_DUMPS_DIR, CHECKPOINT = compute_paths(
        DATA_ROOT, PAGE_NAME, ACCOUNT_TAG
    )

    logger.info(
        "[BOOT] PAGE=%s | TAG=%s | MITM=%s | DATA_ROOT=%s",
        PAGE_NAME,
        ACCOUNT_TAG or "-",
        args.mitm_port,
        DATA_ROOT,
    )
    logger.info(
        "[PATH] DB=%s | OUT=%s | CKPT=%s", DATABASE_PATH, OUT_NDJSON, CHECKPOINT
    )

    # ✅ Xác định NGÀY CẦN CRAWL
    if args.date:
        try:
            target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError:
            raise SystemExit("--date phải ở format YYYY-MM-DD, ví dụ: 2024-12-01")
    else:
        target_date = date.today()

    logger.info("====== CRAWL NGÀY %s ======", target_date.isoformat())

    seen_ids: set[str] = set()

    # d = start_driver_with_proxy(
    #     proxy_host=args.proxy_host or None,
    #     proxy_port=args.proxy_port or None,
    #     proxy_user=args.proxy_user or None,
    #     proxy_pass=args.proxy_pass or None,
    #     headless=False
    # )
    # d.set_script_timeout(40)

    # # Bật CDP tối ưu cache
    # try:
    #     d.execute_cdp_cmd("Network.enable", {})
    #     d.execute_cdp_cmd("Network.setCacheDisabled", {"cacheDisabled": True})
    # except Exception:
    #     pass

    # # Auth
    # if COOKIES and os.path.exists(COOKIES):
    #     bootstrap_auth(d, COOKIES)
    # else:
    #     logger.warning(f"[AUTH] Không tìm thấy cookies: {COOKIES}")
    d = start_driver()
    try:
        d.get(GROUP_URL)
        time.sleep(1.5)

        # Chọn MỘT ngày (ngày kết thúc) trên UI bằng popup 'Đi đến'
        go_to_date(d, target_date)

        # Scroll + bắt GraphQL cho NGÀY target_date
        crawl_scroll_loop(
            d,
            group_url=GROUP_URL,
            out_path=OUT_NDJSON,
            seen_ids=seen_ids,
            keep_last=KEEP_LAST,
            max_scrolls=args.page_limit or 10000,
        )

    finally:
        try:
            d.quit()
        except Exception:
            pass

    logger.info(
        "[DONE] Finished crawl for %s. Total unique posts: %d",
        target_date,
        len(seen_ids),
    )
