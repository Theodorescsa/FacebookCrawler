# =========================
# FB GROUP POST CRAWLER (single-day, clean version)
# =========================

import os, sys, time, signal, json
from pathlib import Path
from typing import Optional, List, Dict, Any, Set
from datetime import datetime, date

from utils import parse_fb_graphql_payload, append_ndjson

# --- project imports ---
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from logs.loging_config import logger
from util.startdriverproxy import bootstrap_auth
from automation import CLEANUP_JS, go_to_date, install_early_hook
from get_info import _best_primary_key, coalesce_posts, collect_post_summaries

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# =========================
# GLOBALS
# =========================

_LATEST_CREATED_TS: Optional[int] = None   # unix ts mới nhất lấy được
_SHOULD_STOP = False


# =========================
# Helpers: env + args
# =========================

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
        help="Thư mục gốc database",
    )
    ap.add_argument(
        "--cookies-path",
        type=str,
        default=env("COOKIE_PATH", ""),
        help="Đường dẫn file cookies.json",
    )
    ap.add_argument(
        "--keep-last",
        type=int,
        default=env("KEEP_LAST", 350, int),
        help="Số bản ghi GQL giữ lại trong window.__gqlReqs",
    )
    ap.add_argument(
        "--headless",
        action="store_true",
        help="Chạy headless (ẩn Chrome)",
    )
    ap.add_argument(
        "--no-headless",
        action="store_true",
        help="Force non-headless",
    )
    ap.add_argument(
        "--page-limit",
        type=int,
        default=env("PAGE_LIMIT", None, int),
        help="Giới hạn lượt scroll (None = 10000)",
    )
    ap.add_argument(
        "--date",
        type=str,
        help="YYYY-MM-DD (ngày cần crawl, mặc định = hôm nay)",
    )


def compute_paths(data_root: Path, page_name: str, account_tag: str):
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
    if args.headless:
        return True
    if args.no_headless:
        return False
    # default: chạy có UI cho dễ debug
    return False


# =========================
# Selenium helpers
# =========================

def create_chrome(headless: bool = False):
    chrome_opts = Options()
    chrome_opts.add_argument("--disable-notifications")
    chrome_opts.add_argument("--disable-dev-shm-usage")
    chrome_opts.add_argument("--disable-gpu")
    chrome_opts.add_argument("--no-sandbox")
    chrome_opts.add_argument("--start-maximized")

    if headless:
        chrome_opts.add_argument("--headless=new")

    driver = webdriver.Chrome(options=chrome_opts)
    driver.set_page_load_timeout(40)
    driver.set_script_timeout(40)
    return driver


# =========================
# GraphQL helpers
# =========================

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
    rec: Dict[str, Any],
    group_url: str,
    seen_ids: Set[str],
    out_path: Path,
    log_prefix: str = "",
) -> int:
    """
    Xử lý 1 bản ghi GraphQL:
      - parse JSON
      - collect_post_summaries
      - coalesce_posts
      - dedup theo _best_primary_key
      - append_ndjson ngay
      - update _LATEST_CREATED_TS
    """
    global _LATEST_CREATED_TS

    text = rec.get("responseText")
    if not text:
        return 0

    payload = parse_fb_graphql_payload(text)
    if payload is None:
        logger.debug("[GQL%s] responseText parse fail (no JSON payload)", log_prefix)
        return 0

    raw_items: List[Dict[str, Any]] = []

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

    written_this_round: Set[str] = set()
    fresh: List[Dict[str, Any]] = []
    for p in page_posts:
        pk = _best_primary_key(p)
        if pk and (pk not in seen_ids) and (pk not in written_this_round):
            fresh.append(p)
            written_this_round.add(pk)

    if not fresh:
        logger.debug("[GQL%s] no fresh posts after dedup", log_prefix)
        return 0

    # update latest created_time
    for p in fresh:
        ts = p.get("created_time")
        if isinstance(ts, (int, float)):
            if _LATEST_CREATED_TS is None or ts > _LATEST_CREATED_TS:
                _LATEST_CREATED_TS = ts

    append_ndjson(fresh, str(out_path))

    for p in fresh:
        pk = _best_primary_key(p)
        if pk:
            seen_ids.add(pk)

    logger.info("[GQL%s] wrote %d fresh posts", log_prefix, len(fresh))
    return len(fresh)


# =========================
# Scroll loop
# =========================

def _handle_sigterm(sig, frame):
    global _SHOULD_STOP
    _SHOULD_STOP = True
    logger.warning("[SIGNAL] Nhận tín hiệu dừng, sẽ thoát an toàn sau bước hiện tại.")


signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)


def crawl_scroll_loop(
    d,
    group_url: str,
    out_path: Path,
    seen_ids: Set[str],
    keep_last: int,
    max_scrolls: int = 10000,
):
    MAX_SCROLLS     = max_scrolls or 10000
    CLEANUP_EVERY   = 25
    STALL_THRESHOLD = 8

    DOM_KEEP = max(30, min(keep_last or 40, 60))

    prev_height = None
    stall_count = 0
    idle_rounds_no_new_posts = 0
    i = 0

    while True:
        if _SHOULD_STOP:
            logger.info("[STOP] Received stop flag, breaking scroll loop.")
            break

        if i >= MAX_SCROLLS:
            logger.info("[STOP] Reach MAX_SCROLLS=%d, break loop.", MAX_SCROLLS)
            break

        # ✅ Scroll
        try:
            d.execute_script(
                "window.scrollBy(0, Math.floor(window.innerHeight * 0.9));"
            )
        except Exception as e:
            logger.warning("[SCROLL] execute_script error: %s", e)
            break

        time.sleep(1.0)

        # ✅ GQL batch xử lý
        recs = flush_gql_recs(d)
        total_new_from_batch = 0

        if recs:
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

        # ✅ Track idle rounds (không thu được bài mới)
        if total_new_from_batch == 0:
            idle_rounds_no_new_posts += 1
        else:
            idle_rounds_no_new_posts = 0

        # ✅ Cleanup DOM
        if i > 0 and (i % CLEANUP_EVERY == 0):
            try:
                d.execute_script(CLEANUP_JS, DOM_KEEP)
            except:
                pass

        # ✅ Scroll height logic
        try:
            cur_height = d.execute_script("return document.body.scrollHeight;")
        except:
            break

        if prev_height is None:
            prev_height = cur_height
        else:
            if cur_height <= prev_height and total_new_from_batch == 0:
                stall_count += 1
            else:
                stall_count = 0
                prev_height = cur_height

        # ✅ Điều kiện dừng CHẮC CHẮN
        if stall_count >= STALL_THRESHOLD and idle_rounds_no_new_posts >= 10:
            logger.info(
                "[STOP] Stall confirmed: no new posts for %d rounds & height stagnant.",
                idle_rounds_no_new_posts,
            )
            break

        i += 1
        time.sleep(1)
    logger.info(
        "[DONE] Crawl loop finished. Total unique posts seen: %d", len(seen_ids)
    )

# =========================
# MAIN
# =========================

if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser("FB Post Crawler (single-day, clean)")
    add_common_args(ap)
    args = ap.parse_args()

    GROUP_URL = args.group_url.strip()
    PAGE_NAME = args.page_name.strip()
    ACCOUNT_TAG = (args.account_tag or "").strip()
    DATA_ROOT = Path(args.data_root).resolve()
    KEEP_LAST = int(args.keep_last)
    COOKIES = args.cookies_path.strip()

    DATABASE_PATH, OUT_NDJSON, RAW_DUMPS_DIR, CHECKPOINT = compute_paths(
        DATA_ROOT, PAGE_NAME, ACCOUNT_TAG
    )

    logger.info(
        "[BOOT] PAGE=%s | TAG=%s | DATA_ROOT=%s",
        PAGE_NAME,
        ACCOUNT_TAG or "-",
        DATA_ROOT,
    )
    logger.info(
        "[PATH] DB=%s | OUT=%s | CKPT=%s", DATABASE_PATH, OUT_NDJSON, CHECKPOINT
    )

    # ngày cần crawl
    if args.date:
        try:
            target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError:
            raise SystemExit("--date phải ở format YYYY-MM-DD, ví dụ: 2024-12-01")
    else:
        target_date = date.today()

    logger.info("====== CRAWL NGÀY %s ======", target_date.isoformat())

    seen_ids: Set[str] = set()

    # 1) Start WebDriver
    d = create_chrome(headless=make_headless(args))

    # 2) Auth bằng cookies
    if COOKIES and os.path.exists(COOKIES):
        try:
            bootstrap_auth(d, COOKIES)
            logger.info("[AUTH] bootstrap_auth OK with cookies %s", COOKIES)
        except Exception as e:
            logger.error("[AUTH] bootstrap_auth FAILED: %s", e)
    else:
        logger.warning("[AUTH] Không tìm thấy cookies: %s", COOKIES)

    # 3) Enable CDP + hook GQL
    try:
        d.execute_cdp_cmd("Network.enable", {})
        d.execute_cdp_cmd("Network.setCacheDisabled", {"cacheDisabled": True})
    except Exception as e:
        logger.warning("[CDP] Cannot enable network/disable cache: %s", e)

    try:
        install_early_hook(d, keep_last=KEEP_LAST)
        logger.info("[HOOK] install_early_hook OK (keep_last=%s)", KEEP_LAST)
    except Exception as e:
        logger.error("[HOOK] install_early_hook FAILED: %s", e)

    # 4) Crawl 1 ngày
    try:
        d.get(GROUP_URL)
        time.sleep(1.5)

        go_to_date(d, target_date)

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

    # 5) Ghi checkpoint theo created_time mới nhất
    if _LATEST_CREATED_TS is not None:
        try:
            CHECKPOINT.parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass

        ck = {
            "last_created_time": int(_LATEST_CREATED_TS),
            "last_created_date": datetime.fromtimestamp(
                _LATEST_CREATED_TS
            ).strftime("%Y-%m-%d"),
        }
        with open(CHECKPOINT, "w", encoding="utf-8") as f:
            json.dump(ck, f, ensure_ascii=False, indent=2)
        logger.info(
            "[CKPT] Saved checkpoint: ts=%s date=%s",
            ck["last_created_time"],
            ck["last_created_date"],
        )

    logger.info(
        "[DONE] Finished crawl for %s. Total unique posts: %d",
        target_date,
        len(seen_ids),
    )
