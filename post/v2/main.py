# =========================
# MAIN (multi-container friendly)
# =========================
import os, sys, time, urllib.parse, signal
from pathlib import Path
from datetime import datetime
from typing import Optional

# --- your project imports ---
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from logs.loging_config import logger
from util.startdriverproxy import bootstrap_auth, start_driver_with_proxy
from automation import install_early_hook, wait_next_req
from checkpoint import load_checkpoint, normalize_seen_ids, save_checkpoint
from get_posts_fb_automation import paginate_window, run_cursor_only
from utils import (
    get_vars_from_form, is_group_feed_req, make_vars_template,
    parse_form, update_vars_for_next_cursor
)

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
    ap.add_argument("--group-url", type=str, default=env("GROUP_URL", "https://www.facebook.com/thoibao.de"))
    ap.add_argument("--page-name", type=str, default=env("PAGE_NAME", "thoibaode"))
    ap.add_argument("--account-tag", type=str, default=env("ACCOUNT_TAG", ""), help="Nhãn để tách dữ liệu theo account (vd: acc01)")
    ap.add_argument("--data-root", type=str, default=env("DATA_ROOT", str(PROJECT_ROOT / "database")), help="Thư mục gốc database (trong container nên mount vào /app/database)")
    ap.add_argument("--cookies-path", type=str, default=env("COOKIE_PATH", ""), help="Đường dẫn file cookies.json trong container")
    ap.add_argument("--keep-last", type=int, default=env("KEEP_LAST", 350, int))
    ap.add_argument("--mitm-port", type=int, default=env("MITM_PORT", 8899, int))
    ap.add_argument("--proxy-host", type=str, default=env("PROXY_HOST", ""))
    ap.add_argument("--proxy-port", type=int, default=env("PROXY_PORT", 0, int))
    ap.add_argument("--proxy-user", type=str, default=env("PROXY_USER", ""))
    ap.add_argument("--proxy-pass", type=str, default=env("PROXY_PASS", ""))

    ap.add_argument("--headless", action="store_true", help="Chạy headless (ưu tiên nếu set)")
    ap.add_argument("--no-headless", action="store_true", help="Tắt headless nếu muốn xem trình duyệt")
    ap.add_argument("--resume", action="store_true", help="Tiếp tục từ cursor trong checkpoint thay vì bám head.")
    ap.add_argument("--page-limit", type=int, default=env("PAGE_LIMIT", None, int), help="Giới hạn số trang để test (None = không giới hạn).")

    # backfill theo tháng
    ap.add_argument("--backfill", action="store_true", help="Crawl ngược theo time-slice.")
    ap.add_argument("--from-month", type=int, default=env("FROM_MONTH", None, int))
    ap.add_argument("--to-month", type=int, default=env("TO_MONTH", None, int))
    ap.add_argument("--year", type=int, default=env("YEAR", None, int))

def compute_paths(data_root: Path, page_name: str, account_tag: str):
    """
    Trả về bộ đường dẫn đã tách biệt:
      database/post/page/<page_name>[/ACC_<account_tag>]
      + posts_all.ndjson, checkpoint.json, raw_dump_posts/
    """
    base = data_root / "post" / "page" / page_name
    if account_tag:
        base = base / f"ACC_{account_tag}"

    out_ndjson   = base / "posts_all.ndjson"
    raw_dump_dir = base / "raw_dump_posts"
    checkpoint   = base / "checkpoint.json"

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

# ---------------------------
# Graceful exit (SIGTERM)
# ---------------------------
_SHOULD_STOP = False
def _handle_sigterm(sig, frame):
    global _SHOULD_STOP
    _SHOULD_STOP = True
    logger.warning("[SIGNAL] Nhận tín hiệu dừng, sẽ thoát an toàn sau bước hiện tại.")
signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)

# ---------------------------
# MAIN
# ---------------------------
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser("FB Post Crawler (multi-container safe)")
    add_common_args(ap)
    args = ap.parse_args()

    GROUP_URL   = args.group_url.strip()
    PAGE_NAME   = args.page_name.strip()
    ACCOUNT_TAG = (args.account_tag or "").strip()
    DATA_ROOT   = Path(args.data_root).resolve()
    COOKIES     = args.cookies_path.strip()
    KEEP_LAST   = int(args.keep_last)
    MITM_PORT   = int(args.mitm_port)

    # Khởi tạo đường dẫn tách biệt theo PAGE + ACCOUNT_TAG (tránh đụng khi chạy song song)
    DATABASE_PATH, OUT_NDJSON, RAW_DUMPS_DIR, CHECKPOINT = compute_paths(DATA_ROOT, PAGE_NAME, ACCOUNT_TAG)

    # Log banner ngắn để phân biệt container
    logger.info(f"[BOOT] PAGE={PAGE_NAME} | TAG={ACCOUNT_TAG or '-'} | MITM={MITM_PORT} | DATA_ROOT={DATA_ROOT}")
    logger.info(f"[PATH] DB={DATABASE_PATH} | OUT={OUT_NDJSON} | CKPT={CHECKPOINT}")

    # Khởi driver + proxy
    headless = make_headless(args)
    d = start_driver_with_proxy(
        proxy_host=args.proxy_host or None,
        proxy_port=args.proxy_port or None,
        proxy_user=args.proxy_user or None,
        proxy_pass=args.proxy_pass or None,
        mitm_port=MITM_PORT,
        headless=headless
    )
    d.set_script_timeout(40)

    # Bật CDP tối ưu cache
    try:
        d.execute_cdp_cmd("Network.enable", {})
        d.execute_cdp_cmd("Network.setCacheDisabled", {"cacheDisabled": True})
    except Exception:
        pass

    # Auth
    if COOKIES and os.path.exists(COOKIES):
        bootstrap_auth(d, COOKIES)
    else:
        logger.warning(f"[AUTH] Không tìm thấy cookies: {COOKIES}")

    # Early hook
    try:
        install_early_hook(d, keep_last=KEEP_LAST)
    except Exception as e:
        logger.error("[WARN] install_early_hook: %s", e)

    # Load trang & bắt request feed
    d.get(GROUP_URL)
    time.sleep(1.2)
    for _ in range(6):
        if _SHOULD_STOP: break
        d.execute_script("window.scrollBy(0, Math.floor(window.innerHeight*0.9));")
        time.sleep(0.6)

    nxt = wait_next_req(d, 0, is_group_feed_req, timeout=25, poll=0.25)
    if not nxt:
        d.quit()
        raise RuntimeError("Không bắt được request feed. Hãy cuộn thêm/kiểm tra quyền.")

    _, first_req = nxt
    form         = parse_form(first_req.get("body", ""))
    friendly     = urllib.parse.parse_qs(first_req.get("body","")).get("fb_api_req_friendly_name", [""])[0]
    vars_now     = get_vars_from_form(form)
    template_now = make_vars_template(vars_now)

    # Checkpoint riêng (theo PAGE + TAG)
    state = load_checkpoint(CHECKPOINT)
    seen_ids      = normalize_seen_ids(state.get("seen_ids"))
    cursor_ckpt   = state.get("cursor")
    vars_template = state.get("vars_template") or template_now
    effective_template = vars_template or template_now

    # ---- BACKFILL MODE ----
    if args.backfill and args.year and args.from_month and args.to_month:
        logger.info(f"[MODE] Backfill {args.from_month:02d}/{args.year} → {args.to_month:02d}/{args.year}")
        cur = args.from_month
        while cur >= args.to_month:
            start_dt = datetime(args.year, cur, 1)
            if cur == 1:
                end_dt = datetime(args.year - 1, 12, 1)
            else:
                end_dt = datetime(args.year, cur - 1, 1)

            t_from = int(end_dt.timestamp())
            t_to   = int(start_dt.timestamp())

            logger.info(f"🕰️ Crawling trước {start_dt.strftime('%Y-%m-%d')} ...")
            total_new, min_created, has_next = paginate_window(
                d, form, effective_template,
                seen_ids=set(),               # backfill theo slice -> không dùng seen chung
                t_from=t_from,
                t_to=t_to,
                group_url=GROUP_URL,
                database_path=DATABASE_PATH,
                page_limit=args.page_limit
            )
            logger.info(f"✅ Done {start_dt.strftime('%Y-%m')} → {total_new} posts | min_created={min_created}")
            save_checkpoint(
                cursor=None,
                seen_ids=list(seen_ids),
                vars_template=effective_template,
                mode="time",
                slice_from=None,
                slice_to=t_to,
                year=args.year,
                check_point_path=CHECKPOINT
            )
            if _SHOULD_STOP:
                logger.warning("[STOP] Nhận tín hiệu dừng giữa backfill — thoát an toàn.")
                break
            time.sleep(2)
            cur -= 1

        logger.info("🎉 [DONE] Backfill completed.")
        d.quit()
        sys.exit(0)

    # ---- RESUME cursor-only ----
    if args.resume and cursor_ckpt:
        form = update_vars_for_next_cursor(form, cursor_ckpt, vars_template=effective_template)
        logger.info(f"[RESUME] Dùng lại cursor: {str(cursor_ckpt)[:40]}...")

    total_got = run_cursor_only(
        d,
        form,
        effective_template,
        seen_ids,
        database_path=DATABASE_PATH,
        page_limit=args.page_limit,
        resume=args.resume
    )

    # Lưu checkpoint cuối (per PAGE+TAG)
    save_checkpoint(
        cursor=None,
        seen_ids=list(seen_ids),
        vars_template=effective_template,
        mode=None, slice_from=None, slice_to=None, year=None,
        check_point_path=CHECKPOINT
    )
    logger.info(f"[DONE] total new written (cursor-only) = {total_got} → {OUT_NDJSON}")
    d.quit()
