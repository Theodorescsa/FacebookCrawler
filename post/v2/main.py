
# =========================
# MAIN
# =========================
import urllib, time, sys, os
from datetime import datetime
from pathlib  import Path
from automation import install_early_hook, wait_next_req
from checkpoint import load_checkpoint, normalize_seen_ids, save_checkpoint
from get_posts_fb_automation import paginate_window, run_cursor_only
from utils import   get_vars_from_form, is_group_feed_req, make_vars_template, parse_form, update_vars_for_next_cursor

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
from logs.loging_config import logger
from util.startdriverproxy import bootstrap_auth,start_driver_with_proxy

GROUP_URL     = "https://www.facebook.com/thoibao.de"
KEEP_LAST     = 350
PAGE_NAME     = "thoibaode"
DATABASE_PATH = Path(__file__).resolve().parent.parent.parent / "database" / "post" / "page" / PAGE_NAME
OUT_NDJSON_    = DATABASE_PATH / "post" /  "page" /  PAGE_NAME / "posts_all.ndjson"
RAW_DUMPS_DIR_ = DATABASE_PATH / "post" /  "page" /  PAGE_NAME / "raw_dump_posts"
CHECKPOINT_    = DATABASE_PATH / "post" /  "page" /  PAGE_NAME / "checkpoint.json"
COOKIES_PATH = r"E:\NCS\fb-selenium\database\facebookaccount\authen_tranhoangdinhnam\cookies.json"
CHECKPOINT   = DATABASE_PATH / "checkpoint.json"
OUT_NDJSON    = DATABASE_PATH / "posts_all.ndjson"

DATABASE_PATH.mkdir(parents=True, exist_ok=True)
OUT_NDJSON.parent.mkdir(parents=True, exist_ok=True)
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--resume", action="store_true",
                    help="Tiếp tục từ cursor trong checkpoint thay vì bám head.")
    ap.add_argument("--page-limit", type=int, default=None,
                    help="Giới hạn số trang để test (None = không giới hạn).")
    ap.add_argument("--backfill", action="store_true",
                    help="Crawl ngược thời gian (ví dụ từ tháng 8/2015 đến tháng 6/2015).")
    ap.add_argument("--from-month", type=int, default=None, help="Tháng bắt đầu (ví dụ: 8).")
    ap.add_argument("--to-month", type=int, default=None, help="Tháng kết thúc (ví dụ: 6).")
    ap.add_argument("--year", type=int, default=None, help="Năm (ví dụ: 2015).")

    args = ap.parse_args()

    d = start_driver_with_proxy(
        proxy_host="142.111.48.253",
        proxy_port=7030,
        proxy_user="ycycsdtq",
        proxy_pass="ka0d32hzsydi",
        mitm_port=8899,
        headless=False
    ) 
    d.set_script_timeout(40)
    try:
        d.execute_cdp_cmd("Network.enable", {})
        d.execute_cdp_cmd("Network.setCacheDisabled", {"cacheDisabled": True})
    except Exception:
        pass

    bootstrap_auth(d, COOKIES_PATH)
    try:
        install_early_hook(d, keep_last=KEEP_LAST)
    except Exception as e:
        logger.error("[WARN] install_early_hook:", e)

    d.get(GROUP_URL); time.sleep(1.2)
    for _ in range(6):
        d.execute_script("window.scrollBy(0, Math.floor(window.innerHeight*0.9));"); time.sleep(0.6)

    nxt = wait_next_req(d, 0, is_group_feed_req, timeout=25, poll=0.25)
    if not nxt:
        raise RuntimeError("Không bắt được request feed. Hãy cuộn thêm/kiểm tra quyền.")
    _, first_req = nxt
    form         = parse_form(first_req.get("body", ""))
    friendly     = urllib.parse.parse_qs(first_req.get("body","")).get("fb_api_req_friendly_name", [""])[0]
    vars_now     = get_vars_from_form(form)
    template_now = make_vars_template(vars_now)

    state = load_checkpoint(DATABASE_PATH / "checkpoint.json")
    seen_ids      = normalize_seen_ids(state.get("seen_ids"))
    cursor_ckpt   = state.get("cursor")
    vars_template = state.get("vars_template") or template_now
    effective_template = vars_template or template_now
    if args.backfill and args.year and args.from_month and args.to_month:
        logger.info(f"[MODE] Backfill từ tháng {args.from_month}/{args.year} → {args.to_month}/{args.year}")
        cur = args.from_month
        while cur >= args.to_month:
            start = datetime.datetime(args.year, cur, 1)
            if cur == 1:
                end = datetime.datetime(args.year - 1, 12, 1)
            else:
                end = datetime.datetime(args.year, cur - 1, 1)

            t_from = int(end.timestamp())
            t_to = int(start.timestamp())

            logger.info(f"\n🕰️ Crawling trước {start.strftime('%Y-%m-%d')} ...")
            print("RAW_DUMPS_DIR",DATABASE_PATH / "raw_dump_posts")
            total_new, min_created, has_next = paginate_window(
                d, form, effective_template, seen_ids=set(),
                t_from=t_from,
                t_to=t_to,
                group_url=GROUP_URL,
                database_path = DATABASE_PATH,
                page_limit=args.page_limit
            )
            logger.info(f"✅ Done {start.strftime('%Y-%m')} → {total_new} posts | min_created={min_created}")
            save_checkpoint(cursor=None, seen_ids=list(seen_ids),
                            vars_template=effective_template,
                            mode="time", slice_from=None, slice_to=t_to, year=args.year,check_point_path=CHECKPOINT)
            time.sleep(2)
            cur -= 1

        logger.info("\n🎉 [DONE] Backfill completed.")
        d.quit()
        sys.exit(0)

    # ✅ Resume đúng vị trí (nếu có --resume và có cursor trong checkpoint)
    if args.resume and cursor_ckpt:
        form = update_vars_for_next_cursor(form, cursor_ckpt, vars_template=effective_template)
        logger.info(f"[RESUME] Dùng lại cursor từ checkpoint: {str(cursor_ckpt)[:40]}...")

    # 🔁 Chạy crawl theo cursor-only (không time-slice)
    total_got = run_cursor_only(
        d, 
        form, 
        effective_template, 
        seen_ids,
        database_path = DATABASE_PATH,
        page_limit=args.page_limit,
        resume=args.resume   
    )

    # Lưu checkpoint cuối (giữ seen_ids & template; cursor đã được cập nhật trong quá trình paginate)
    save_checkpoint(cursor=None, seen_ids=list(seen_ids), vars_template=effective_template,
                    mode=None, slice_from=None, slice_to=None, year=None)
    logger.info(f"[DONE] total new written (cursor-only) = {total_got} → {OUT_NDJSON}")
