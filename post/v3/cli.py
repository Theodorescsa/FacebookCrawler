# post/v3/cli.py
import os
import sys
import signal
import time
from pathlib import Path
from datetime import datetime, date
from typing import Set

from logs.loging_config import logger
from util.startdriverproxy import bootstrap_auth

from .config import PROJECT_ROOT, env
from .browser.driver import create_chrome, make_headless
from .browser.hooks import install_early_hook
from .browser.navigation import go_to_date
from .browser.scroll import crawl_scroll_loop, set_stop_flag
from .storage.paths import compute_paths
from .storage.checkpoint import save_checkpoint
from .pipeline import LATEST_CREATED_TS


def add_common_args(ap):
    ap.add_argument("--group-url", type=str,
                    default=env("GROUP_URL", "https://web.facebook.com/profile.php?id=100079810928296"))
    ap.add_argument("--page-name", type=str,
                    default=env("PAGE_NAME", "thoibaode"))
    ap.add_argument("--account-tag", type=str,
                    default=env("ACCOUNT_TAG", ""),
                    help="Nhãn để tách dữ liệu theo account (vd: acc01)")
    ap.add_argument("--data-root", type=str,
                    default=env("DATA_ROOT", str(PROJECT_ROOT / "database")),
                    help="Thư mục gốc database")
    ap.add_argument("--cookies-path", type=str,
                    default=env("COOKIE_PATH", ""),
                    help="Đường dẫn file cookies.json")
    ap.add_argument("--keep-last", type=int,
                    default=env("KEEP_LAST", 350, int),
                    help="Số bản ghi GQL giữ lại trong window.__gqlReqs")
    ap.add_argument("--headless", action="store_true",
                    help="Chạy headless (ẩn Chrome)")
    ap.add_argument("--no-headless", action="store_true",
                    help="Force non-headless")
    ap.add_argument("--page-limit", type=int,
                    default=env("PAGE_LIMIT", None, int),
                    help="Giới hạn lượt scroll (None = 10000)")
    ap.add_argument("--date", type=str,
                    help="YYYY-MM-DD (ngày cần crawl, mặc định = hôm nay)")


def _handle_sigterm(sig, frame):
    logger.warning("[SIGNAL] Nhận tín hiệu dừng, sẽ thoát an toàn sau bước hiện tại.")
    set_stop_flag()


signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)


def main(argv=None):
    import argparse

    ap = argparse.ArgumentParser("FB Post Crawler (single-day, clean)")
    add_common_args(ap)
    args = ap.parse_args(argv)

    group_url = args.group_url.strip()
    page_name = args.page_name.strip()
    account_tag = (args.account_tag or "").strip()
    data_root = Path(args.data_root).resolve()
    keep_last = int(args.keep_last)
    cookies = args.cookies_path.strip()

    database_path, out_ndjson, raw_dumps_dir, checkpoint = compute_paths(
        data_root, page_name, account_tag
    )

    logger.info(
        "[BOOT] PAGE=%s | TAG=%s | DATA_ROOT=%s",
        page_name,
        account_tag or "-",
        data_root,
    )
    logger.info(
        "[PATH] DB=%s | OUT=%s | CKPT=%s", database_path, out_ndjson, checkpoint
    )

    if args.date:
        try:
            target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError:
            raise SystemExit("--date phải ở format YYYY-MM-DD, ví dụ: 2024-12-01")
    else:
        target_date = date.today()

    logger.info("====== CRAWL NGÀY %s ======", target_date.isoformat())

    seen_ids: Set[str] = set()

    d = create_chrome(headless=make_headless(args))

    if cookies and os.path.exists(cookies):
        try:
            bootstrap_auth(d, cookies)
            logger.info("[AUTH] bootstrap_auth OK with cookies %s", cookies)
        except Exception as e:
            logger.error("[AUTH] bootstrap_auth FAILED: %s", e)
    else:
        logger.warning("[AUTH] Không tìm thấy cookies: %s", cookies)

    try:
        d.execute_cdp_cmd("Network.enable", {})
        d.execute_cdp_cmd("Network.setCacheDisabled", {"cacheDisabled": True})
    except Exception as e:
        logger.warning("[CDP] Cannot enable network/disable cache: %s", e)

    try:
        install_early_hook(d, keep_last=keep_last)
        logger.info("[HOOK] install_early_hook OK (keep_last=%s)", keep_last)
    except Exception as e:
        logger.error("[HOOK] install_early_hook FAILED: %s", e)

    try:
        d.get(group_url)
        time.sleep(1.5)
        go_to_date(d, target_date)

        crawl_scroll_loop(
            d,
            group_url=group_url,
            out_path=out_ndjson,
            seen_ids=seen_ids,
            keep_last=keep_last,
            max_scrolls=args.page_limit or 10000,
        )
    finally:
        try:
            d.quit()
        except Exception:
            pass

    if LATEST_CREATED_TS is not None:
        save_checkpoint(checkpoint, LATEST_CREATED_TS)

    logger.info(
        "[DONE] Finished crawl for %s. Total unique posts: %d",
        target_date,
        len(seen_ids),
    )


if __name__ == "__main__":
    main()
