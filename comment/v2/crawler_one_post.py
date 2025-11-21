import time,os,sys
from pathlib import Path
from configs import POST_URL
from get_comment_fb_utils import (
                                open_reel_comments_if_present,
                                )
from get_comment_fb_automation import (
                                RateLimitError,
                                crawl_comments,
                                hook_graphql,
                                install_early_hook,
                                )

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
from logs.loging_config import logger
from util.startdriverproxy import bootstrap_auth,start_driver_with_proxy

os.makedirs("raw_dumps", exist_ok=True)
# =========================
# MAIN
# =========================
COOKIES_PATH = r"E:\NCS\fb-selenium\database\facebookaccount\authen_tranhoangdinhnam\cookies.json"
def make_driver():
    d = start_driver_with_proxy(
        proxy_host="",
        proxy_port="",
        proxy_user="",
        proxy_pass="",
        mitm_port=8899,
        headless=False
    )
    d.set_script_timeout(40)

    install_early_hook(d)

    try:
        d.execute_cdp_cmd("Network.enable", {})
        d.execute_cdp_cmd("Network.setCacheDisabled", {"cacheDisabled": True})
    except Exception:
        pass

    bootstrap_auth(d, COOKIES_PATH)

    d.get(POST_URL)
    time.sleep(2)

    hook_graphql(d)
    try:
        d.execute_script("window.__gqlReqs = window.__gqlReqs || []; window.__gqlReqs.length = 0;")
    except Exception:
        pass
    return d

if __name__ == "__main__":
    checkpoint_path = "checkpoint_comments.json"
    raw_dump_path = "raw_dumps"
    out_json = "comments.ndjson"


    texts = []

    while True:
        d = make_driver()
        try:
            texts = crawl_comments(
                d,
                raw_dump_path=raw_dump_path,
                out_json=out_json,
                checkpoint_path=checkpoint_path,
                max_pages=None
            )
            d.quit()
            break   # done ok
        except RateLimitError as e:
            logger.warning(f"[MAIN] Rate limited (429). . Restarting driver…")
            d.quit()
            continue
        except Exception as e:
            logger.exception("[MAIN] Unexpected error")
            d.quit()
            break

    logger.info(f"[MAIN] Finished with {len(texts)} comments.")
