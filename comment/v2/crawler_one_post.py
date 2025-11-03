import time,os,sys
from pathlib import Path
from configs import POST_URL
from get_comment_fb_utils import (
                                open_reel_comments_if_present,
                                )
from get_comment_fb_automation import (
                                crawl_comments,
                                hook_graphql,
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
if __name__ == "__main__":
    # d = start_driver(CHROME_PATH, USER_DATA_DIR, PROFILE_NAME, port=REMOTE_PORT, headless=False)
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

    d.get(POST_URL)
    time.sleep(2)
    hook_graphql(d)
    time.sleep(0.5)
    if "reel" in POST_URL:
        open_reel_comments_if_present(d)
    time.sleep(0.8)
    texts = crawl_comments(
        d,
        out_json="comments.ndjson",
        checkpoint_path="checkpoint_comments.json",
        max_pages=None  
    )