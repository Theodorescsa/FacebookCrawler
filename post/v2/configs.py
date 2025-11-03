# =========================
# CONFIG — chỉnh theo máy bạn
# =========================
from pathlib import Path
import re, os
HERE = Path(__file__).resolve().parent
DATABASE_PATH = Path(__file__).resolve().parent.parent.parent / "database"

# Page/Group/Profile gốc bạn muốn crawl
GROUP_URL     = "https://www.facebook.com/thoibao.de"

# (Optional) Nếu muốn nạp login thủ công từ file, set path 2 hằng dưới; nếu không, để None:
COOKIES_PATH         = DATABASE_PATH / "facebookaccount" / "authen" / "cookies.json"
LOCALSTORAGE_PATH    = DATABASE_PATH / "facebookaccount" / "authen" / "localstorage.json"
SESSIONSTORAGE_PATH  = DATABASE_PATH / "facebookaccount" / "authen" / "sessionstorage.json"

# Proxy tuỳ chọn cho selenium-wire (để trống nếu không dùng)
PROXY_URL = ""
# Cookie
ALLOWED_COOKIE_DOMAINS = {".facebook.com", "facebook.com", "m.facebook.com", "web.facebook.com"}

# Lưu trữ
KEEP_LAST     = 350
OUT_NDJSON    = DATABASE_PATH / "post" /  "page" /  "thoibaode" / "posts_all.ndjson"
RAW_DUMPS_DIR = DATABASE_PATH / "post" /  "page" /  "thoibaode" / "raw_dump_posts"
CHECKPOINT    = DATABASE_PATH / "post" /  "page" /  "thoibaode" / "checkpoint.json"
os.makedirs(RAW_DUMPS_DIR, exist_ok=True)
# Cursor
CURSOR_KEYS = {"end_cursor","endCursor","after","afterCursor","feedAfterCursor","cursor"}

POST_URL_RE = re.compile(
    r"""https?://(?:web\.)?facebook\.com/
        (?:
            groups/[^/]+/(?:permalink|posts)/\d+
          | [A-Za-z0-9.\-]+/posts/\d+
          | [A-Za-z0-9.\-]+/reel/\d+
          | photo(?:\.php)?\?(?:.*(?:fbid|story_fbid|video_id)=\d+)
          | .*?/pfbid[A-Za-z0-9]+
        )
    """, re.I | re.X
)