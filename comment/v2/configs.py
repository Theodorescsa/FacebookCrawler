import os
from pathlib import Path

DATABASE_PATH = Path(__file__).resolve().parent.parent.parent / "database"

# ========================= CONFIG FOR MAIN =========================
CHROME_PATH   = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
USER_DATA_DIR = r"E:\NCS\Userdata"
PROFILE_NAME  = "Profile 5"
POST_URL      = "https://www.facebook.com/reel/1528393574968999/?rdid=ZfoaJKOZshTMvF1C&share_url=https%3A%2F%2Fwww.facebook.com%2Fshare%2Fr%2F1ByqCxiEDF%2F#"
OUT_FILE      = "comments_batch1.json"
REMOTE_PORT  = 9222
REPLY_DOC_ID = "25396268633304296"  
CURSOR_KEYS = {"end_cursor","endCursor","after","afterCursor","commentsAfterCursor","feedAfterCursor","cursor"}
RAW_DUMS = DATABASE_PATH / "comment" / "page" / "thoibaode" / "sheet3" / "raw_dump_comments"
# ========================= CONFIG crawler from ndjson (điều chỉnh theo máy bạn) =========================
INPUT_EXCEL = DATABASE_PATH / "post" / "page" / "thoibaode" / "thoibao-de-last-split-sheet3.xlsx"
SHEET_NAME = "Sheet_3"
OUTPUT_NDJSON_DIR = DATABASE_PATH / "comment" / "page" / "thoibaode" / "sheet3" / "ndjson_per_post"
ERROR_EXCEL       = DATABASE_PATH / "comment" / "page" / "thoibaode" / "sheet3" / "crawl_errors-sheet3.xlsx"  # để log lỗi
STATUS_STORE_PATH = DATABASE_PATH / "comment" / "page" / "thoibaode" / "sheet3" / "status_store_sheet3.json"
TMP_DIR = DATABASE_PATH / "comment" / "page" / "thoibaode" / "sheet3" / "tmp_comments_sheet3"
DEDUP_CACHE_PATH = DATABASE_PATH / "comment" / "page" / "thoibaode" / "sheet3" / "reply_dedup_cache_sheet3.json"
os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(OUTPUT_NDJSON_DIR, exist_ok=True)
os.makedirs(RAW_DUMS, exist_ok=True)