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
# ========================= CONFIG crawler from ndjson (điều chỉnh theo máy bạn) =========================
