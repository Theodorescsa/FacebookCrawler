import os
from pathlib import Path

DATABASE_PATH = Path(__file__).resolve().parent.parent.parent / "database"

# ========================= CONFIG FOR MAIN =========================
CHROME_PATH   = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
USER_DATA_DIR = r"E:\NCS\Userdata"
PROFILE_NAME  = "Profile 5"
POST_URL      = "https://www.facebook.com/tranbobg79/posts/pfbid0bcx41ydRjYi55EmGTjYUnnvckzxqAvmmpsBFQdxAVdKiyrvLgh87tyAyjPGk87KUl?rdid=x4mrINHWSbMQi2Mq#"
OUT_FILE      = "comments_batch1.json"
REMOTE_PORT  = 9222
REPLY_DOC_ID = "25435536076040042"  
CURSOR_KEYS = {"end_cursor","endCursor","after","afterCursor","commentsAfterCursor","feedAfterCursor","cursor"}
# ========================= CONFIG crawler from ndjson (điều chỉnh theo máy bạn) =========================
