# main_batch.py

import sys, re, urllib
from pathlib import Path

import pandas as pd  # pip install pandas

from crawl_comments import crawl_comments_for_post
from logs.loging_config import logger

if len(sys.argv) < 2:
    print("Usage: python main_batch.py <excel_path>")
    sys.exit(1)

EXCEL_PATH = Path(sys.argv[1]).resolve()

# Cookies login FB
COOKIES_PATH = r"E:\NCS\fb-selenium\database\facebookaccount\authen_tranhoangdinhnam\cookies.json"

# Base dir cố định
BASE_DIR = Path("database") / "comment" / "page"

FANPAGE_NAME = EXCEL_PATH.stem 

OUT_DIR = (BASE_DIR / FANPAGE_NAME).resolve()
OUT_DIR.mkdir(parents=True, exist_ok=True)


def safe_filename_from_url(url: str, max_len: int = 100) -> str:
    """
    Biến URL thành tên file an toàn, không chứa ký tự cấm, giới hạn độ dài.
    Ví dụ:
      https://www.facebook.com/.../posts/12345?__cft__[...] -> 
      https_www_facebook_com_..._posts_12345
    """
    parsed = urllib.parse.urlparse(url)

    # bỏ query + fragment cho đỡ rác
    base = parsed._replace(query="", fragment="").geturl()

    # thay mọi thứ không phải chữ/số bằng "_"
    name = re.sub(r"[^A-Za-z0-9]+", "_", base).strip("_")

    # cắt ngắn cho chắc (NTFS/EXT4 ko thích tên quá dài)
    if len(name) > max_len:
        name = name[-max_len:]  # lấy đoạn cuối (chứa post id / pfbid)

    return name or "post"

if __name__ == "__main__":
    # Đọc excel
    try:
        df = pd.read_excel(EXCEL_PATH)
    except Exception as e:
        print(f"[ERROR] Không đọc được file Excel: {EXCEL_PATH} - {e}")
        sys.exit(1)

    if "link" not in df.columns:
        print(f"[ERROR] File Excel {EXCEL_PATH} không có cột 'link'")
        sys.exit(1)

    links = df["link"].dropna().astype(str).tolist()
    logger.info(
        "[BATCH] Excel: %s | Fanpage folder: %s | Tổng link: %s",
        EXCEL_PATH,
        OUT_DIR,
        len(links),
    )
    for idx, url in enumerate(links, start=1):
        fname = safe_filename_from_url(url) + ".ndjson"
        out_path = OUT_DIR / fname

        logger.info(
            "[BATCH] (%s/%s) Crawl link: %s -> %s",
            idx,
            len(links),
            url,
            out_path,
        )

        try:
            rows = crawl_comments_for_post(
                page_url=url,
                cookies_path=COOKIES_PATH,
                max_rounds=200,
                sleep_between_rounds=1.5,
                headless=False,
                out_path=str(out_path),
            )
            logger.info(
                "[BATCH] Done file=%s, unique comments+replies=%s",
                fname,
                len(rows),
            )
        except Exception as e:
            logger.exception("[BATCH] Lỗi khi crawl %s: %s", url, e)
            continue

    logger.info("[BATCH] Hoàn thành crawl cho file Excel: %s", EXCEL_PATH)
