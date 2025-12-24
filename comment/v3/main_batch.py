# comment\v3\main_batch.py

import sys, re, urllib
from pathlib import Path

import pandas as pd
import argparse

from crawl_comments import crawl_comments_for_post
from logs.loging_config import logger

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


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


def parse_args():
    parser = argparse.ArgumentParser(description="Batch crawl comments cho danh sách link trong Excel.")

    parser.add_argument(
        "--page-name",
        dest="page_name",
        default="thoibaode",
        help="Tên fanpage (dùng làm tên folder output).",
    )
    parser.add_argument(
        "--input-excel",
        dest="input_excel",
        required=True,
        help="Đường dẫn file Excel chứa cột 'link'.",
    )
    parser.add_argument(
        "--cookies-path",
        dest="cookies_path",
        required=True,
        help="Đường dẫn file cookies.json để login Facebook.",
    )
    parser.add_argument(
        "--data-root",
        dest="data_root",
        default="/app/database",
        help="Thư mục gốc lưu dữ liệu (mặc định /app/database).",
    )

    # --headless / --no-headless
    parser.add_argument(
        "--headless",
        dest="headless",
        action="store_true",
        help="Chạy headless (mặc định).",
    )
    parser.add_argument(
        "--no-headless",
        dest="headless",
        action="store_false",
        help="Tắt headless để debug.",
    )
    parser.set_defaults(headless=True)

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    EXCEL_PATH = args.input_excel
    COOKIES_PATH = args.cookies_path
    FANPAGE_NAME = args.page_name

    DATA_ROOT = Path(args.data_root)
    BASE_DIR = DATA_ROOT / "comment" / "page"
    OUT_DIR = (BASE_DIR / FANPAGE_NAME).resolve()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    logger.info(
        "[BATCH] START | EXCEL=%s | COOKIES=%s | OUT_DIR=%s | PAGE=%s | DATA_ROOT=%s | HEADLESS=%s",
        EXCEL_PATH,
        COOKIES_PATH,
        OUT_DIR,
        FANPAGE_NAME,
        DATA_ROOT,
        args.headless,
    )

    # Đọc excel
    try:
        df = pd.read_excel(EXCEL_PATH)
    except Exception as e:
        logger.exception("[ERROR] Không đọc được file Excel: %s - %s", EXCEL_PATH, e)
        sys.exit(1)

    if "link" not in df.columns:
        logger.error("[ERROR] File Excel %s không có cột 'link'", EXCEL_PATH)
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
                headless=args.headless,
                out_path=str(out_path),
            )
            if rows == []:
                logger.warning("[BATCH] Không tìm thấy bài viết: %s", url)
            logger.info(
                "[BATCH] Done file=%s, unique comments+replies=%s",
                fname,
                len(rows),
            )
        except Exception as e:
            logger.exception("[BATCH] Lỗi khi crawl %s: %s", url, e)
            continue

    logger.info("[BATCH] Hoàn thành crawl cho file Excel: %s", EXCEL_PATH)
