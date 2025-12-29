# comment/v3/main_batch.py

import sys
import re
import urllib.parse
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
    Biến URL thành tên file an toàn.
    """
    try:
        parsed = urllib.parse.urlparse(url)
        # bỏ query + fragment
        base = parsed._replace(query="", fragment="").geturl()
        # thay ký tự lạ bằng "_"
        name = re.sub(r"[^A-Za-z0-9]+", "_", base).strip("_")
        # cắt ngắn
        if len(name) > max_len:
            name = name[-max_len:]
        return name or "post"
    except Exception:
        return "unknown_post"


def parse_args():
    parser = argparse.ArgumentParser(description="Batch crawl comments using MoreLogin.")

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
    # --- THAY ĐỔI: Dùng profile_id thay vì cookies_path ---
    parser.add_argument(
        "--profile-id",
        dest="profile_id",
        required=True,
        help="ID của Profile trong MoreLogin (VD: 12345).",
    )
    parser.add_argument(
        "--data-root",
        dest="data_root",
        default="/app/database",
        help="Thư mục gốc lưu dữ liệu.",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    EXCEL_PATH = args.input_excel
    PROFILE_ID = args.profile_id  # Lấy Profile ID từ tham số
    FANPAGE_NAME = args.page_name

    DATA_ROOT = Path(args.data_root)
    # Cấu trúc folder: data_root/comment/page/fanpage_name
    BASE_DIR = DATA_ROOT / "comment" / "page"
    OUT_DIR = (BASE_DIR / FANPAGE_NAME).resolve()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    logger.info(
        "[BATCH] START | EXCEL=%s | PROFILE_ID=%s | OUT_DIR=%s | PAGE=%s",
        EXCEL_PATH,
        PROFILE_ID,
        OUT_DIR,
        FANPAGE_NAME,
    )

    # Đọc Excel
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
        "[BATCH] Excel: %s | Tổng link cần crawl: %s",
        EXCEL_PATH,
        len(links),
    )

    # Duyệt từng link và crawl
    for idx, url in enumerate(links, start=1):
        fname = safe_filename_from_url(url) + ".ndjson"
        out_path = OUT_DIR / fname

        logger.info(
            "[BATCH] (%s/%s) Crawling: %s -> %s",
            idx,
            len(links),
            url,
            out_path,
        )

        try:
            # Gọi hàm crawl đã update MoreLogin
            # Lưu ý: Hàm này sẽ tự mở profile -> crawl -> đóng profile cho TỪNG LINK.
            # Điều này an toàn để tránh lỗi state giữa các lần crawl.
            rows = crawl_comments_for_post(
                page_url=url,
                profile_id=PROFILE_ID,
                max_rounds=200,
                sleep_between_rounds=1.5,
                out_path=str(out_path),
            )

            if not rows:
                logger.warning("[BATCH] Không lấy được comment hoặc lỗi (rows empty): %s", url)
            else:
                logger.info(
                    "[BATCH] Done file=%s, items=%s",
                    fname,
                    len(rows),
                )

        except Exception as e:
            logger.exception("[BATCH] Lỗi ngoại lệ khi crawl %s: %s", url, e)
            continue

    logger.info("[BATCH] Hoàn thành toàn bộ batch job.")