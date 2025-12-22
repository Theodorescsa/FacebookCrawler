import json
from pathlib import Path

import pandas as pd


def read_ndjson(path: Path):
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for i, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                # skip line lỗi JSON
                continue

            url = obj.get("url") or ""
            author = obj.get("author") or ""
            comment = obj.get("comment") or ""
            image_path = obj.get("image_path") or ""

            rows.append(
                {
                    "Link bài viết": url,
                    "Người bình luận": author,
                    "Nội dung bình luận": comment,
                    "Đường dẫn ảnh": image_path,
                }
            )
    return rows


def main():
    # ==== chỉnh đường dẫn theo máy bạn ====
    NDJSON_FILE = Path(r"E:\NCS\fb-selenium\comment\v3\evidence_screenshots\results.ndjson")
    OUT_XLSX = Path(r"E:\NCS\fb-selenium\comment\v3\evidence_screenshots\results.xlsx")

    rows = read_ndjson(NDJSON_FILE)
    if not rows:
        print("Không có dữ liệu để xuất (ndjson rỗng hoặc toàn dòng lỗi).")
        return

    df = pd.DataFrame(rows)

    # (Optional) bỏ dòng fail không có ảnh nếu bạn muốn
    # df = df[df["Đường dẫn ảnh"].astype(str).str.strip() != ""]

    # (Optional) bỏ trùng theo 3 trường chính
    df = df.drop_duplicates(subset=["Link bài viết", "Người bình luận", "Nội dung bình luận"], keep="last")

    OUT_XLSX.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(OUT_XLSX, index=False)
    print("✅ Saved:", OUT_XLSX.resolve())


if __name__ == "__main__":
    main()
