import json
import os
from pathlib import Path


def split_ndjson(input_file: Path, output_dir: Path):
    # Kiểm tra nếu thư mục đích không tồn tại, tạo mới
    output_dir.mkdir(parents=True, exist_ok=True)

    # Đọc file NDJSON gốc
    with open(input_file, "r", encoding="utf-8") as f:
        for line in f:
            try:
                # Parse dòng JSON
                obj = json.loads(line.strip())
                uid = obj.get("uid")
                if not uid:
                    print(f"[WARN] Dòng không có UID, bỏ qua: {line}")
                    continue

                # Tạo thư mục cho mỗi UID nếu chưa có
                uid_dir = output_dir / f"uid_{uid}"
                uid_dir.mkdir(parents=True, exist_ok=True)

                # Tạo file .ndjson cho mỗi UID
                uid_file = uid_dir / f"uid_{uid}.ndjson"
                with open(uid_file, "a", encoding="utf-8") as uid_f:
                    uid_f.write(json.dumps(obj, ensure_ascii=False) + "\n")
            except json.JSONDecodeError as e:
                print(f"[ERROR] Lỗi khi đọc dòng: {e}")
            except Exception as e:
                print(f"[ERROR] Lỗi không xác định: {e}")


def main():
    # Đường dẫn file NDJSON gốc và thư mục xuất
    input_file = Path("results.ndjson")  # Chỉnh đường dẫn nếu cần
    output_dir = Path("")

    # Chạy hàm chia nhỏ NDJSON
    split_ndjson(input_file, output_dir)

    print(f"✅ Đã chia NDJSON thành các file nhỏ theo UID tại {output_dir}")


if __name__ == "__main__":
    main()
