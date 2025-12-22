import pandas as pd
import numpy as np # Import thêm numpy để dùng NaN nếu cần

# Đọc file
try:
    df1 = pd.read_excel(r"E:\NCS\fb-selenium\comment\v3\evidence_screenshots\results.xlsx")
    df2 = pd.read_excel(r"E:\NCS\fb-selenium\util\output_links.xlsx")
except Exception as e:
    print(f"Lỗi đọc file: {e}")
    exit()

# Chuẩn hoá link (tránh lỗi khoảng trắng)
if "Link bài viết" in df1.columns:
    df1["Link bài viết"] = df1["Link bài viết"].astype(str).str.strip()
else:
    print("❌ Lỗi: File 1 không có cột 'Link bài viết'")
    exit()
    
if "facebook_link" in df2.columns:
    df2["facebook_link"] = df2["facebook_link"].astype(str).str.strip()
else:
    print("❌ Lỗi: File 2 không có cột 'facebook_link'")
    exit()

# Merge dữ liệu
print("Đang ghép dữ liệu...")
merged = df1.merge(
    df2[["facebook_link", "comment_content", "author_comment", "author_id"]],
    how="left",
    left_on="Link bài viết",
    right_on="facebook_link"
)

# --- HÀM XỬ LÝ ĐIỀN DỮ LIỆU ---
def fill_missing_column(df, target_col, source_col):
    # 1. Nếu cột đích chưa có trong file 1, tạo mới cột đó
    if target_col not in df.columns:
        print(f"ℹ️ Cột '{target_col}' chưa có, đang tạo mới...")
        df[target_col] = pd.NA # Hoặc để "" tuỳ nhu cầu

    # 2. Tạo mask để tìm những ô đang trống
    # Chuyển về string, strip khoảng trắng, so sánh với "" hoặc check NA
    mask = df[target_col].isna() | (df[target_col].astype(str).str.strip() == "") | (df[target_col].astype(str).str.lower() == "nan")
    
    # 3. Điền dữ liệu từ cột nguồn (source_col) vào các ô trống
    df.loc[mask, target_col] = df.loc[mask, source_col]
    return df

# --- THỰC HIỆN CẬP NHẬT ---

# 1. Cập nhật Nội dung bình luận
merged = fill_missing_column(merged, "Nội dung bình luận", "comment_content")

# 2. Cập nhật Người bình luận
merged = fill_missing_column(merged, "Người bình luận", "author_comment")

# 3. Cập nhật Id người bình luận
merged = fill_missing_column(merged, "Id người bình luận", "author_id")

# Xoá các cột phụ
merged.drop(columns=["facebook_link", "comment_content", "author_comment", "author_id"], inplace=True)

# Ghi lại file
output_path = "file1_updated_full.xlsx"
merged.to_excel(output_path, index=False)

print(f"✅ Đã cập nhật xong. Kiểm tra file: {output_path}")