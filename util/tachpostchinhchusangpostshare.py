import pandas as pd

file_path = r"E:\NCS\fb-selenium\a_done\nvdai0906-100044544594726.xlsx"
sheet_name = "Post"
target_name = "Nguyễn Văn Đài"
new_sheet = "Post Share"

# Đọc file
df = pd.read_excel(file_path, sheet_name=sheet_name)

# 1) Lọc ra các dòng KHÔNG phải Nguyễn Văn Đài → để đưa sang sheet khác
df_not_dai = df[df["author"] != target_name]

# 2) Lọc lại sheet gốc chỉ giữ dòng của Nguyễn Văn Đài
df_only_dai = df[df["author"] == target_name]

# 3) Ghi lại vào file (ghi đè sheet gốc + tạo sheet mới)
with pd.ExcelWriter(file_path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
    # Ghi sheet gốc sau khi XOÁ các hàng không cần
    df_only_dai.to_excel(writer, sheet_name=sheet_name, index=False)

    # Ghi sheet mới chứa các dòng không phải Nguyễn Văn Đài
    df_not_dai.to_excel(writer, sheet_name=new_sheet, index=False)

print("✅ Done! Đã xoá các dòng không phải Nguyễn Văn Đài khỏi sheet cũ và chuyển sang sheet mới.")
