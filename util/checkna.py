import pandas as pd

# Đọc 2 file Excel
df1 = pd.read_excel("file1.xlsx")  # có facebook_link, comment_content
df2 = pd.read_excel("file2.xlsx")  # có Link bài viết, Nội dung bình luận

# Tạo dict mapping từ file 1
mapping = dict(zip(df1["facebook_link"], df1["comment_content"]))

# Chỉ bổ sung cho những dòng đang bị thiếu nội dung bình luận
df2["Nội dung bình luận"] = df2["Nội dung bình luận"].fillna(
    df2["Link bài viết"].map(mapping)
)

# Lưu lại file mới
df2.to_excel("file2_da_bo_sung.xlsx", index=False)

print("✅ Đã bổ sung xong Nội dung bình luận")
