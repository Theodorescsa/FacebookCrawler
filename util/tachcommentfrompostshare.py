import pandas as pd

file_path = r"E:\NCS\fb-selenium\database\comment\thoibaode.xlsx"

SHEET_COMMENT = "Comment"
SHEET_POST_SHARE = "Post Share"
SHEET_COMMENT_SHARE = "Comment Share"

# Đọc 2 sheet một lần cho nhanh
dfs = pd.read_excel(
    file_path,
    sheet_name=[SHEET_COMMENT, SHEET_POST_SHARE]
)

df_comment = dfs[SHEET_COMMENT]
df_post_share = dfs[SHEET_POST_SHARE]

# Chuẩn hóa kiểu string + strip cho chắc
df_post_share["link"] = df_post_share["link"].astype(str).str.strip()
df_comment["postlink"] = df_comment["postlink"].astype(str).str.strip()

# Tập hợp tất cả link bài post cần lấy comment
post_links_set = set(df_post_share["link"].dropna().tolist())

# Mask: comment nào thuộc các post trong Post Share
mask_share = df_comment["postlink"].isin(post_links_set)

# Các comment thuộc các bài viết trong Post Share
df_comment_share = df_comment[mask_share]

# Các comment còn lại (không thuộc Post Share) – nếu bạn muốn giữ lại ở sheet Comment
df_comment_remain = df_comment[~mask_share]

# Ghi lại file Excel:
# - Ghi đè sheet "Comment" = các comment còn lại
# - Tạo/ghi đè sheet "Comment Share" = comment thuộc Post Share
with pd.ExcelWriter(file_path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
    df_comment_remain.to_excel(writer, sheet_name=SHEET_COMMENT, index=False)
    df_comment_share.to_excel(writer, sheet_name=SHEET_COMMENT_SHARE, index=False)

print("✅ Done! Đã tách comment thuộc các bài trong 'Post Share' sang sheet 'Comment Share'.")
