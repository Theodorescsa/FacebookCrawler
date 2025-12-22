import pandas as pd

# Đọc file Excel
df = pd.read_excel(r"E:\NCS\fb-selenium\util\output_links.xlsx")  # đổi tên file nếu cần

# Điều kiện lọc
mask_videos = df["facebook_link"].str.contains("videos", case=False, na=False)

# Sheet mới: chỉ chứa link videos
df_videos = df[mask_videos]

# Sheet cũ: loại bỏ các dòng videos
df_remaining = df[~mask_videos]

# Ghi ra Excel
with pd.ExcelWriter("output.xlsx", engine="openpyxl") as writer:
    df_remaining.to_excel(writer, sheet_name="original_data", index=False)
    df_videos.to_excel(writer, sheet_name="videos_only", index=False)

print("✅ Đã chuyển dòng sang sheet khác và xóa khỏi sheet cũ")
