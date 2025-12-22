import pandas as pd

# ===== CONFIG =====
INPUT_FILE = r"E:\NCS\fb-selenium\util\final2.xlsx"
OUTPUT_FILE = "output_links.xlsx"

LINK_COMMENT_COL = "Bài viết : comment"
AUTHOR_COL = "author"   # <- đổi nếu tên cột khác (vd: "Author", "author_comment", ...)
AUTHOR_ID_COL = "author_id"
# ==================

df = pd.read_excel(INPUT_FILE)

rows = []

for _, r in df.iterrows():
    cell = r.get(LINK_COMMENT_COL)
    if pd.isna(cell):
        continue

    author = r.get(AUTHOR_COL, "")
    
    author = "" if pd.isna(author) else str(author).strip()
    author_id = r.get(AUTHOR_ID_COL, "")
    author_id = "" if pd.isna(author_id) else str(author_id).strip()
    cell = str(cell)
    parts = [p.strip() for p in cell.split("|") if p.strip()]

    for p in parts:
        if "facebook.com" not in p:
            continue

        # format thường: <link> : <comment>
        if " : " in p:
            link, comment = p.split(" : ", 1)
        elif ":" in p:
            link, comment = p.split(":", 1)
        else:
            link, comment = p, ""

        rows.append({
            "facebook_link": link.strip(),
            "author_id": author_id,
            "author_comment": author,
            "comment_content": comment.strip()
        })

df_comments = pd.DataFrame(rows)

# unique links
df_links = pd.DataFrame({"facebook_link": df_comments["facebook_link"].dropna().unique()})

# (optional) bỏ trùng comment theo (link, author, comment)
# df_comments = df_comments.drop_duplicates(subset=["facebook_link", "author_comment", "comment_content"])

with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
    df_links.to_excel(writer, sheet_name="links", index=False)
    df_comments.to_excel(writer, sheet_name="comment_content", index=False)

print(f"✅ Done! {len(df_links)} unique links | {len(df_comments)} comment rows → {OUTPUT_FILE}")
