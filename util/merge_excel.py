import pandas as pd

# Danh sách file cần gộp
INPUT_FILES = [
    r"E:\NCS\fb-selenium\database\comment\thoibaode_part1.xlsx",
    r"E:\NCS\fb-selenium\database\comment\thoibaode_part2.xlsx",
    r"E:\NCS\fb-selenium\database\comment\thoibaode_part3.xlsx",
    r"E:\NCS\fb-selenium\database\comment\thoibaode_part4.xlsx",
    r"E:\NCS\fb-selenium\database\comment\thoibaode_part5.xlsx",
    r"E:\NCS\fb-selenium\database\comment\thoibaode_part6.xlsx",
    # thêm các file khác vào đây nếu có
]

OUTPUT_FILE = r"E:\NCS\fb-selenium\database\comment\thoibaode.xlsx"

def merge_excels(input_files, output_file):
    frames = []

    for path in input_files:
        print(f"[READ] {path}")
        # mặc định đọc sheet đầu tiên, nếu cần sheet khác thì thêm sheet_name="..."
        df = pd.read_excel(path)
        frames.append(df)

    # Gộp theo chiều dọc (append các dòng)
    merged = pd.concat(frames, ignore_index=True)

    print(f"[WRITE] {output_file}")
    merged.to_excel(output_file, index=False)
    print("[OK] Done.")

if __name__ == "__main__":
    merge_excels(INPUT_FILES, OUTPUT_FILE)
