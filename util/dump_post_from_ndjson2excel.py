import json
import pandas as pd
from datetime import datetime
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE  # ⬅️ THÊM DÒNG NÀY

def convert_timestamp(timestamp):
    """Chuyển đổi Unix timestamp sang định dạng datetime"""
    if timestamp:
        try:
            return datetime.fromtimestamp(int(timestamp))
        except:
            return timestamp
    return None

def process_list_field(field_value):
    """Xử lý các trường dạng list, giữ nguyên format ["item","item"]"""
    if isinstance(field_value, list):
        return str(field_value)
    return field_value

def clean_illegal_chars(value):
    """Loại bỏ các ký tự không hợp lệ cho Excel (control chars)."""
    if isinstance(value, str):
        return ILLEGAL_CHARACTERS_RE.sub("", value)
    return value

def ndjson_to_excel(input_file, output_file):
    """
    Đọc file NDJSON và chuyển đổi sang Excel
    
    Args:
        input_file: đường dẫn đến file .ndjson hoặc .jsonl
        output_file: đường dẫn file Excel output (phải có đuôi .xlsx)
    """
    
    # Các trường cần lấy theo thứ tự
    fields = [
        'id', 'type', 'link', 'author_id', 'author', 'author_link', 
        'avatar', 'created_time', 'content', 'image_url', 'like', 
        'comment', 'haha', 'wow', 'sad', 'love', 'angry', 'care', 
        'share', 'hashtag', 'video', 'source_id', 'is_share', 
        'link_share', 'type_share'
    ]
    
    data_list = []
    
    # Đọc file NDJSON
    print(f"Đang đọc file: {input_file}")
    with open(input_file, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            
            try:
                # Parse JSON từ mỗi dòng
                json_obj = json.loads(line)
                
                # Tạo dictionary cho dòng dữ liệu
                row_data = {}
                for field in fields:
                    value = json_obj.get(field, '')

                    # Xử lý các trường dạng list
                    if field in ['image_url', 'hashtag', 'video']:
                        value = process_list_field(value)

                    # (Nếu muốn convert timestamp thì bật dòng này)
                    # if field == 'created_time':
                    #     value = convert_timestamp(value)

                    row_data[field] = value
                
                data_list.append(row_data)
                
            except json.JSONDecodeError as e:
                print(f"Lỗi parse JSON ở dòng {line_num}: {e}")
                continue
    
    # Tạo DataFrame
    df = pd.DataFrame(data_list, columns=fields)
    
    print(f"Đã đọc {len(df)} bản ghi")

    # 🔥 QUAN TRỌNG: làm sạch ký tự illegal trước khi ghi Excel
    # Áp dụng clean_illegal_chars cho toàn bộ DataFrame
    df = df.applymap(clean_illegal_chars)
    
    # Xuất ra Excel
    print(f"Đang ghi vào file: {output_file}")
    df.to_excel(output_file, index=False, engine='openpyxl')
    
    print(f"Hoàn tất! Đã xuất {len(df)} bản ghi vào {output_file}")
    
    return df

# Sử dụng script
if __name__ == "__main__":
    # Thay đổi đường dẫn file của bạn ở đây
    input_file = r"E:\NCS\fb-selenium\database\post\page\nvdai0906_done\ACC_nvdai0906\posts_all.ndjson"  # hoặc "input.jsonl"
    output_file = r"E:\NCS\fb-selenium\database\post\page\nvdai0906.xlsx"
    
    try:
        df = ndjson_to_excel(input_file, output_file)
        
        # In thông tin tổng quan
        print("\n=== THÔNG TIN TỔNG QUAN ===")
        print(f"Tổng số bản ghi: {len(df)}")
        print(f"Các cột: {list(df.columns)}")
        print("\nMẫu 3 dòng đầu tiên:")
        print(df.head(3))
        
    except FileNotFoundError:
        print(f"Không tìm thấy file: {input_file}")
    except Exception as e:
        print(f"Lỗi: {e}")
