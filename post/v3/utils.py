import json, re
from typing import List, Optional
from .config import *
from urllib.parse import urlparse, urlunparse, parse_qs

def _norm_link(u: str) -> Optional[str]:
    if not u or not isinstance(u, str):
        return None
    try:
        p = urlparse(u)
        host = p.netloc.lower()
        if host.endswith("facebook.com"): host = "facebook.com"
        path = (p.path or "").rstrip("/")
        if re.search(r"/(?:reel|posts|permalink)/\d+$", path.lower()):
            return urlunparse(("https", host, path.lower(), "", "", ""))
        return None
    except Exception:
        return None

def extract_id_from_url(url: str) -> str:
    """
    Biến đổi URL thành một định danh an toàn cho tên file/folder.
    VD: https://www.facebook.com/le.t.khoa.1?ref=... -> le.t.khoa.1
    VD: https://www.facebook.com/profile.php?id=1000798 -> 1000798
    """
    try:
        parsed = urlparse(url)
        
        # Trường hợp 1: URL dạng profile.php?id=...
        query = parse_qs(parsed.query)
        if 'id' in query:
            return query['id'][0]
        
        # Trường hợp 2: URL dạng /username hoặc /groups/groupID
        path_parts = [p for p in parsed.path.split('/') if p]
        
        if not path_parts:
            return "unknown"
            
        # Lấy phần cuối cùng của path (thường là username)
        candidate = path_parts[-1]
        
        # Nếu URL có dạng /groups/12345/ thì lấy 12345
        if candidate in ['posts', 'videos', 'about'] and len(path_parts) > 1:
            candidate = path_parts[-2]
            
        # Làm sạch chuỗi (chỉ giữ lại ký tự an toàn cho tên folder)
        # Loại bỏ các ký tự không phải chữ, số, dấu chấm, gạch dưới
        clean_name = re.sub(r'[^a-zA-Z0-9._-]', '', candidate)
        
        return clean_name if clean_name else "unknown"
    except Exception:
        return "unknown_id"