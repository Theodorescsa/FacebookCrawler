import json
import os
import time
import subprocess
import socket
from typing import List, Dict, Any, Optional

from selenium import webdriver
from selenium.webdriver.chrome.options import Options


# ----------------------------
# CONFIG CHUNG
# ----------------------------
CHROME_PATH   = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
USER_DATA_DIR = r"E:\NCS\Userdata"       # thư mục user data (chứa các Profile)
REMOTE_PORT   = 9222                     # dùng 1 port, chạy tuần tự từng profile

# Các origin sẽ vào để lấy storage (nếu cần dùng sau)
ORIGINS = [
    "https://www.facebook.com/",
    "https://m.facebook.com/",
    "https://web.facebook.com/",
]

# Domain cookie cần lấy (substring match, không quá khắt khe)
ALLOWED_COOKIE_DOMAINS = [
    ".facebook.com",
    "facebook.com",
    "m.facebook.com",
    "web.facebook.com",
]

# ----------------------------
# DANH SÁCH PROFILE CẦN CHẠY
# => CHỈNH CHO ĐÚNG Ở ĐÂY
# ----------------------------
PROFILES = [
    # {
    #     "profile_name": "Profile 72",
    #     "output_path": r"E:\NCS\fb-selenium\database\facebookaccount\authen_theodorescsa0312@gmail.com\cookies.json",
    # },
    # {
    #     "profile_name": "Profile 5",
    #     "output_path": r"E:\NCS\fb-selenium\database\facebookaccount\authen_dinhthai160@icloud.com\cookies.json",
    # },
    # {
    #     "profile_name": "Profile 10",
    #     "output_path": r"E:\NCS\fb-selenium\database\facebookaccount\authen_0385348933\cookies.json",
    # },
    # {
    #     "profile_name": "Profile 40",
    #     "output_path": r"E:\NCS\fb-selenium\database\facebookaccount\authen_theodorescsa2004@gmail.com\cookies.json",
    # },
    {
        "profile_name": "Profile 8",
        "output_path": r"E:\NCS\fb-selenium\database\facebookaccount\authen_0896691804\cookies.json",
    },
]


# ----------------------------
# Utils
# ----------------------------
def _wait_port(host: str, port: int, timeout: float = 15.0, poll: float = 0.1) -> bool:
    end = time.time() + timeout
    while time.time() < end:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except Exception:
            time.sleep(poll)
    return False


def start_driver(profile_name: str, remote_port: int = REMOTE_PORT) -> webdriver.Chrome:
    """
    Mở Chrome với đúng profile thật bằng remote debugging.
    """
    args = [
        CHROME_PATH,
        f'--remote-debugging-port={remote_port}',
        f'--user-data-dir={USER_DATA_DIR}',
        f'--profile-directory={profile_name}',
        '--no-first-run',
        '--no-default-browser-check',
        '--disable-extensions',
        '--disable-background-networking',
        '--disable-popup-blocking',
        '--disable-default-apps',
        '--disable-infobars',
        '--window-size=1280,900',
    ]
    subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    if not _wait_port('127.0.0.1', remote_port, timeout=20):
        raise RuntimeError(f"Chrome remote debugging port {remote_port} not available.")

    options = Options()
    options.add_experimental_option("debuggerAddress", f"127.0.0.1:{remote_port}")
    driver = webdriver.Chrome(options=options)
    return driver


def filter_cookies(all_cookies: List[Dict[str, Any]],
                   allow_domains: List[str]) -> List[Dict[str, Any]]:
    out = []
    for ck in all_cookies:
        domain = ck.get("domain") or ""
        if any(d in domain for d in allow_domains):
            # Chuẩn hóa một số field để tương thích Network.setCookie
            item = {
                "name": ck.get("name"),
                "value": ck.get("value"),
                "domain": domain,
                "path": ck.get("path", "/"),
                "secure": bool(ck.get("secure", False)),
                "httpOnly": bool(ck.get("httpOnly", False)),
            }
            # SameSite: CDP trả về "Strict"/"Lax"/"None" hoặc None
            ss = ck.get("sameSite") or ck.get("same_site")
            if ss in ("Strict", "Lax", "None"):
                item["sameSite"] = ss
            # expires: giây epoch hoặc None
            expires = ck.get("expires")
            if isinstance(expires, (int, float)) and expires > 0:
                item["expires"] = expires
            out.append(item)
    return out


def dump_storage(driver: webdriver.Chrome, origins: List[str]):
    """
    Trả về:
      local_map: dict origin -> {key:value}
      session_map: dict origin -> {key:value}
    (Hiện chưa dùng trong main, nhưng giữ lại nếu cần xài thêm.)
    """
    local_map: Dict[str, Dict[str, str]] = {}
    session_map: Dict[str, Dict[str, str]] = {}

    for url in origins:
        try:
            driver.get(url)
            time.sleep(0.8)
            # localStorage
            ls = driver.execute_script("""
                const o = {};
                try {
                    for (let i=0; i<localStorage.length; i++){
                        const k = localStorage.key(i);
                        o[k] = localStorage.getItem(k);
                    }
                } catch(e) {}
                return o;
            """)
            # sessionStorage
            ss = driver.execute_script("""
                const o = {};
                try {
                    for (let i=0; i<sessionStorage.length; i++){
                        const k = sessionStorage.key(i);
                        o[k] = sessionStorage.getItem(k);
                    }
                } catch(e) {}
                return o;
            """)
            local_map[url] = ls or {}
            session_map[url] = ss or {}
            print(f"[STORAGE] {url} -> local={len(local_map[url])} keys, session={len(session_map[url])} keys")
        except Exception as e:
            print(f"[WARN] storage dump failed for {url}: {e}")

    return local_map, session_map


def smart_merge_storage(storage_by_origin: Dict[str, Dict[str, str]]) -> Dict[str, str]:
    merged: Dict[str, str] = {}
    priority = [
        "https://www.facebook.com/",
        "https://web.facebook.com/",
        "https://m.facebook.com/",
    ]
    # build ordered origins list
    ordered = [o for o in priority if o in storage_by_origin] + [o for o in storage_by_origin if o not in priority]
    for origin in ordered:
        for k, v in (storage_by_origin.get(origin) or {}).items():
            if k not in merged:
                merged[k] = v
    return merged


def export_cookies_for_profile(profile_name: str, output_path: str):
    print(f"\n==============================")
    print(f"[PROFILE] Đang xử lý: {profile_name}")
    print(f"[OUTPUT ] {output_path}")
    print(f"==============================")

    driver = None
    try:
        driver = start_driver(profile_name)
        driver.get("https://www.facebook.com/")
        time.sleep(3) # Chờ load ổn định

        # 1. Lấy User-Agent hiện tại của Profile
        user_agent = driver.execute_script("return navigator.userAgent;")
        print(f"[UA] {user_agent}")

        # 2. Lấy Cookies
        driver.execute_cdp_cmd("Network.enable", {})
        res = driver.execute_cdp_cmd("Network.getAllCookies", {})
        all_cookies = res.get("cookies", []) if isinstance(res, dict) else []
        fb_cookies = filter_cookies(all_cookies, ALLOWED_COOKIE_DOMAINS)
        print(f"[COOKIES] total={len(all_cookies)}, selected={len(fb_cookies)}")

        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # 3. Ghi file: Lưu cả Cookie và User-Agent
        # Nên đổi cấu trúc JSON output để chứa cả 2 thông tin
        data_export = {
            "user_agent": user_agent,
            "cookies": fb_cookies
        }
        
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data_export, f, ensure_ascii=False, indent=2)
            
        print(f"[WRITE] {output_path}")

    except Exception as e:
        print(f"[ERROR] Lỗi khi xử lý profile {profile_name}: {e}")
    finally:
        if driver is not None:
            # Quan trọng: Quit driver để đóng session phía trình duyệt này
            driver.quit()
            time.sleep(2)

    print(f"[DONE] Exported cookies for profile: {profile_name}")


def main():
    for idx, cfg in enumerate(PROFILES, start=1):
        profile_name = cfg["profile_name"]
        output_path  = cfg["output_path"]
        print(f"\n===== ({idx}/{len(PROFILES)}) BẮT ĐẦU PROFILE {profile_name} =====")
        export_cookies_for_profile(profile_name, output_path)
    print("\n[TOTAL DONE] Đã xử lý xong tất cả profile trong PROFILES.")


if __name__ == "__main__":
    main()
