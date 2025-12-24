# startdriverproxy.py (PATCHED)
from pathlib import Path
from typing import List, Dict, Any, Optional
import json, time, os
import sys
import shutil
import subprocess
import threading
import socket
import signal
from urllib.parse import urlparse
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import logging
# ------------------ CONFIG ------------------
ALLOWED_COOKIE_DOMAINS = {".facebook.com", "facebook.com", "m.facebook.com", "web.facebook.com"}
COOKIES_PATH         = r"E:\NCS\fb-selenium\database\facebookaccount\authen_tranhoangdinhnam\cookies.json"
MITM_PORT = 8899
MITM_WAIT_TIMEOUT = 12.0
# --------------------------------------------

def _coerce_epoch(v):
    try:
        vv = float(v)
        if vv > 10_000_000_000:  # ms -> s
            vv = vv / 1000.0
        return int(vv)
    except Exception:
        return None

def _normalize_cookie(c: dict) -> Optional[dict]:
    if not isinstance(c, dict):
        return None
    name  = c.get("name")
    value = c.get("value")
    if not name or value is None:
        return None

    domain = c.get("domain")
    host_only = c.get("hostOnly", False)
    if domain:
        domain = domain.strip()
        if host_only and domain.startswith("."):
            domain = domain.lstrip(".")
    if not domain:
        domain = "facebook.com"

    if not any(domain.endswith(d) or ("."+domain).endswith(d) for d in ALLOWED_COOKIE_DOMAINS):
        return None

    path = c.get("path") or "/"
    secure    = bool(c.get("secure", True))
    httpOnly  = bool(c.get("httpOnly", c.get("httponly", False)))

    expiry = c.get("expiry", None)
    if expiry is None:
        expiry = c.get("expirationDate", None)
    if expiry is None:
        expiry = c.get("expires", None)
    expiry = _coerce_epoch(expiry) if expiry is not None else None

    out = {
        "name": name,
        "value": value,
        "domain": domain,
        "path": path,
        "secure": secure,
        "httpOnly": httpOnly,
    }
    if expiry is not None:
        out["expiry"] = expiry
    return out

# ----------------------------
# CẬP NHẬT HÀM XỬ LÝ COOKIE & AUTH
# ----------------------------
def check_login_visual(driver) -> bool:
    """
    Kiểm tra giao diện xem có hiện form đăng nhập hay không.
    Trả về: True (Đã đăng nhập OK), False (Chưa đăng nhập/Bị hiện form)
    """
    try:
        # 1. Check cái Popup chặn màn hình (Dựa vào HTML bạn gửi: id="login_popup_cta_form")
        popups = driver.find_elements(By.ID, "login_popup_cta_form")
        if len(popups) > 0:
            print("[CHECK] Phát hiện Popup bắt đăng nhập (login_popup_cta_form).")
            return False # Chưa đăng nhập

        # 2. Check form đăng nhập thường (trang login full màn hình)
        email_inputs = driver.find_elements(By.NAME, "email")
        pass_inputs = driver.find_elements(By.NAME, "pass")
        
        # Nếu thấy cả ô email và ô pass -> Chắc chắn đang ở màn hình login
        if len(email_inputs) > 0 and len(pass_inputs) > 0:
             # Kiểm tra thêm: đôi khi ô input bị ẩn, chỉ tính khi nó hiển thị
            if email_inputs[0].is_displayed():
                print("[CHECK] Phát hiện ô nhập Email/Pass.")
                return False # Chưa đăng nhập

        # 3. (Optional) Check nút "Đăng nhập"
        # login_buttons = driver.find_elements(By.XPATH, "//div[@aria-label='Đăng nhập vào Facebook']")
        
        # Nếu không tìm thấy dấu hiệu login form -> Tạm coi là OK
        return True

    except Exception as e:
        print(f"[WARN] Lỗi khi check visual: {e}")
        # Nếu lỗi check element, ta tạm tin vào cookie
        return True
def _add_cookies_safely(driver, cookies_path: Path):
    """
    Đọc file JSON, set User-Agent (nếu có) và add Cookies.
    Trả về: (số cookie add thành công, user-agent đã set hoặc None)
    """
    if not os.path.exists(cookies_path):
        return 0, None

    with open(cookies_path, "r", encoding="utf-8") as f:
        try:
            raw = json.load(f)
        except json.JSONDecodeError:
            return 0, None

    # Biến lưu trữ dữ liệu
    cookies_list = []
    user_agent = None

    # 1. Xử lý logic đọc file (hỗ trợ cả format cũ và mới)
    if isinstance(raw, list):
        # Format cũ: Chỉ là list cookies
        cookies_list = raw
    elif isinstance(raw, dict):
        # Format mới: {"user_agent": "...", "cookies": [...]}
        cookies_list = raw.get("cookies", [])
        user_agent = raw.get("user_agent")
    else:
        print("[ERR] File cookies sai định dạng.")
        return 0, None

    # 2. Quan trọng: Set User-Agent NẾU tìm thấy trong file
    # Việc này phải làm trước khi load trang với cookie mới
    if user_agent:
        try:
            print(f"[AUTH] Found User-Agent in file, applying override...")
            driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": user_agent})
        except Exception as e:
            print(f"[WARN] Failed to set User-Agent: {e}")

    # 3. Add từng cookie
    added = 0
    if not isinstance(cookies_list, list):
        return 0, user_agent

    for c in cookies_list:
        nc = _normalize_cookie(c)
        if not nc:
            continue
        try:
            driver.add_cookie(nc)
            added += 1
        except Exception:
            pass
            
    return added, user_agent


def bootstrap_auth(d, cookie_path):
    """
    Hàm khởi động auth: vào facebook -> nạp cookie + UA -> reload check login
    """
    # Bước 1: Vào trang login trắng để khởi tạo domain context
    try:
        if "facebook.com" not in d.current_url:
            d.get("https://www.facebook.com/")
    except:
        d.get("https://www.facebook.com/")

    if cookie_path and os.path.exists(cookie_path):
        _add_cookies_safely(d, Path(cookie_path)) # Nhớ dùng hàm add cookie mới
        time.sleep(1.0)
        d.get("https://www.facebook.com/") # Reload để nhận cookie
        time.sleep(3.0) # Chờ load giao diện
    
    # --- PHẦN CHECK MỚI ---
    
    # 1. Check bằng Cookie (Logic ngầm)
    all_cookies = {c["name"]: c.get("value") for c in d.get_cookies()}
    has_cuser = "c_user" in all_cookies
    
    # 2. Check bằng Giao diện (HTML bạn gửi)
    is_visual_login = check_login_visual(d)

    if has_cuser and is_visual_login:
        print("[AUTH] Đăng nhập THÀNH CÔNG (Cookie OK + Không hiện form).")
        return True
    else:
        print("[AUTH] Đăng nhập THẤT BẠI (Cookie mất hoặc Bị hiện Popup).")
        return False

def find_mitmdump_executable():
    # 1) try PATH
    exe = shutil.which("mitmdump")
    if exe:
        return exe

    # 2) try venv typical locations (based on sys.executable)
    venv_root = os.path.dirname(os.path.dirname(sys.executable))
    candidates = []
    if os.name == "nt":
        candidates += [
            os.path.join(venv_root, "Scripts", "mitmdump.exe"),
            os.path.join(venv_root, "Scripts", "mitmdump"),
        ]
    else:
        candidates += [
            os.path.join(venv_root, "bin", "mitmdump"),
        ]

    for c in candidates:
        if os.path.exists(c):
            return c

    # 3) give up
    return None

def wait_for_port(host="127.0.0.1", port=MITM_PORT, timeout=MITM_WAIT_TIMEOUT):
    start = time.time()
    while time.time() - start < timeout:
        try:
            s = socket.create_connection((host, port), timeout=1.0)
            s.close()
            return True
        except Exception:
            time.sleep(0.2)
    return False

def safe_kill_process(proc):
    try:
        if proc and proc.poll() is None:
            proc.send_signal(signal.SIGINT)
            time.sleep(0.4)
            if proc.poll() is None:
                proc.kill()
    except Exception as e:
        print("[cleanup] error killing mitmdump:", e)

def start_driver_with_proxy(
    proxy_host: Optional[str],
    proxy_port: Optional[int],
    proxy_user: Optional[str],
    proxy_pass: Optional[str],
    mitm_port: int = 8899,
    headless: bool = False,
    quiet: bool = False,
    logger: Optional[logging.Logger] = None,
) -> webdriver.Chrome:

    if logger is None:
        logger = logging.getLogger("crawl_sheet1")  # fallback to your main logger

    # Build upstream strings safely (but only use them if proxy provided)
    upstream = None
    upstream_auth = None
    if proxy_host and proxy_port:
        upstream = f"{proxy_host}:{int(proxy_port)}"
    if proxy_user and proxy_pass:
        upstream_auth = f"{proxy_user}:{proxy_pass}"

    # find mitmdump
    mitmdump_bin = find_mitmdump_executable()
    if not mitmdump_bin:
        raise RuntimeError("Không tìm thấy mitmdump, hãy cài pip install mitmproxy")

    # Base args
    args = [
        mitmdump_bin,
        "-p", str(mitm_port),
        "--set", "connection_strategy=lazy",
        "--ssl-insecure",
    ]

    # Only add upstream mode if proxy is provided
    if upstream:
        args += ["--mode", f"upstream:http://{upstream}"]
        if upstream_auth:
            args += ["--upstream-auth", upstream_auth]

    # Debug: if MITM_DEBUG=1 in env then stream mitmdump stdout for easier debugging
    mitm_debug = os.environ.get("MITM_DEBUG", "0") in ("1", "true", "True")

    stdout_target = subprocess.PIPE if mitm_debug else subprocess.DEVNULL

    proc = subprocess.Popen(
        args,
        stdout=stdout_target,
        stderr=subprocess.STDOUT if stdout_target is subprocess.PIPE else subprocess.DEVNULL,
        text=True,
        bufsize=1,
    )

    def _log_stream():
        try:
            for line in proc.stdout:
                logger.info("[mitm:%s] %s", mitm_port, line.rstrip("\n"))
        except Exception:
            logger.exception("mitmdump logging thread ended")

    if stdout_target is subprocess.PIPE:
        threading.Thread(target=_log_stream, daemon=True).start()
    else:
        logger.debug("mitmdump started with stdout suppressed (DEVNULL)")

    # wait for mitmdump port
    if not wait_for_port("127.0.0.1", mitm_port, timeout=MITM_WAIT_TIMEOUT):
        safe_kill_process(proc)
        raise RuntimeError(f"mitmdump không mở cổng {mitm_port}")

    logger.info("MITM ready at 127.0.0.1:%s (quiet=%s)", mitm_port, bool(quiet))

    # Chrome options
    chrome_opts = Options()
    if headless:
        chrome_opts.add_argument("--headless=new")
    chrome_opts.add_argument(f"--proxy-server=http://127.0.0.1:{mitm_port}")
    chrome_opts.add_argument("--ignore-certificate-errors")
    chrome_opts.add_argument("--disable-blink-features=AutomationControlled")
    chrome_opts.add_argument("--no-sandbox")
    chrome_opts.add_argument("--disable-dev-shm-usage")
    chrome_opts.add_argument("--window-size=1366,768")

    driver = webdriver.Chrome(options=chrome_opts)
    logger.info("[Chrome] started, checking IP...")

    try:
        driver.get("https://api.ipify.org/?format=json")
        time.sleep(1.0)
        ip_html = (driver.page_source or "").strip()
        if quiet:
            logger.debug("[Chrome] ipify: %s", ip_html)
        else:
            logger.info("[Chrome] ipify: %s", ip_html)
    except Exception as e:
        logger.warning("[Chrome] ipify check failed: %s", e)

    logger.info("[Driver] Ready.")
    return driver

if __name__ == "__main__":
    # Example standalone run
    driver = start_driver_with_proxy(
        proxy_host="142.111.48.253",
        proxy_port=7030,
        proxy_user="ycycsdtq",
        proxy_pass="ka0d32hzsydi",
        mitm_port=8899,
        headless=False
    )
    bootstrap_auth(driver, COOKIES_PATH)
    time.sleep(3)
    driver.get("https://api.ipify.org/?format=json")
    print(driver.page_source)
