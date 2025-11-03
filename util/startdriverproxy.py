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
import logging
# ------------------ CONFIG ------------------
ALLOWED_COOKIE_DOMAINS = {".facebook.com", "facebook.com", "m.facebook.com", "web.facebook.com"}
COOKIES_PATH         = r"E:\NCS\fb-selenium\database\facebookaccount\authen_tranhoangdinhnam\cookies.json"
MITM_PORT = 8899  
UPSTREAM_PROXY = "142.111.48.253:7030"  
UPSTREAM_AUTH = "ycycsdtq:ka0d32hzsydi" 
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

def _add_cookies_safely(driver, cookies_path: Path):
    with open(cookies_path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    if isinstance(raw, dict) and "cookies" in raw:
        raw = raw["cookies"]
    if not isinstance(raw, list):
        raise ValueError("File cookies không phải mảng JSON.")

    added = 0
    for c in raw:
        nc = _normalize_cookie(c)
        if not nc:
            continue
        try:
            driver.add_cookie(nc)
            added += 1
        except Exception:
            pass
    return added

def bootstrap_auth(d,cookie_path):
    d.get("https://www.facebook.com/")
    time.sleep(1.0)
    print("[AUTH] Bootstrapping auth...")
    if cookie_path and os.path.exists(cookie_path):
        try:
            count = _add_cookies_safely(d, Path(cookie_path))
            d.get("https://www.facebook.com/")
            time.sleep(1.0)
            print(f"[AUTH] Added cookies: {count}")
        except Exception as e:
            print("[WARN] bootstrap cookies:", e)
    try:
        all_cookies = {c["name"]: c.get("value") for c in d.get_cookies()}
        has_cuser = "c_user" in all_cookies
        has_xs    = "xs" in all_cookies
        print(f"[AUTH] c_user={has_cuser}, xs={has_xs}")
    except Exception as e:
        print("[WARN] bootstrap cookies:", e)


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
    proxy_host: str,
    proxy_port: int,
    proxy_user: str,
    proxy_pass: str,
    mitm_port: int = 8899,
    headless: bool = False,
    cookies_path: Optional[Path] = COOKIES_PATH,
    quiet: bool = False,
    logger: Optional[logging.Logger] = None,
) -> webdriver.Chrome:

    if logger is None:
        logger = logging.getLogger("crawl_sheet1")  # fallback to your main logger

    upstream = f"{proxy_host}:{proxy_port}"
    upstream_auth = f"{proxy_user}:{proxy_pass}"

    # find mitmdump
    mitmdump_bin = find_mitmdump_executable()
    if not mitmdump_bin:
        raise RuntimeError("Không tìm thấy mitmdump, hãy cài pip install mitmproxy")

    args = [
        mitmdump_bin,
        "-p", str(mitm_port),
        "--set", "connection_strategy=lazy",
        "--ssl-insecure",
        "--mode", f"upstream:http://{upstream}",
        "--upstream-auth", upstream_auth,
    ]

    stdout_target = subprocess.DEVNULL

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
                logger.debug("[mitm:%s] %s", mitm_port, line.rstrip("\n"))
        except Exception:
            # if we couldn't read stdout (e.g. DEVNULL), just ignore
            logger.debug("mitmdump logging thread ended or not available", exc_info=True)

    if stdout_target is subprocess.PIPE:
        threading.Thread(target=_log_stream, daemon=True).start()
    else:
        # no thread started; mitmdump output suppressed
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
        # Only log at debug if quiet, otherwise info
        if quiet:
            logger.debug("[Chrome] ipify: %s", ip_html)
        else:
            logger.info("[Chrome] ipify: %s", ip_html)
    except Exception as e:
        logger.warning("[Chrome] ipify check failed: %s", e)

    logger.info("[Driver] Ready.")
    return driver
if __name__ == "__main__":
    driver = start_driver_with_proxy(
        proxy_host="142.111.48.253",
        proxy_port=7030,
        proxy_user="ycycsdtq",
        proxy_pass="ka0d32hzsydi",
        mitm_port=8899,
        headless=False
    )
    bootstrap_auth(driver,COOKIES_PATH)
    # Test: mở Facebook group / crawl gì đó
    time.sleep(3)
    driver.get("https://api.ipify.org/?format=json")
    print(driver.page_source)
