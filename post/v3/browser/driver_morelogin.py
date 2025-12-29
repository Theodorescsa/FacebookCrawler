# post/v3/browser/driver_morelogin.py
import logging
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import SessionNotCreatedException

# Tắt bớt log rác
logging.getLogger("WDM").setLevel(logging.WARNING)

def _get_driver_path(version: str):
    """Hàm phụ trợ để tải driver theo version"""
    try:
        return ChromeDriverManager(driver_version=version).install()
    except Exception as e:
        print(f"Không tải được driver bản {version}: {e}")
        return None

def create_chrome_attach(debug_port: int) -> webdriver.Chrome:
    """
    Attach Selenium vào Chrome MoreLogin.
    Tự động thử Driver v142 trước, nếu lỗi version thì fallback về v140.
    """
    chrome_opts = Options()
    chrome_opts.add_experimental_option("debuggerAddress", f"127.0.0.1:{debug_port}")

    # --- CHIẾN THUẬT: THỬ SAI (TRY-EXCEPT) ---
    
    # 1. Ưu tiên thử bản 142 (cho các Profile mới)
    try:
        driver_path = _get_driver_path("142")
        service = Service(executable_path=driver_path)
        driver = webdriver.Chrome(service=service, options=chrome_opts)
        
        # Setup chuẩn
        driver.set_window_size(1920, 1080)
        driver.set_page_load_timeout(40)
        driver.set_script_timeout(40)
        return driver
        
    except SessionNotCreatedException:
        print(">> Driver v142 không khớp, đang chuyển sang thử Driver v140...")
    except Exception as e:
        print(f">> Lỗi khác khi khởi tạo v142: {e}")

    # 2. Nếu thất bại, thử bản 140 (cho các Profile cũ)
    print(">> Đang khởi tạo fallback Driver v140...")
    driver_path = _get_driver_path("140")
    service = Service(executable_path=driver_path)
    
    # Lần này nếu lỗi thì cho crash luôn để báo lỗi ra ngoài
    driver = webdriver.Chrome(service=service, options=chrome_opts)
    
    driver.set_window_size(1920, 1080)
    driver.set_page_load_timeout(40)
    driver.set_script_timeout(40)
    
    return driver