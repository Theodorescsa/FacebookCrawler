import requests
import time
from typing import Optional

MORELOGIN_API = "http://127.0.0.1:40000/api"

API_ID = "1670752236065669"     # nếu MoreLogin yêu cầu
API_KEY = "119e310a2e2248f58d8e35653290544e"    # nếu MoreLogin yêu cầu

HEADERS = {
    "Content-Type": "application/json",
}

if API_ID:
    HEADERS["api-id"] = API_ID
if API_KEY:
    HEADERS["api-key"] = API_KEY


class MoreLoginError(RuntimeError):
    pass


def open_profile(
    profile_id: str,
    *,
    headless: bool = False,
    cdp_evasion: bool = True,
    wait_ready: float = 2.0,
) -> int:
    """
    Start MoreLogin profile (env/start)
    Trả về Chrome debugging port để Selenium attach

    headless     : headless của MoreLogin (KHÔNG phải selenium --headless)
    cdp_evasion  : rất nên bật cho Facebook
    """

    payload = {
        "envId": profile_id,
        "isHeadless": headless,
        "cdpEvasion": cdp_evasion,
    }

    resp = requests.post(
        f"{MORELOGIN_API}/env/start",
        json=payload,
        headers=HEADERS,
        timeout=30,
    )

    resp.raise_for_status()
    data = resp.json()

    if data.get("code") != 0:
        raise MoreLoginError(f"MoreLogin start failed: {data}")

    debug_port = data.get("data", {}).get("debugPort")
    if not debug_port:
        raise MoreLoginError("Không lấy được debugPort từ MoreLogin")

    # đợi Chrome ready
    if wait_ready > 0:
        time.sleep(wait_ready)

    return int(debug_port)


def close_profile(profile_id: str):
    """
    Stop MoreLogin profile (env/close)
    """
    try:
        requests.post(
            f"{MORELOGIN_API}/env/close",
            json={"envId": profile_id},
            headers=HEADERS,
            timeout=10,
        )
    except Exception:
        pass
