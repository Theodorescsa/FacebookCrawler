# =========================
# Hook /api/graphql/
# =========================
from datetime import datetime, date, timedelta

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
CLEANUP_JS = r"""
(function(keep) {
  try {
    const selectors = [
      "div[data-pagelet^='FeedUnit_']",
      "div[role='article']",
      "div[aria-posinset]"
    ];
    let posts = [];
    for (const sel of selectors) {
      posts = Array.from(document.querySelectorAll(sel));
      if (posts.length >= 10) break;
    }

    const total = posts.length;
    const k = keep || 30;
    if (total <= k) return;

    const removeCount = total - k;
    for (let i = 0; i < removeCount; i++) {
      const el = posts[i];
      if (!el) continue;
      const story = el.closest("[data-testid='fbfeed_story']") || el;
      story.remove();
    }
  } catch (e) {
    // ignore
  }
})(arguments[0]);
"""

def install_early_hook(driver, keep_last=350):
    HOOK_SRC = r"""
    (function(){
      if (window.__gqlHooked) return;
      window.__gqlHooked = true;
      window.__gqlReqs = [];
      function headersToObj(h){try{
        if (!h) return {};
        if (h instanceof Headers){const o={}; h.forEach((v,k)=>o[k]=v); return o;}
        if (Array.isArray(h)){const o={}; for (const [k,v] of h) o[k]=v; return o;}
        return (typeof h==='object')?h:{};}catch(e){return {}}
      }
      function pushRec(rec){try{
        const q = window.__gqlReqs; q.push(rec);
        if (q.length > __KEEP_LAST__) q.splice(0, q.length - __KEEP_LAST__);
      }catch(e){}}
      const origFetch = window.fetch;
      window.fetch = async function(input, init){
        const url = (typeof input==='string') ? input : (input&&input.url)||'';
        const method = (init&&init.method)||'GET';
        const body = (init && typeof init.body==='string') ? init.body : '';
        const hdrs = headersToObj(init && init.headers);
        let rec = null;
        if (url.includes('/api/graphql/') && method==='POST'){
          rec = {kind:'fetch', url, method, headers:hdrs, body:String(body)};
        }
        const res = await origFetch(input, init);
        if (rec){
          try{ rec.responseText = await res.clone().text(); }catch(e){ rec.responseText = null; }
          pushRec(rec);
        }
        return res;
      };
      const XO = XMLHttpRequest.prototype.open, XS = XMLHttpRequest.prototype.send;
      XMLHttpRequest.prototype.open = function(m,u,a){ this.__m=m; this.__u=u; return XO.apply(this, arguments); };
      XMLHttpRequest.prototype.send = function(b){
        this.__b = (typeof b==='string')?b:'';
        this.addEventListener('load', ()=>{
          try{
            if ((this.__u||'').includes('/api/graphql/') && (this.__m||'')==='POST'){
              pushRec({kind:'xhr', url:this.__u, method:this.__m, headers:{}, body:String(this.__b),
                       responseText:(typeof this.responseText==='string'?this.responseText:null)});
            }
          }catch(e){}
        });
        return XS.apply(this, arguments);
      };
    })();
    """.replace("__KEEP_LAST__", str(keep_last))
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": HOOK_SRC})
    driver.execute_script(HOOK_SRC)

def wait_for(driver, timeout: int = 20) -> WebDriverWait:
    return WebDriverWait(driver, timeout)
def wait_for(driver, timeout: int = 20) -> WebDriverWait:
    return WebDriverWait(driver, timeout)


def open_filter_dialog(driver):
    """
    Click nút 'Bộ lọc' ở header group để mở panel có 'Đi đến:' + các combobox.
    """
    w = wait_for(driver)
    btn = w.until(
        EC.element_to_be_clickable(
            (
                By.XPATH,
                # span 'Bộ lọc' rồi leo lên container cha để click
                "//span[normalize-space()='Bộ lọc']"
                "/ancestor::div[@role='none' or @role='button'][1]",
            )
        )
    )
    driver.execute_script("arguments[0].click();", btn)


def _select_enddate_combo_option(driver, part: str, option_text: str):
    """
    Chọn combobox 'ngày kết thúc <part>' trong popup 'Đi đến:',
    rồi chọn option tương ứng.

    part: 'năm' | 'tháng' | 'ngày'
    option_text: text hiển thị trong dropdown (vd: '2025', 'Tháng 1', '1', '15', ...)
    """
    if part not in ("năm", "tháng", "ngày"):
        raise ValueError("part phải là 'năm' / 'tháng' / 'ngày'")

    w = wait_for(driver)

    # aria-label ví dụ:
    # "Chỉnh sửa ngày kết thúc năm  Lựa chọn hiện tại là 2025"
    # "Chỉnh sửa ngày kết thúc tháng  Lựa chọn hiện tại là 1"
    # "Chỉnh sửa ngày kết thúc ngày  Lựa chọn hiện tại là 1"
    label_contains = f"kết thúc {part}"

    combo_xpath = (
        "//div[@role='combobox' and contains(@aria-label, '%s')]" % label_contains
    )
    combo = w.until(
        EC.element_to_be_clickable(
            (
                By.XPATH,
                combo_xpath,
            )
        )
    )
    # Dùng JS click cho chắc
    driver.execute_script("arguments[0].click();", combo)

    # Option trong dropdown (role='option')
    option_xpath = (
        "//div[@role='option']//span[normalize-space()='%s']" % option_text
    )
    opt = w.until(
        EC.element_to_be_clickable(
            (
                By.XPATH,
                option_xpath,
            )
        )
    )
    driver.execute_script("arguments[0].click();", opt)


def go_to_date(driver, target: date):
    """
    Mở popup 'Bộ lọc' → panel 'Đi đến:',
    chọn một NGÀY DUY NHẤT (ngày kết thúc) = target,
    rồi bấm 'Xong' để Facebook nhảy tới ngày đó.
    """
    w = wait_for(driver)

    # 1) Mở popup Bộ lọc
    open_filter_dialog(driver)

    # (Nếu sau khi mở 'Bộ lọc' còn phải click dòng 'Đi đến' để hiện 3 combobox
    #  thì thêm 1 step click ở đây, vd:
    # go_to_btn = w.until(EC.element_to_be_clickable((
    #     By.XPATH,
    #     "//span[normalize-space()='Đi đến:']"
    #     "/ancestor::div[@role='button' or @role='menuitem'][1]"
    # )))
    # driver.execute_script("arguments[0].click();", go_to_btn)
    # )

    # 2) Chọn Năm (combobox 'ngày kết thúc năm')
    _select_enddate_combo_option(driver, "năm", str(target.year))

    # 3) Chọn Tháng (combobox 'ngày kết thúc tháng')
    # Trên UI của bạn đang hiển thị 'Tháng 1', 'Tháng 2', ...
    month_text = f"Tháng {target.month}"
    try:
        _select_enddate_combo_option(driver, "tháng", month_text)
    except Exception:
        # fallback: nếu option chỉ là số '1', '2', ...
        _select_enddate_combo_option(driver, "tháng", str(target.month))

    # 4) Chọn Ngày (combobox 'ngày kết thúc ngày')
    _select_enddate_combo_option(driver, "ngày", str(target.day))

    # 5) Bấm nút 'Xong' (nút thật, không bị aria-disabled, không bị aria-hidden)
    done_btn = w.until(
        EC.element_to_be_clickable(
            (
                By.XPATH,
                "//div[@role='button' and .//span[normalize-space()='Xong'] and not(@aria-disabled='true')]",
            )
        )
    )
    driver.execute_script("arguments[0].click();", done_btn)
    time.sleep(2.0)