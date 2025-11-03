import json, time, urllib.parse, sys, re
from selenium.webdriver.common.by import By
from collections import deque
from pathlib import Path
from extract_comment_utils import extract_full_posts_from_resptext, extract_replies_from_depth1_resp
from configs import RAW_DUMS, REPLY_DOC_ID
from get_comment_fb_utils import (
                                 _split_top_level_json_objects,
                                 _strip_xssi_globally,
                                 append_ndjson_line,
                                 clean_fb_resp_text,
                                 collect_reply_tokens_from_json,
                                 detect_cursor_key,
                                 load_checkpoint,
                                 open_reel_comments_if_present,
                                 save_checkpoint,
                                 set_sort_to_all_comments_unified,
                                 strip_cursors_from_vars
                                 )


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
from logs.loging_config import logger


def install_early_hook(driver):
    HOOK_SRC = r"""
    (function(){
      if (window.__gqlHooked) return;
      window.__gqlHooked = true;
      window.__gqlReqs = [];
      function headersToObj(h){
        try{
          if (!h) return {};
          if (h instanceof Headers){ const o={}; h.forEach((v,k)=>o[k]=v); return o; }
          if (Array.isArray(h)){ const o={}; for(const [k,v] of h) o[k]=v; return o; }
          return (typeof h==='object') ? h : {};
        }catch(e){ return {}; }
      }
      const pushRec = (rec)=>{ try{ (window.__gqlReqs||[]).push(rec); }catch(e){} };
      const origFetch = window.fetch;
      window.fetch = async function(input, init){
        const url = (typeof input==='string') ? input : (input && input.url) || '';
        const method = (init && init.method) || 'GET';
        const body = (init && typeof init.body==='string') ? init.body : '';
        const hdrs = headersToObj(init && init.headers);
        let rec = null;
        if (url.includes('/api/graphql/') && method === 'POST'){
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
        this.__b = (typeof b==='string') ? b : '';
        this.addEventListener('load', ()=>{
          try{
            if ((this.__u||'').includes('/api/graphql/') && (this.__m||'')==='POST'){
              (window.__gqlReqs||[]).push({
                kind:'xhr', url:this.__u, method:this.__m, headers:{},
                body:String(this.__b),
                responseText:(typeof this.responseText==='string'?this.responseText:null)
              });
            }
          }catch(e){}
        });
        return XS.apply(this, arguments);
      };
    })();
    """
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": HOOK_SRC})

def hook_graphql(driver):
    js = r"""
    (function() {
      if (window.__gqlHooked) return;
      window.__gqlHooked = true;
      window.__gqlReqs = window.__gqlReqs || [];

      // wrap fetch
      const _fetch = window.fetch;
      window.fetch = function(input, init) {
        try {
          const url = (typeof input === 'string') ? input : (input && input.url) || '';
          const method = (init && init.method) || 'GET';
          let body = (init && init.body) || '';
          if (body instanceof URLSearchParams) body = body.toString();
          if (String(url).includes('/api/graphql/')) {
            window.__gqlReqs.push({ts:Date.now(), type:'fetch', url:String(url), method:String(method), body:String(body||'')});
          }
        } catch(e) {}
        return _fetch.apply(this, arguments);
      };

      // wrap XHR
      const _open = XMLHttpRequest.prototype.open;
      const _send = XMLHttpRequest.prototype.send;
      XMLHttpRequest.prototype.open = function(method, url) {
        this.__gql_meta = { url: String(url||''), method: String(method||'GET') };
        return _open.apply(this, arguments);
      };
      XMLHttpRequest.prototype.send = function(body) {
        try {
          const meta = this.__gql_meta || {};
          if (String(meta.url).includes('/api/graphql/')) {
            window.__gqlReqs.push({ts:Date.now(), type:'xhr', url:String(meta.url), method:String(meta.method||'GET'), body:String(body||'')});
          }
        } catch(e) {}
        return _send.apply(this, arguments);
      };
    })();
    """
    driver.execute_script(js)

# =========================
# Utils (GraphQL buffer)
# =========================
def gql_count(driver):
    return driver.execute_script("return (window.__gqlReqs||[]).length")

def get_gql_at(driver, i):
    return driver.execute_script("return (window.__gqlReqs||[])[arguments[0]]", i)

def wait_next_comment_req(driver, start_idx, timeout=10, poll=0.2):
    """Đợi đúng 1 request comment mới sau mốc start_idx."""
    end = time.time() + timeout
    cur = start_idx
    while time.time() < end:
        n = gql_count(driver)
        while cur < n:
            req = get_gql_at(driver, cur)
            if req and match_comment_req(req):
                return (cur, req)
            cur += 1
        time.sleep(poll)
    return None

# =========================
# FB GraphQL comment match & parsing
# =========================
def parse_form(body_str):
    qs = urllib.parse.parse_qs(body_str, keep_blank_values=True)
    return {k:(v[0] if isinstance(v, list) else v) for k,v in qs.items()}

friendly_name_whitelist = [
    "CommentsListComponentsPaginationQuery",
    "UFI2CommentsProviderPaginationQuery",
    "CometUFI.*Comments.*Pagination",
]

def match_comment_req(rec):
    if "/api/graphql/" not in rec.get("url",""): return False
    if rec.get("method") != "POST": return False
    body = rec.get("body","") or ""
    if "fb_api_req_friendly_name=" in body:
        if "fb_api_req_friendly_name=CommentsListComponentsPaginationQuery" in body: return True
        if "fb_api_req_friendly_name=UFI2CommentsProviderPaginationQuery" in body: return True
        if re.search(r"fb_api_req_friendly_name=CometUFI[^&]*Comments[^&]*Pagination", body): return True
    if "variables=" in body:
        try:
            v = parse_form(body).get("variables","")
            vj = json.loads(urllib.parse.unquote_plus(v))
            keys = set(vj.keys())
            signs = {"commentable_object_id","commentsAfterCursor","feedLocation","focusCommentID","feedbackSource"}
            if keys & signs: return True
        except:
            pass
    return False

def find_pageinfo(obj):
    if isinstance(obj, dict):
        if "page_info" in obj and isinstance(obj["page_info"], dict):
            pi = obj["page_info"]
            return pi.get("end_cursor"), pi.get("has_next_page")
        for v in obj.values():
            c = find_pageinfo(v)
            if c: return c
    elif isinstance(obj, list):
        for v in obj:
            c = find_pageinfo(v)
            if c: return c
    return (None, None)

def extract_comment_texts(obj, out):
    if isinstance(obj, dict):
        if "body" in obj and isinstance(obj["body"], dict) and "text" in obj["body"]:
            out.append(obj["body"]["text"])
        if "message" in obj and isinstance(obj["message"], dict) and "text" in obj["message"]:
            out.append(obj["message"]["text"])
        for v in obj.values():
            extract_comment_texts(v, out)
    elif isinstance(obj, list):
        for v in obj:
            extract_comment_texts(v, out)

def extract_comments_from_resptext(resp_text):
    texts = []
    try:
        obj = json.loads(resp_text)
    except:
        return texts, None, None, None
    extract_comment_texts(obj, texts)
    end_cursor, has_next = find_pageinfo(obj)
    total = None
    try:
        c = obj["data"]["node"]["comment_rendering_instance_for_feed_location"]["comments"]
        total = c.get("count") or c.get("total_count")
    except:
        pass
    return texts, end_cursor, total, obj
# =========================
# UI interactions (scroll/click)
# =========================
def click_view_more_if_any(driver, max_clicks=1):
    xps = [
        "//div[@role='button'][contains(.,'Xem thêm bình luận') or contains(.,'Xem thêm phản hồi')]",
        "//span[contains(.,'Xem thêm bình luận') or contains(.,'Xem thêm phản hồi')]/ancestor::div[@role='button']",
        "//div[@role='button'][contains(.,'View more comments') or contains(.,'View more replies')]",
        "//span[contains(.,'View more comments') or contains(.,'View more replies')]/ancestor::div[@role='button']",
    ]
    clicks = 0
    for xp in xps:
        for b in driver.find_elements(By.XPATH, xp):
            if clicks >= max_clicks: return clicks
            try:
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", b)
                time.sleep(0.15)
                b.click()
                clicks += 1
                time.sleep(0.35)
            except: pass
    return clicks

def scroll_to_last_comment(driver):
    js = r"""
    (function(){
      const cands = Array.from(document.querySelectorAll("div[role='article'][aria-label]"));
      let nodes = cands.filter(n => /Bình luận/i.test(n.getAttribute('aria-label')||""));
      if (nodes.length === 0) nodes = cands.filter(n => /(Comment|Comments)/i.test(n.getAttribute('aria-label')||""));
      if (nodes.length === 0) return false;
      nodes[nodes.length - 1].scrollIntoView({behavior: 'instant', block: 'center'});
      window.scrollBy(0, Math.floor(window.innerHeight*0.1));
      return true;
    })();
    """
    return bool(driver.execute_script(js))

COMMENT_FRIENDLY_HINTS = (
    "UFI", "Comment", "Comments", "CommentList", "CommentPagination",
    "CometUFI", "CometComment"
)

def _is_comments_gql(req: dict) -> bool:
    if not req:
        return False
    body = req.get("body") or ""
    try:
        form = parse_form(body)
    except Exception:
        return False

    friendly = (form.get("fb_api_req_friendly_name") or "").lower()
    if any(h.lower() in friendly for h in COMMENT_FRIENDLY_HINTS):
        return True

    # Dựa vào variables thay vì friendly_name (chắc cú hơn)
    vars_str = urllib.parse.unquote_plus(form.get("variables","") or "")
    try:
        vars_obj = json.loads(vars_str) if vars_str else {}
    except Exception:
        vars_obj = {}

    keys = set(map(str, vars_obj.keys()))
    # các keys thường gặp của comments
    comment_keys = {
        "feedbackID", "feedback_id",
        "displayCommentsContextEnableComment",
        "commentsAfterCount", "commentsAfterCursor",
        "after", "before", "first", "last"
    }
    if keys & comment_keys:
        return True

    return False

def wait_first_comment_request(driver, baseline, timeout=12, poll=0.2):
    import time as _t
    end = _t.time() + timeout
    last_n = 0
    while _t.time() < end:
        reqs = driver.execute_script("return (window.__gqlReqs||[])")
        n = len(reqs)
        if n != last_n:
            # duyệt từ request mới nhất về cũ, chỉ trong phần tăng thêm sau baseline
            for i in range(n-1, max(baseline-1, -1), -1):
                req = reqs[i]
                if _is_comments_gql(req):
                    return req
            last_n = n
        _t.sleep(poll)
    raise TimeoutError("Không thấy request comments sau khi set sort/click")


# =========================
# Replay GraphQL inside the page (keeps auth/cookies)
# =========================
def graphql_post_in_page(driver, url: str, form_params: dict, override_vars: dict):
    fp = dict(form_params)
    fp["variables"] = json.dumps(override_vars, separators=(',',':'), ensure_ascii=False)
    body = urllib.parse.urlencode(fp)
    js = r"""
    const url = arguments[0], body = arguments[1], cb = arguments[2];
    fetch(url, {
      method:'POST', credentials:'include',
      headers:{'content-type':'application/x-www-form-urlencoded'},
      body
    }).then(r=>r.text()).then(t=>cb({ok:true,text:t}))
      .catch(e=>cb({ok:false,err:String(e)}));
    """
    driver.set_script_timeout(120)
    ret = driver.execute_async_script(js, url, body)
    if not ret or not ret.get("ok"):
        raise RuntimeError("Replay GraphQL failed: %s" % (ret and ret.get('err')))
    return ret["text"]

def pick_reply_template_from_page(driver):
    """
    Lấy cái request GraphQL dùng để load REPLIES (Depth1).
    Ưu tiên mấy friendly name kiểu Depth1CommentsListPaginationQuery.
    """
    reqs = driver.execute_script("return window.__gqlReqs || []") or []
    # duyệt từ cuối lên đầu để lấy request mới nhất
    for r in reversed(reqs):
        body = r.get("body") or ""
        form = parse_form(body)
        friendly = form.get("fb_api_req_friendly_name", "") or ""
        vars_str = urllib.parse.unquote_plus(form.get("variables","") or "")
        try:
            vars_obj = json.loads(vars_str) if vars_str else {}
        except Exception:
            vars_obj = {}

        # vài pattern tên thường gặp
        if (
            "Depth1CommentsListPaginationQuery" in friendly
            or "CommentRepliesList" in friendly
            or "CommentReplies" in friendly
            or ("repliesAfterCount" in vars_obj)
        ):
            # đây mới là template reply thật
            return r.get("url"), form, vars_obj

    # không tìm được → trả None, caller sẽ fallback
    return None, None, None


def crawl_replies_for_parent_expansion(
    driver,
    url,
    form,
    base_reply_vars,
    parent_id,
    parent_token,
    out_json,
    extract_fn,
    clean_fn,
    max_reply_pages=None
):
    pages = 0
    current_token = parent_token

    reply_form = dict(form)
    reply_form["doc_id"] = REPLY_DOC_ID
    reply_form["fb_api_req_friendly_name"] = "Depth1CommentsListPaginationQuery"

    while True:
        pages += 1
        if max_reply_pages and pages > max_reply_pages:
            break

        use_vars = dict(base_reply_vars)
        # dọn field comment-level
        use_vars.pop("commentsAfterCount", None)
        use_vars.pop("commentsAfterCursor", None)
        use_vars.pop("commentsBeforeCount", None)
        use_vars.pop("commentsBeforeCursor", None)

        # query theo FEEDBACK ID
        use_vars["id"] = parent_id
        use_vars["repliesAfterCount"] = 20
        if current_token:
            use_vars["expansionToken"] = current_token

        raw_ret = graphql_post_in_page(driver, url, reply_form, use_vars)
        resp_text = raw_ret.get("text") if isinstance(raw_ret, dict) else raw_ret

        try:
            json.loads(resp_text)
        except Exception:
            resp_text = clean_fn(resp_text)

        # 👇 Lúc này replies là list "full rows"
        replies, next_token = extract_fn(resp_text, parent_id)

        new_cnt = 0
        for r in replies:
            # r đã là dạng comment-row rồi → chỉ thêm metadata để phân biệt reply
            rec = {
                **r,
                "is_reply": True,
                "parent_id": parent_id,
                "page": pages,
                "ts": time.time(),
            }
            append_ndjson_line(out_json, rec)
            new_cnt += 1

        logger.info(f"[V2-REPLIES] parent={parent_id[:12]}… page {pages}: +{new_cnt}/{len(replies)}")

        if not next_token or next_token == current_token:
            logger.info("[V2-REPLIES] Hết trang replies (no new expansion_token).")
            break

        current_token = next_token

def crawl_comments(driver, out_json="comments.ndjson", checkpoint_path="checkpoint_comments.json", max_pages=None):

    # 1) ensure one lightweight scroll to produce first request
    baseline = driver.execute_script("return (window.__gqlReqs||[]).length")
    set_sort_to_all_comments_unified(driver)
    # 1) click “Xem thêm …” nếu có, else kéo đến comment cuối
    for _ in range(1):
        if click_view_more_if_any(driver, max_clicks=1) == 0:  # FIX: dùng driver
            if not scroll_to_last_comment(driver):             # FIX: dùng driver
                driver.execute_script("window.scrollBy(0, Math.floor(window.innerHeight*0.8));")
        time.sleep(1)

    first_req = wait_first_comment_request(driver, baseline, timeout=12, poll=0.2)

    url = first_req.get("url")
    form = parse_form(first_req.get("body",""))
    # variables gốc
    orig_vars_str = urllib.parse.unquote_plus(form.get("variables","") or "")
    try:
        orig_vars = json.loads(orig_vars_str) if orig_vars_str else {}
    except Exception:
        orig_vars = {}

    cursor_key = detect_cursor_key(orig_vars)
    vars_template = strip_cursors_from_vars(orig_vars)

    # doc_id / friendly (giữ nguyên để replay đúng tài liệu)
    doc_id = form.get("doc_id")
    friendly = form.get("fb_api_req_friendly_name")

    # 2) load checkpoint (nếu có)
    ck = load_checkpoint(checkpoint_path)
    if ck and ck.get("doc_id") == doc_id and ck.get("friendly") == friendly:
        # resume
        last_cursor = ck.get("cursor")
        saved_template = ck.get("vars_template") or {}
        saved_cursor_key = ck.get("cursor_key") or cursor_key
        if saved_template:
            vars_template = saved_template
        if saved_cursor_key:
            cursor_key = saved_cursor_key
    else:
        # init checkpoint fresh
        ck = {
            "cursor": None,
            "vars_template": vars_template,
            "cursor_key": cursor_key,
            "doc_id": doc_id,
            "friendly": friendly,
            "ts": time.time()
        }
        # save_checkpoint(ck, checkpoint_path)

    # 3) paginate via replay
    all_texts = []
    pages = 0
    current_cursor = ck.get("cursor")
    seen_cursors = set()
    reply_jobs = deque()  # NEW: hàng đợi crawl replies

    skip_count = 0
    seen_links = set()
    while True:
        pages += 1
        if max_pages and pages > max_pages:
            break
        if skip_count > 1:
            break
        use_vars = dict(vars_template)
        use_vars.setdefault("commentsAfterCount", 50)
        if current_cursor:
            use_vars[cursor_key] = current_cursor

        # replay
        raw_ret = graphql_post_in_page(driver, url, form, use_vars)
        resp_text = raw_ret.get("text") if isinstance(raw_ret, dict) else raw_ret

        # parse “an toàn”
        reply_token_map = {}
        try:
            # # case FB trả JSON sạch
            # with open(f"raw_dumps/page{pages}.txt", "w", encoding="utf-8") as f:
            #     f.write(resp_text)
            json_resp = json.loads(resp_text)
            
            cleaned = resp_text
            reply_token_map = {}
            collect_reply_tokens_from_json(json_resp, reply_token_map)
        except Exception as e:
            # case FB trả 2 JSON dính nhau → dùng hàm clean
            raw = resp_text
            stripped = _strip_xssi_globally(raw)
            parts = _split_top_level_json_objects(stripped)
            if len(parts) > 1:
                cleaned = clean_fb_resp_text(raw)      # ưu tiên block có cursor bằng score đệ quy
                json_resp = json.loads(cleaned)
            else:
                json_resp = json.loads(stripped)
                cleaned = stripped
            logger.warning(f"[WARN] page {pages} parse fail:", e)

        # # lưu JSON sạch để trace (optional)
        with open(f"{RAW_DUMS}/page{pages}.json", "w", encoding="utf-8") as f:
            json.dump(json_resp, f, ensure_ascii=False, indent=2)

        # extract
        batch_texts, end_cursor, total_target, extra = extract_full_posts_from_resptext(cleaned)
        if extra and isinstance(extra, dict):
            for job in extra.get("reply_jobs", []):
                # job kiểu: {"id": parent_comment_id, "token": expansion_token}
                reply_jobs.append(job)
        # stop if no next page
        if not end_cursor:
            logger.info("[V2] Hết trang (không còn end_cursor).")
            break

        # guard: cursor không tiến hoặc lặp
        if current_cursor and end_cursor == current_cursor:
            logger.info(f"[FUSE] cursor no-advance at page {pages} (cursor={current_cursor[:20]}...). Stop to avoid loop.")
            break
        if end_cursor in seen_cursors:
            logger.info(f"[FUSE] cursor repeated: {str(end_cursor)[:20]}... Stop.")
            break
        seen_cursors.add(end_cursor)

        logger.debug(f"[DBG] cursor_key={cursor_key} current={str(current_cursor)[:24]}... next={str(end_cursor)[:24]}...")

        # ✅ GHI THEO COMMENT — MỖI COMMENT 1 DÒNG + ENQUEUE REPLIES
        if batch_texts:
            new_cnt = 0
            for idx, item in enumerate(batch_texts, 1):
                # lấy text
                if isinstance(item, dict):
                    txt = (
                        item.get("text")
                        or item.get("message")
                        or item.get("body")
                        or json.dumps(item, ensure_ascii=False)
                    )
                    reply_count = (
                        item.get("comment")
                        or item.get("reply_count")
                        or item.get("comments_count")
                        or 0
                    )
                else:
                    txt = str(item)
                    reply_count = 0

                txt = (txt or "").strip()
                if not txt:
                    continue

                # dedupe
                link = item.get("link").strip().lower()

                # Nếu đã có thì bỏ qua
                if link in seen_links:
                    logger.info(f"[SKIP] trùng comment {link or '(no link)'} -> skip")
                    skip_count += 1

                # Nếu chưa có thì thêm vào set
                seen_links.add(link)

                # ghi dòng comment
                rec = {
                    **item,
                    "is_reply": False,
                    "parent_id": None,
                    "page": pages,
                    "index_in_page": idx,
                    "cursor": end_cursor,
                    "ts": time.time(),
                    "target": total_target,
                }
                append_ndjson_line(out_json, rec)
                new_cnt += 1
                if skip_count > 1:
                    break
                # 🟣🟣🟣 ENQUEUE REPLIES Ở ĐÂY
                # extractor mới đã có: item["feedback_id"], item["raw_comment_id"]
                fb_id = item.get("feedback_id")
                raw_cid = item.get("raw_comment_id") or item.get("id")

                if isinstance(reply_count, int) and reply_count > 0:
                    info = None
                    # 1) ưu tiên feedback_id vì crawl replies đang query theo feedback
                    if fb_id:
                        info = reply_token_map.get(fb_id)
                    # 2) thử theo id gốc
                    if not info and raw_cid:
                        info = reply_token_map.get(raw_cid)
                    # 3) thử theo id hiện tại
                    if not info and item.get("id"):
                        info = reply_token_map.get(item["id"])
                    if info:
                        reply_jobs.append({
                            "id": info["feedback_id"], 
                            "token": info["token"],
                        })
                    else:
                        logger.warning(f"[REPLIES] comment {(raw_cid or fb_id or '')[:12]}… có {reply_count} replies nhưng KHÔNG thấy expansionToken/feedback_id → skip")

            all_texts.extend(batch_texts)
            logger.info(f"[V2] Page {pages}: +{new_cnt}/{len(batch_texts)} comments (cursor={bool(current_cursor)})")
        ck["cursor"] = end_cursor
        ck["vars_template"] = vars_template
        ck["cursor_key"] = cursor_key
        ck["ts"] = time.time()
        current_cursor = end_cursor
        # # === crawl replies cho các parent vừa phát hiện ===
        while reply_jobs:
            job = reply_jobs.popleft()
            parent_id = job["id"]
            parent_token = job.get("token") 

            crawl_replies_for_parent_expansion(
                driver,
                url,
                form,
                base_reply_vars=vars_template,   
                parent_id=parent_id,
                parent_token=parent_token,
                out_json=out_json,
                extract_fn=extract_replies_from_depth1_resp,
                clean_fn=clean_fb_resp_text,
                max_reply_pages=None
            )
    logger.info(f"[V2] DONE. Collected {len(all_texts)} comments → {out_json}. Checkpoint at {checkpoint_path}.")
    return all_texts
