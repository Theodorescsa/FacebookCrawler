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

      function pushReq(rec) {
        try { (window.__gqlReqs || []).push(rec); } catch (e) {}
      }

      // ==== wrap fetch (có cả responseText) ====
      const _fetch = window.fetch;
      window.fetch = async function(input, init) {
        let url = '', method = 'GET', body = '';
        try {
          url = (typeof input === 'string') ? input : (input && input.url) || '';
          method = (init && init.method) || 'GET';
          body = (init && init.body) || '';
          if (body instanceof URLSearchParams) body = body.toString();
        } catch (e) {}

        const res = await _fetch.apply(this, arguments);

        try {
          if (String(url).includes('/api/graphql/')) {
            let text = '';
            try {
              text = await res.clone().text();
            } catch (e) {}

            pushReq({
              ts: Date.now(),
              type: 'fetch',
              url: String(url),
              method: String(method),
              body: String(body || ''),
              responseText: text
            });
          }
        } catch (e) {}

        return res;
      };

      // ==== wrap XHR (có cả responseText) ====
      const _open = XMLHttpRequest.prototype.open;
      const _send = XMLHttpRequest.prototype.send;

      XMLHttpRequest.prototype.open = function(method, url) {
        this.__gql_meta = {
          url: String(url || ''),
          method: String(method || 'GET')
        };
        return _open.apply(this, arguments);
      };

      XMLHttpRequest.prototype.send = function(body) {
        const self = this;
        const rawBody = body;

        this.addEventListener('load', function() {
          try {
            const meta = self.__gql_meta || {};
            if (String(meta.url).includes('/api/graphql/')) {
              pushReq({
                ts: Date.now(),
                type: 'xhr',
                url: String(meta.url),
                method: String(meta.method || 'GET'),
                body: String(rawBody || ''),
                responseText: (typeof self.responseText === 'string' ? self.responseText : '')
              });
            }
          } catch (e) {}
        });

        return _send.apply(this, arguments);
      };
    })();
    """
    driver.execute_script(js)
