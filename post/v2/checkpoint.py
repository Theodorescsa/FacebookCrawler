# =========================
# Checkpoint / Output
# =========================
import os, json
from datetime import datetime
def load_checkpoint(check_point_path):
    if not os.path.exists(check_point_path):
        return {"cursor": None, "seen_ids": [], "vars_template": {}, "ts": None,
                "mode": None, "slice_to": None, "slice_from": None, "year": None,
                "page": None, "min_created": None}
    try:
        with open(check_point_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {"cursor": None, "seen_ids": [], "vars_template": {}, "ts": None,
                "mode": None, "slice_to": None, "slice_from": None, "year": None,
                "page": None, "min_created": None}

def save_checkpoint(**kw):
    data = load_checkpoint(kw["check_point_path"])
    data.update(kw)
    data["ts"] = datetime.now().isoformat(timespec="seconds")
    with open(kw["check_point_path"], "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def append_ndjson(items, output_dir):
    if not items: return
    with open(output_dir, "a", encoding="utf-8") as f:
        for it in items:
            f.write(json.dumps(it, ensure_ascii=False) + "\n")

def normalize_seen_ids(seen_ids):
    return set(seen_ids or [])
