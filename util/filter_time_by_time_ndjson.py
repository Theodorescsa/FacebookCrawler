import json

INPUT = r"E:\NCS\fb-selenium\database\post\page\nvdai0906\ACC_nvdai0906\posts_all.ndjson"
OUTPUT = "filtered.ndjson"

start_ts = 1708905600   # 26/02/2024
end_ts   = 1764115199   # 25/11/2025 23:59:59

with open(INPUT, "r", encoding="utf-8") as fin, open(OUTPUT, "w", encoding="utf-8") as fout:
    for line in fin:
        try:
            obj = json.loads(line)
            ct = obj.get("created_time")
            if isinstance(ct, int) and start_ts <= ct <= end_ts:
                fout.write(json.dumps(obj, ensure_ascii=False) + "\n")
        except:
            continue

print("Done! File saved:", OUTPUT)
