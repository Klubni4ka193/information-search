import sys
import time
import yaml
from datetime import datetime
from pymongo import MongoClient


def load_config(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def fmt_int(n: int) -> str:
    return f"{n:,}".replace(",", " ")


def main():
    if len(sys.argv) != 2:
        print("Usage: python3 monitor.py config.yaml")
        sys.exit(1)

    cfg = load_config(sys.argv[1])
    uri = cfg["db"]["uri"]
    dbname = cfg["db"].get("database", "searchlab")

    client = MongoClient(uri)
    db = client[dbname]
    urls = db.urls
    docs = db.docs

    interval = 1
    print(f"Monitoring MongoDB '{dbname}' at {uri}, refresh every {interval}s")
    print("Press Ctrl+C to stop.\n")

    prev_docs = None
    prev_ts = None

    while True:
        now = int(time.time())
        now_dt = datetime.fromtimestamp(now).strftime("%Y-%m-%d %H:%M:%S")

        total_urls = urls.count_documents({})
        due_now = urls.count_documents({"next_crawl_ts": {"$lte": now}})
        crawled_any = urls.count_documents({"last_crawl_ts": {"$ne": None}})
        never_crawled = urls.count_documents({"last_crawl_ts": None})

        ok_200 = urls.count_documents({"status_code": 200})
        not_modified_304 = urls.count_documents({"status_code": 304})
        err_4xx = urls.count_documents({"status_code": {"$gte": 400, "$lt": 500}})
        err_5xx = urls.count_documents({"status_code": {"$gte": 500}})

        by_source = list(
            urls.aggregate([
                {"$group": {"_id": "$source", "c": {"$sum": 1}}},
                {"$sort": {"c": -1}},
            ])
        )

        total_docs = docs.count_documents({})

        speed_docs_per_min = None
        if prev_docs is not None and prev_ts is not None:
            dt = max(1, now - prev_ts)
            dd = total_docs - prev_docs
            speed_docs_per_min = dd * 60.0 / dt

        docs_last_60s = docs.count_documents({"crawl_ts": {"$gte": now - 60}})

        print("\033[2J\033[H", end="")

        print(f"[{now_dt}] Mongo crawl progress\n")

        print("URL queue (collection: urls)")
        print(f"  Total URLs:             {fmt_int(total_urls)}")
        print(f"  Crawled at least once:  {fmt_int(crawled_any)}")
        print(f"  Never crawled:          {fmt_int(never_crawled)}")
        print(f"  Ready to crawl now:     {fmt_int(due_now)}  (next_crawl_ts <= now)")
        print("")
        print("URL status_code summary")
        print(f"  200 OK:                 {fmt_int(ok_200)}")
        print(f"  304 Not Modified:       {fmt_int(not_modified_304)}")
        print(f"  4xx errors:             {fmt_int(err_4xx)}")
        print(f"  5xx errors:             {fmt_int(err_5xx)}")
        print("")

        print("Docs storage (collection: docs)")
        print(f"  Total stored docs:      {fmt_int(total_docs)}")
        print(f"  Docs saved last 60s:    {fmt_int(docs_last_60s)}")
        if speed_docs_per_min is not None:
            print(f"  Approx speed:           {speed_docs_per_min:.1f} docs/min")
        print("")
        print("By source (urls)")
        for row in by_source[:10]:
            src = row["_id"] if row["_id"] is not None else "<none>"
            print(f"  {src:20s} {fmt_int(int(row['c']))}")
        if len(by_source) > 10:
            print(f"  ... ({len(by_source)-10} more)")
        print("")

        print("Interpretation for labs")
        print(f"  Remaining to download at least once: {fmt_int(never_crawled)} URLs")
        print(f"  Already have raw HTML stored:        {fmt_int(total_docs)} docs")
        print("")
        print("Tip: if 'Ready to crawl now' becomes 0, crawler is waiting for revisit schedule.\n")

        prev_docs = total_docs
        prev_ts = now
        time.sleep(interval)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped.")