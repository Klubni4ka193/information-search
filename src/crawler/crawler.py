import io
import zipfile
from datetime import datetime, date, timedelta
import gzip
import hashlib
import logging
import random
import re
import signal
import sys
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse, urlunparse
from pymongo import ReturnDocument
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse, urlunparse
import threading


import requests
import yaml
from pymongo import ASCENDING, MongoClient
from pymongo.collection import Collection

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("crawler")

RUNNING = True


def _sig_handler(sig, frame):
    global RUNNING
    logger.info("Получен сигнал остановки. Завершусь после текущей итерации...")
    RUNNING = False


signal.signal(signal.SIGINT, _sig_handler)
signal.signal(signal.SIGTERM, _sig_handler)


def load_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def normalize_url(url: str) -> str:
    p = urlparse(url)
    scheme = (p.scheme or "https").lower()
    netloc = (p.netloc or "").lower()
    path = p.path or "/"
    while "//" in path:
        path = path.replace("//", "/")
    fragment = ""
    query = p.query
    if path != "/":
        path = path.rstrip("/") or "/"
    return urlunparse((scheme, netloc, path, "", query, fragment))


def md5_text(s: str) -> str:
    return hashlib.md5(s.encode("utf-8", errors="ignore")).hexdigest()


def get_db(cfg: Dict[str, Any]):
    uri = cfg["db"]["uri"]
    dbname = cfg["db"].get("database", "searchlab")
    client = MongoClient(uri)
    db = client[dbname]

    db.urls.create_index([("url", ASCENDING)], unique=True)
    db.urls.create_index([("next_crawl_ts", ASCENDING), ("locked_until", ASCENDING)])

    db.docs.create_index([("url", ASCENDING)])
    db.docs.create_index([("crawl_ts", ASCENDING)])
    return db



def fetch(
    session: requests.Session,
    url: str,
    etag: Optional[str],
    last_modified: Optional[str],
    timeout: int,
    ua: str,
):
    headers = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    if etag:
        headers["If-None-Match"] = etag
    if last_modified:
        headers["If-Modified-Since"] = last_modified

    last_exc: Optional[Exception] = None

    for attempt in range(5):
        try:
            r = session.get(url, headers=headers, timeout=timeout, allow_redirects=True)

            if r.status_code == 406:
                time.sleep(2.0 * (attempt + 1))
                continue

            return r.status_code, {k.lower(): v for k, v in r.headers.items()}, r.text

        except requests.RequestException as e:
            last_exc = e
            time.sleep(1.0 * (attempt + 1))

    if last_exc:
        raise last_exc
    return 406, {}, ""



_BIB_URL_RE = re.compile(r'url\s*=\s*(?:\{([^}]+)\}|"([^"]+)")', re.IGNORECASE)

_BIB_YEAR_RE = re.compile(r"year\s*=\s*\{([^}]+)\}", re.IGNORECASE)



def iter_geonames_urls(
    dump_url: str,
    countries: Optional[List[str]] = None,
    feature_classes: Optional[List[str]] = None,
    min_population: int = 0,
) -> Iterable[str]:
    resp = requests.get(dump_url, timeout=120, headers={"User-Agent": "SearchLabBot/1.0"})
    resp.raise_for_status()

    countries_set = {c.upper() for c in (countries or [])}
    feature_classes_set = {fc.upper() for fc in (feature_classes or [])}

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        inner_name = zf.namelist()[0]
        with zf.open(inner_name) as f:
            for raw in f:
                line = raw.decode("utf-8", errors="ignore").rstrip("\n")
                if not line:
                    continue

                parts = line.split("\t")
                if len(parts) < 19:
                    continue

                geoname_id = parts[0].strip()
                feature_class = parts[6].strip().upper()
                country_code = parts[8].strip().upper()

                try:
                    population = int(parts[14].strip() or 0)
                except ValueError:
                    population = 0

                if countries_set and country_code not in countries_set:
                    continue
                if feature_classes_set and feature_class not in feature_classes_set:
                    continue
                if population < int(min_population):
                    continue
                if not geoname_id:
                    continue

                yield f"https://sws.geonames.org/{geoname_id}/"



def iter_pubmed_urls(
    term: str,
    year_from: int,
    year_to: int,
    batch_size: int,
    require_abstract: bool,
    email: str,
    tool: str,
) -> Iterable[str]:
    session = requests.Session()
    base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    batch_size = max(1, min(int(batch_size), 10000))

    full_term = term.strip()
    if require_abstract:
        full_term = f"({full_term}) AND hasabstract"

    def esearch(start_d: date, end_d: date, retstart: int, retmax: int) -> Dict[str, Any]:
        params = {
            "db": "pubmed",
            "term": full_term,
            "retmode": "json",
            "retstart": retstart,
            "retmax": retmax,
            "datetype": "pdat",
            "mindate": start_d.strftime("%Y/%m/%d"),
            "maxdate": end_d.strftime("%Y/%m/%d"),
            "tool": tool,
            "email": email,
        }
        r = session.get(
            base,
            params=params,
            timeout=60,
            headers={"User-Agent": "SearchLabBot/1.0"},
        )
        r.raise_for_status()
        time.sleep(0.34)  # чтобы не вылезти за лимит NCBI
        return r.json()["esearchresult"]

    def walk(start_d: date, end_d: date):
        data = esearch(start_d, end_d, 0, 0)
        count = int(data.get("count", "0"))

        if count == 0:
            return

        if count > 10000 and start_d < end_d:
            mid = start_d + timedelta(days=(end_d - start_d).days // 2)
            yield from walk(start_d, mid)
            yield from walk(mid + timedelta(days=1), end_d)
            return

        if count > 10000 and start_d == end_d:
            logger.warning(
                "PubMed single day still too large: %s count=%d; taking first 10000 only",
                start_d.isoformat(),
                count,
            )
            count = 10000

        for retstart in range(0, count, batch_size):
            data = esearch(start_d, end_d, retstart, batch_size)
            for pmid in data.get("idlist", []):
                yield f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/"

    for y in range(int(year_from), int(year_to) + 1):
        start_d = date(y, 1, 1)
        end_d = date(y, 12, 31)
        yield from walk(start_d, end_d)


class Crawler:
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg
        self.db = get_db(cfg)
        self.urls: Collection = self.db.urls
        self.docs: Collection = self.db.docs

        logic = cfg.get("logic", {}) or {}

        self.workers = int(logic.get("workers", 1))
        self.lock_seconds = int(logic.get("lock_seconds", 120))

        self.seed_on_start = bool(logic.get("seed_on_start", True))
        self.seed_if_empty_only = bool(logic.get("seed_if_empty_only", True))

        delay = logic.get("delay_between_requests", [0.8, 1.6])
        if isinstance(delay, (int, float)):
            self.delay_min = self.delay_max = float(delay)
        else:
            self.delay_min = float(delay[0])
            self.delay_max = float(delay[1])

        self.revisit_interval = int(logic.get("revisit_interval", 7 * 86400))
        self.http_timeout = int(logic.get("http_timeout", 20))
        self.idle_sleep = int(logic.get("idle_sleep_seconds", 3))
        self.ua = str(logic.get("user_agent", "SearchLabBot/1.0 (+lab) email:your_email@example.com"))

        crawl = cfg.get("crawl", {}) or {}
        self.allowed_domains = set(d.lower() for d in (crawl.get("allowed_domains", []) or []))

        arxiv_min_interval = float(logic.get("arxiv_min_interval_seconds", 3.0))
        geonames_min_interval = float(logic.get("geonames_min_interval_seconds", 0.25))
        pubmed_min_interval = float(logic.get("pubmed_min_interval_seconds", 0.34))

        self._rate_lock = threading.Lock()
        self._next_allowed_ts: Dict[str, float] = {}
        self._min_interval_by_host = {
            "export.arxiv.org": arxiv_min_interval,
            "arxiv.org": arxiv_min_interval,
            "sws.geonames.org": geonames_min_interval,
            "pubmed.ncbi.nlm.nih.gov": pubmed_min_interval,
            "eutils.ncbi.nlm.nih.gov": pubmed_min_interval,
}
    def _rate_limit(self, url: str):
        host = urlparse(url).netloc.lower()
        min_interval = self._min_interval_by_host.get(host)
        if not min_interval:
            return

        with self._rate_lock:
            now = time.time()
            next_allowed = self._next_allowed_ts.get(host, now)
            wait = next_allowed - now
            if wait > 0: 
                time.sleep(wait)
            self._next_allowed_ts[host] = time.time() + min_interval



    def _domain_ok(self, url: str) -> bool:
        if not self.allowed_domains:
            return True
        netloc = urlparse(url).netloc.lower()
        return netloc in self.allowed_domains
    
    def upsert_url(self, url: str, source: str, next_ts: int) -> bool:
        res = self.urls.update_one(
            {"url": url},
            {"$setOnInsert": {
                "url": url,
                "source": source,
                "etag": None,
                "last_modified": None,
                "hash": None,
                "last_crawl_ts": None,
                "status_code": None,
                "first_seen_ts": int(time.time()),
                "next_crawl_ts": int(next_ts),
            }},
            upsert=True,
        )
        return res.upserted_id is not None



    def claim_next_url(self) -> Optional[Dict[str, Any]]:
        now = int(time.time())

        rec = self.urls.find_one_and_update(
            {
                "next_crawl_ts": {"$lte": now},
                "$or": [{"locked_until": {"$exists": False}}, {"locked_until": {"$lte": now}}, {"locked_until": 0}],
            },
            {"$set": {"locked_until": now + self.lock_seconds}},
            sort=[("last_crawl_ts", ASCENDING), ("next_crawl_ts", ASCENDING)],
            return_document=ReturnDocument.AFTER,
        )

        return rec

    def seed(self):
        seeding = self.cfg.get("seeding", {}) or {}
        max_urls_per_source = int(seeding.get("max_urls_per_source", 15000))
        now = int(time.time())

        before = self.urls.count_documents({})
        logger.info("DB urls before seeding: %d", before)

        def add_url(raw_url: str, source: str) -> str:
            nu = normalize_url(raw_url)
            if not self._domain_ok(nu):
                return "skip"

            inserted = self.upsert_url(nu, source, now)
            return "inserted" if inserted else "exists"

        geonames = seeding.get("geonames", {}) or {}
        geonames_dump_url = geonames.get("dump_url")

        if geonames_dump_url:
            logger.info("Seeding GeoNames from %s", geonames_dump_url)
            attempted = inserted_new = existed = skipped = 0

            for u in iter_geonames_urls(
                dump_url=geonames_dump_url,
                countries=geonames.get("countries"),
                feature_classes=geonames.get("feature_classes"),
                min_population=int(geonames.get("min_population", 0)),
            ):
                if attempted >= max_urls_per_source:
                    break
                attempted += 1

                try:
                    st = add_url(u, "geonames")
                except Exception:
                    logger.exception("Failed to upsert url: %s", u)
                    raise

                if st == "inserted":
                    inserted_new += 1
                elif st == "exists":
                    existed += 1
                else:
                    skipped += 1

            after_geonames = self.urls.count_documents({})
            logger.info(
                "GeoNames seeding done: attempted=%d inserted_new=%d already_had=%d skipped_domain=%d",
                attempted, inserted_new, existed, skipped,
            )
            logger.info("DB urls after GeoNames: %d (+%d)", after_geonames, after_geonames - before)
            before = after_geonames

        pubmed = seeding.get("pubmed", {}) or {}
        pubmed_term = str(pubmed.get("term", "")).strip()

        if pubmed_term:
            logger.info("Seeding PubMed via E-utilities: term=%s", pubmed_term)
            attempted = inserted_new = existed = skipped = 0

            for u in iter_pubmed_urls(
                term=pubmed_term,
                year_from=int(pubmed.get("year_from", 2018)),
                year_to=int(pubmed.get("year_to", 2025)),
                batch_size=int(pubmed.get("batch_size", 500)),
                require_abstract=bool(pubmed.get("require_abstract", True)),
                email=str(pubmed.get("email", "")).strip(),
                tool=str(pubmed.get("tool", "searchlab_crawler")).strip(),
            ):
                if attempted >= max_urls_per_source:
                    break
                attempted += 1

                try:
                    st = add_url(u, "pubmed")
                except Exception:
                    logger.exception("Failed to upsert url: %s", u)
                    raise

                if st == "inserted":
                    inserted_new += 1
                elif st == "exists":
                    existed += 1
                else:
                    skipped += 1

            after_pubmed = self.urls.count_documents({})
            logger.info(
                "PubMed seeding done: attempted=%d inserted_new=%d already_had=%d skipped_domain=%d",
                attempted, inserted_new, existed, skipped,
            )
            logger.info("DB urls after PubMed: %d (+%d)", after_pubmed, after_pubmed - before)
            before = after_pubmed


    def next_batch(self) -> List[Dict[str, Any]]:
        now = int(time.time())
        return list(
            self.urls.find({"next_crawl_ts": {"$lte": now}})
            .sort("next_crawl_ts", ASCENDING)
            .limit(self.batch_size)
        )

    def process_one(self, rec: Dict[str, Any], session: requests.Session):
        _id = rec["_id"]
        url = rec["url"]
        source = rec.get("source", urlparse(url).netloc)

        etag = rec.get("etag")
        last_modified = rec.get("last_modified")
        old_hash = rec.get("hash")


        logger.info("GET %s (source=%s)", url, source)

        self._rate_limit(fetch_url)

        try:
            status, headers, body = fetch(session, fetch_url, etag, last_modified, self.http_timeout, self.ua)
        except requests.RequestException as e:
            logger.warning("Request error: %s", e)
            now_ts = int(time.time())
            self.urls.update_one(
                {"_id": _id},
                {
                    "$set": {
                        "last_crawl_ts": now_ts,
                        "next_crawl_ts": now_ts + 3600,
                        "status_code": None,
                    },
                    "$unset": {"locked_until": ""},
                },
            )
            return

        now_ts = int(time.time())

        if status == 304:
            self.urls.update_one(
                {"_id": _id},
                {
                    "$set": {
                        "last_crawl_ts": now_ts,
                        "next_crawl_ts": now_ts + self.revisit_interval,
                        "status_code": 304,
                    },
                    "$unset": {"locked_until": ""},
                },
            )
            return

        if status == 406:
            logger.warning("HTTP 406 for %s (will retry later)", url)
            self.urls.update_one(
                {"_id": _id},
                {
                    "$set": {
                        "last_crawl_ts": now_ts,
                        "next_crawl_ts": now_ts + 6 * 3600,
                        "status_code": 406,
                    },
                    "$unset": {"locked_until": ""},
                },
            )
            return

        if status == 200 and body:
            new_hash = md5_text(body)
            changed = (old_hash != new_hash)

            if changed:
                self.docs.insert_one({"url": url, "raw_html": body, "source": source, "crawl_ts": now_ts})
                logger.info("saved changed doc: %s", url)
            else:
                logger.info("unchanged (md5 same): %s", url)

            self.urls.update_one(
                {"_id": _id},
                {
                    "$set": {
                        "etag": headers.get("etag"),
                        "last_modified": headers.get("last-modified"),
                        "hash": new_hash,
                        "last_crawl_ts": now_ts,
                        "next_crawl_ts": now_ts + self.revisit_interval,
                        "status_code": 200,
                    },
                    "$unset": {"locked_until": ""},
                },
            )
            return

        logger.warning("HTTP %s for %s", status, url)
        backoff = 24 * 3600
        if 500 <= status < 600:
            backoff = 2 * 3600

        self.urls.update_one(
            {"_id": _id},
            {
                "$set": {
                    "last_crawl_ts": now_ts,
                    "next_crawl_ts": now_ts + backoff,
                    "status_code": status,
                },
                "$unset": {"locked_until": ""},
            },
        )

    def run(self):
        logger.info("Start crawler")

        if self.seed_on_start:
            if self.seed_if_empty_only:
                cnt = self.urls.estimated_document_count()
                if cnt == 0:
                    self.seed()
                else:
                    logger.info("Skip seeding (urls not empty): %d", cnt)
            else:
                self.seed()

        def worker_loop(wid: int):
            logger.info("worker-%d started", wid)
            session = requests.Session()

            while RUNNING:
                rec = self.claim_next_url()
                if not rec:
                    time.sleep(self.idle_sleep)
                    continue

                self.process_one(rec, session=session)
                time.sleep(random.uniform(self.delay_min, self.delay_max))

            logger.info("worker-%d stopped", wid)

        if self.workers <= 1:
            worker_loop(0)
        else:
            with ThreadPoolExecutor(max_workers=self.workers) as ex:
                futures = [ex.submit(worker_loop, i) for i in range(self.workers)]
                for f in futures:
                    f.result()

        logger.info("Crawler stopped")




def main():
    if len(sys.argv) != 2:
        print("Usage: python crawler.py config.yaml")
        sys.exit(1)
    cfg = load_config(sys.argv[1])
    Crawler(cfg).run()


if __name__ == "__main__":
    main()