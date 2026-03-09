import gzip
import hashlib
import os
import re
import sys
import time
from dataclasses import dataclass
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, Optional, Tuple

import yaml
from pymongo import MongoClient


def load_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def md5_hex(s: str) -> str:
    return hashlib.md5(s.encode("utf-8", errors="ignore")).hexdigest()


def sanitize_tsv_field(s: str) -> str:
    return (s or "").replace("\t", " ").replace("\r", " ").replace("\n", " ").strip()


class _VisibleTextExtractor(HTMLParser):
    """
    Простой HTML->text:
    - игнорирует script/style/noscript
    - собирает только текстовые узлы
    """
    def __init__(self):
        super().__init__(convert_charrefs=False)
        self._ignore_depth = 0
        self._ignore_tags = {"script", "style", "noscript"}
        self._chunks: list[str] = []
        self._in_title = False
        self.title: str = ""

    def handle_starttag(self, tag: str, attrs):
        t = (tag or "").lower()
        if t in self._ignore_tags:
            self._ignore_depth += 1
            return
        if t == "title":
            self._in_title = True
        if t in {"p", "div", "br", "hr", "li", "tr", "td", "th", "h1", "h2", "h3", "h4", "h5", "h6"}:
            self._chunks.append(" ")

    def handle_endtag(self, tag: str):
        t = (tag or "").lower()
        if t in self._ignore_tags:
            if self._ignore_depth > 0:
                self._ignore_depth -= 1
            return
        if t == "title":
            self._in_title = False
        if t in {"p", "div", "li", "tr", "td", "th"}:
            self._chunks.append(" ")

    def handle_data(self, data: str):
        if self._ignore_depth > 0:
            return
        if not data:
            return
        txt = unescape(data)
        if self._in_title:
            self.title += txt
        else:
            self._chunks.append(txt)

    def handle_entityref(self, name):
        self.handle_data(f"&{name};")

    def handle_charref(self, name):
        self.handle_data(f"&#{name};")

    def get_text(self) -> str:
        s = "".join(self._chunks)
        s = re.sub(r"\s+", " ", s).strip()
        self.title = re.sub(r"\s+", " ", (self.title or "")).strip()
        return s


def html_to_title_and_text(raw_html: str) -> Tuple[str, str]:
    parser = _VisibleTextExtractor()
    try:
        parser.feed(raw_html)
        parser.close()
    except Exception:

        title = ""
        text = re.sub(r"<[^>]+>", " ", raw_html)
        text = unescape(text)
        text = re.sub(r"\s+", " ", text).strip()
        return title, text

    text = parser.get_text()
    title = parser.title
    return title, text


@dataclass
class ExportStats:
    exported: int = 0
    skipped_empty: int = 0
    started_ts: float = time.time()


def ensure_dirs(out_dir: Path):
    (out_dir / "docs").mkdir(parents=True, exist_ok=True)
    (out_dir / "raw_html").mkdir(parents=True, exist_ok=True)


def mongo_latest_docs_cursor(db, *, match_filter: Optional[dict] = None):
    """
    Возвращает курсор, где каждый документ — последняя версия по url.
    """
    pipeline = [
        {"$match": match_filter or {"raw_html": {"$type": "string", "$ne": ""}}},
        {"$sort": {"url": 1, "crawl_ts": -1}},
        {"$group": {"_id": "$url", "doc": {"$first": "$$ROOT"}}},
        {"$replaceRoot": {"newRoot": "$doc"}},
    ]
    return db.docs.aggregate(pipeline, allowDiskUse=True)


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 export_corpus.py <config.yaml> [--with-raw-html]")
        sys.exit(1)

    cfg_path = sys.argv[1]
    with_raw = "--with-raw-html" in sys.argv[2:]

    cfg = load_config(cfg_path)

    db_cfg = cfg.get("db", {}) or {}
    uri = db_cfg.get("uri", "mongodb://localhost:27017")
    dbname = db_cfg.get("database", "searchlab")

    export_cfg = cfg.get("export", {}) or {}
    out_dir = Path(export_cfg.get("out_dir", "./corpus_out"))
    max_docs = export_cfg.get("max_docs", None)
    max_docs_int: Optional[int] = None
    if max_docs is not None:
        try:
            max_docs_int = int(max_docs)
        except Exception:
            max_docs_int = None

    client = MongoClient(uri)
    db = client[dbname]

    ensure_dirs(out_dir)

    meta_path = out_dir / "meta.tsv"
    meta_tmp = out_dir / "meta.tsv.tmp"

    match_filter = {"raw_html": {"$type": "string", "$ne": ""}}

    cur = mongo_latest_docs_cursor(db, match_filter=match_filter)

    stats = ExportStats()
    t0 = time.time()

    with meta_tmp.open("w", encoding="utf-8") as meta:
        meta.write("doc_id\turl\tsource\tcrawl_ts\ttitle\ttext_len\n")

        for doc in cur:
            url = doc.get("url", "")
            raw_html = doc.get("raw_html", "")
            source = doc.get("source", "") or ""
            crawl_ts = int(doc.get("crawl_ts", 0) or 0)

            if not url or not raw_html:
                stats.skipped_empty += 1
                continue

            doc_id = md5_hex(url)  

            title, text = html_to_title_and_text(raw_html)

            txt_path = out_dir / "docs" / f"{doc_id}.txt"
            txt_path.write_text(text, encoding="utf-8", errors="ignore")

            if with_raw:
                raw_path = out_dir / "raw_html" / f"{doc_id}.html.gz"
                with gzip.open(raw_path, "wb") as f:
                    f.write(raw_html.encode("utf-8", errors="ignore"))

            meta.write(
                f"{doc_id}\t{sanitize_tsv_field(url)}\t{sanitize_tsv_field(source)}\t{crawl_ts}\t"
                f"{sanitize_tsv_field(title)}\t{len(text)}\n"
            )

            stats.exported += 1

            if stats.exported % 500 == 0:
                dt = time.time() - t0
                speed = stats.exported / dt * 60.0 if dt > 0 else 0.0
                print(f"[export] exported={stats.exported} skipped_empty={stats.skipped_empty} speed≈{speed:.1f} docs/min")

            if max_docs_int is not None and stats.exported >= max_docs_int:
                break

    meta_tmp.replace(meta_path)

    dt = time.time() - t0
    speed = stats.exported / dt * 60.0 if dt > 0 else 0.0
    print("\nDone.")
    print(f"  out_dir: {out_dir}")
    print(f"  exported: {stats.exported}")
    print(f"  skipped_empty: {stats.skipped_empty}")
    print(f"  speed: ~{speed:.1f} docs/min")
    print(f"  meta: {meta_path}")


if __name__ == "__main__":
    main()