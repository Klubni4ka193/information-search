#!/usr/bin/env python3
import os
import argparse
from pathlib import Path

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--meta_in", required=True, help="Path to meta.tsv")
    ap.add_argument("--docs_list_abs", required=True, help="Path to docs_list_abs.txt")
    ap.add_argument("--meta_out", required=True, help="Path to meta_docid.tsv")
    args = ap.parse_args()

    meta_in = Path(args.meta_in)
    docs_list_abs = Path(args.docs_list_abs)
    meta_out = Path(args.meta_out)

    if not meta_in.exists():
        raise SystemExit(f"ERROR: meta_in not found: {meta_in}")
    if not docs_list_abs.exists():
        raise SystemExit(f"ERROR: docs_list_abs not found: {docs_list_abs}")

    mapping = {}
    with docs_list_abs.open("r", encoding="utf-8", errors="ignore") as f:
        idx = 0
        for line in f:
            p = line.strip()
            if not p:
                continue
            stem = os.path.basename(p).rsplit(".", 1)[0]
            mapping[stem] = idx
            idx += 1

    written = 0
    meta_out.parent.mkdir(parents=True, exist_ok=True)

    with meta_in.open("r", encoding="utf-8", errors="ignore") as fin, \
         meta_out.open("w", encoding="utf-8") as fout:
        header = fin.readline()
        if not header:
            raise SystemExit("ERROR: meta.tsv is empty")
        fout.write(header)

        for line in fin:
            line = line.rstrip("\n")
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) < 2:
                continue

            old = parts[0]
            if old not in mapping:
                continue

            parts[0] = str(mapping[old])
            fout.write("\t".join(parts) + "\n")
            written += 1

    print(f"OK: wrote {meta_out} rows: {written}")

if __name__ == "__main__":
    main()