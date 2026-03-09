#!/usr/bin/env python3
import os
import argparse
from pathlib import Path

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--docs_dir", required=True, help="Path to docs directory (with *.txt)")
    ap.add_argument("--out_list", required=True, help="Path to docs_list.txt")
    ap.add_argument("--out_abs", required=True, help="Path to docs_list_abs.txt")
    args = ap.parse_args()

    docs_dir = Path(args.docs_dir)
    out_list = Path(args.out_list)
    out_abs = Path(args.out_abs)

    if not docs_dir.exists() or not docs_dir.is_dir():
        raise SystemExit(f"ERROR: docs_dir not found: {docs_dir}")

    out_list.parent.mkdir(parents=True, exist_ok=True)

    files = sorted(docs_dir.glob("*.txt"), key=lambda p: p.name)
    if not files:
        raise SystemExit(f"ERROR: no *.txt in {docs_dir}")

    with out_list.open("w", encoding="utf-8") as f:
        for p in files:
            f.write(str(p) + "\n")

    with out_abs.open("w", encoding="utf-8") as g:
        for p in files:
            g.write(str(p.resolve()) + "\n")

    print(f"OK: wrote {out_list} ({len(files)} files)")
    print(f"OK: wrote {out_abs} ({len(files)} files)")

if __name__ == "__main__":
    main()