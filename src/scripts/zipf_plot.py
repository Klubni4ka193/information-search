#!/usr/bin/env python3
import csv
import math
import argparse
from pathlib import Path

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--termfreq", required=True)
    ap.add_argument("--out_csv", required=True)
    ap.add_argument("--out_png", required=True)
    args = ap.parse_args()

    termfreq = Path(args.termfreq)
    out_csv = Path(args.out_csv)
    out_png = Path(args.out_png)

    items = []
    with termfreq.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            try:
                term, cnt = line.split("\t", 1)
                cnt = int(cnt)
            except:
                continue
            items.append((term, cnt))

    items.sort(key=lambda x: x[1], reverse=True)
    if not items:
        raise SystemExit("ERROR: termfreq is empty")

    k = items[0][1]

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", encoding="utf-8", newline="") as w:
        wr = csv.writer(w)
        wr.writerow(["rank","term","freq","zipf_k_over_r","log10_rank","log10_freq","log10_zipf"])
        for i, (t, f0) in enumerate(items, start=1):
            z = k / i
            wr.writerow([i, t, f0, z, math.log10(i), math.log10(f0), math.log10(z)])

    print("OK: wrote", out_csv)

    import matplotlib.pyplot as plt
    ranks = list(range(1, len(items) + 1))
    freqs = [c for _, c in items]
    zipf = [k / r for r in ranks]

    plt.figure(figsize=(8, 5))
    plt.xscale("log")
    plt.yscale("log")
    plt.plot(ranks, freqs, label="Corpus")
    plt.plot(ranks, zipf, label="Zipf k/r")
    plt.xlabel("Rank (log)")
    plt.ylabel("Frequency (log)")
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_png, dpi=200)
    print("OK: wrote", out_png)

if __name__ == "__main__":
    main()