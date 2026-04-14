#!/usr/bin/env python3
"""
KidneyHood Amazon Reviews - Export Script
Reads merged CSV files from reviews_data/ and prints a summary of review counts.
Also validates data integrity (missing ratings, flags, etc.)
"""

import csv
import os
import sys
from collections import defaultdict

BOOKS = {
    "evidence_based_guide.csv":      "Evidence-Based Guide to Kidney and Renal Diets",
    "stopping_kidney_disease.csv":   "Stopping Kidney Disease",
    "food_guide.csv":                "Stopping Kidney Disease Food Guide",
    "basics.csv":                    "Stopping Kidney Disease Basics",
    "transplantation.csv":           "Kidney Failure & Transplantation",
}

TARGETS = {
    "evidence_based_guide.csv":      21,
    "stopping_kidney_disease.csv":   220,
    "food_guide.csv":                230,
    "basics.csv":                    49,
    "transplantation.csv":           16,
}

DATA_DIR = os.path.join(os.path.dirname(__file__), "reviews_data")


def load_csv(path):
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def print_summary():
    print("=" * 72)
    print("KidneyHood Amazon Reviews — Summary")
    print("=" * 72)
    print(f"{'Book':<50} {'Count':>5} {'Target':>6} {'Cover%':>7}")
    print("-" * 72)

    total_scraped = 0
    total_target = 0
    all_flagged = []

    for fname, book_name in BOOKS.items():
        fpath = os.path.join(DATA_DIR, fname)
        if not os.path.exists(fpath):
            print(f"{book_name:<50} {'MISSING':>5}")
            continue

        rows = load_csv(fpath)
        n = len(rows)
        target = TARGETS[fname]
        pct = n / target * 100
        total_scraped += n
        total_target += target

        print(f"{book_name:<50} {n:>5} {target:>6} {pct:>6.1f}%")

        # Collect flagged
        for r in rows:
            if r.get("flag"):
                all_flagged.append({**r, "_book": book_name})

    print("-" * 72)
    total_pct = total_scraped / total_target * 100
    print(f"{'TOTAL':<50} {total_scraped:>5} {total_target:>6} {total_pct:>6.1f}%")
    print("=" * 72)

    # Source breakdown
    print("\nSource breakdown per book:")
    for fname, book_name in BOOKS.items():
        fpath = os.path.join(DATA_DIR, fname)
        if not os.path.exists(fpath):
            continue
        rows = load_csv(fpath)
        by_src = defaultdict(int)
        for r in rows:
            by_src[r.get("source", "unknown")] += 1
        src_str = " | ".join(f"{s}: {c}" for s, c in sorted(by_src.items()))
        print(f"  {book_name[:45]:<45}: {src_str}")

    # Flagged reviews
    if all_flagged:
        print(f"\nFlagged reviews ({len(all_flagged)} total):")
        for r in all_flagged:
            print(f"  [{r['flag']}] {r['_book'][:30]} | {r['author'][:25]} | {r['title'][:45]} | {r['date']}")
    else:
        print("\nNo flagged reviews.")

    # Rating distribution
    print("\nRating distribution (all books combined):")
    all_ratings = defaultdict(int)
    for fname in BOOKS:
        fpath = os.path.join(DATA_DIR, fname)
        if not os.path.exists(fpath):
            continue
        for r in load_csv(fpath):
            try:
                rating = int(float(r.get("rating") or 0))
                all_ratings[rating] += 1
            except:
                all_ratings[0] += 1
    for star in sorted(all_ratings.keys(), reverse=True):
        bar = "█" * (all_ratings[star] // 5)
        print(f"  {star}★: {all_ratings[star]:>4}  {bar}")


if __name__ == "__main__":
    print_summary()
