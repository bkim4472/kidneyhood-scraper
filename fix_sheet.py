#!/usr/bin/env python3
"""Fix Google Sheet: restore Stopping KD header, normalize all dates to YYYY-MM-DD."""

import csv, json, re, subprocess, sys
from datetime import datetime
from pathlib import Path

SPREADSHEET_ID = "1cunfXKYY8fzDvnASMRaFVpLCEff0eSaCUxbKvf-j2Vo"
REVIEWS_DIR = Path("reviews_data")

BOOKS = [
    {"asin": "B0DM96NVSX", "csv": "evidence_based_guide.csv",     "sheet": "Evidence-Based Guide"},
    {"asin": "0692901159", "csv": "stopping_kidney_disease.csv",   "sheet": "Stopping Kidney Disease"},
    {"asin": "0578493624", "csv": "food_guide.csv",                "sheet": "Food Guide"},
    {"asin": "1734262419", "csv": "basics.csv",                    "sheet": "Basics"},
    {"asin": "B082S6ZRVB", "csv": "transplantation.csv",          "sheet": "Kidney Failure & Transplant"},
]

SHEET_HEADERS = ["Review ID", "Date", "Rating", "Verified Purchase",
                 "Helpful Votes", "Title", "Review Text", "Format"]

BOOK_META = {
    "B0DM96NVSX": {"name": "Evidence-Based Guide",        "avg": 4.4, "total": 98,   "dataset": "NanNefxJBwFh70JPM"},
    "0692901159": {"name": "Stopping Kidney Disease",     "avg": 4.3, "total": 1633, "dataset": "H3LIeUh9UBUjEj1aJ"},
    "0578493624": {"name": "Food Guide",                  "avg": 4.3, "total": 2635, "dataset": "DfKJguPyRWXRSzFT0"},
    "1734262419": {"name": "Basics",                      "avg": 4.3, "total": 450,  "dataset": "CpF3EgBraBteTaW0c"},
    "B082S6ZRVB": {"name": "Kidney Failure & Transplant", "avg": 4.4, "total": 39,   "dataset": "o2zw1XVQR0iFOouzd"},
}


def gws(*args, params=None, body=None):
    cmd = ["gws"] + list(args)
    if params is not None:
        cmd += ["--params", json.dumps(params, ensure_ascii=False)]
    if body is not None:
        cmd += ["--json", json.dumps(body, ensure_ascii=False)]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        print(f"gws error ({' '.join(args[:4])}):\n{r.stderr.strip()}", file=sys.stderr)
        sys.exit(1)
    return json.loads(r.stdout)


def normalize_date(date_str):
    """Convert 'DD Month YYYY' or already-ISO dates to YYYY-MM-DD."""
    if not date_str:
        return ""
    date_str = date_str.strip()
    if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
        return date_str
    for fmt in ("%d %B %Y", "%d %b %Y", "%-d %B %Y", "%-d %b %Y",
                "%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return date_str  # return as-is if unparseable


def clean(text):
    if not text:
        return ""
    text = (text
        .replace("&#39;", "'").replace("&#34;", '"')
        .replace("&amp;", "&").replace("&#x27;", "'")
        .replace("&#x2F;", "/"))
    m = re.match(r'^(.{40,350})\.\.\. (.{100,})', text, re.DOTALL)
    if m:
        text = m.group(2)
    return text.replace("\n", " ").strip()


def clean_format(fmt):
    if not fmt:
        return ""
    return re.sub(r'^Format:\s*', '', fmt).strip()


# ── Read and normalize all CSVs ───────────────────────────────────────────────
print("Reading and normalizing CSVs...")
book_data = {}
date_fixed_total = 0

for b in BOOKS:
    path = REVIEWS_DIR / b["csv"]
    with open(path, newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))

    fixed = 0
    for row in rows:
        orig = row.get("date", "")
        norm = normalize_date(orig)
        if norm != orig:
            row["date"] = norm
            fixed += 1

    date_fixed_total += fixed
    book_data[b["asin"]] = rows
    print(f"  {b['csv']}: {len(rows)} rows, {fixed} dates normalized")

    # Write normalized dates back to CSV
    with open(path, newline="", encoding="utf-8") as fh:
        fieldnames = csv.DictReader(fh).fieldnames

    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

print(f"  Total dates normalized: {date_fixed_total}")


# ── Rebuild all book tabs + Summary on the Sheet ──────────────────────────────
print("\nUpdating Google Sheet...")

data_ranges = []

# Per-book tabs (with guaranteed header row)
for b in BOOKS:
    rows = book_data[b["asin"]]
    sheet_rows = [SHEET_HEADERS]
    for r in rows:
        verified = r.get("verified_purchase", r.get("verified", ""))
        verified_str = "Yes" if str(verified).lower() in ("true", "yes", "1") else "No"
        sheet_rows.append([
            r.get("reviewId", ""),
            r.get("date", ""),
            int(float(r.get("rating", 0) or 0)),
            verified_str,
            int(r.get("helpful_votes", r.get("helpfulVotes", 0)) or 0),
            clean(r.get("title", "")),
            clean(r.get("body", r.get("text", ""))),
            clean_format(r.get("format", "")),
        ])
    data_ranges.append({"range": f"'{b['sheet']}'!A1", "values": sheet_rows})
    print(f"  Prepared '{b['sheet']}': {len(sheet_rows)-1} data rows + header")

# Summary tab with actual row counts (no percentages in Reviews Scraped)
summary_rows = [
    ["KidneyHood Amazon Reviews — Scrape Summary"],
    ["Generated", "2026-04-16"],
    [],
    ["Book", "ASIN", "Avg Rating", "Total Ratings", "Reviews Scraped", "Apify Dataset ID"],
]
for b in BOOKS:
    meta = BOOK_META[b["asin"]]
    count = len(book_data[b["asin"]])
    summary_rows.append([
        meta["name"], b["asin"], meta["avg"], meta["total"], count, meta["dataset"]
    ])
summary_rows += [
    [],
    ["Total Reviews", sum(len(book_data[b["asin"]]) for b in BOOKS)],
    [],
    ["Rating Distribution"],
    ["Book", "5-star %", "4-star %", "3-star %", "2-star %", "1-star %"],
    ["Evidence-Based Guide",        "73%", "13%", "10%", "3%", "3%"],
    ["Stopping Kidney Disease",     "67%", "16%", "9%",  "3%", "5%"],
    ["Food Guide",                  "66%", "16%", "10%", "4%", "4%"],
    ["Basics",                      "63%", "17%", "10%", "5%", "5%"],
    ["Kidney Failure & Transplant", "68%", "18%", "10%", "0%", "4%"],
]
data_ranges.append({"range": "Summary!A1", "values": summary_rows})

# Clear all tabs then batch-write
all_tabs = ["Summary"] + [b["sheet"] for b in BOOKS]
for tab in all_tabs:
    gws("sheets", "spreadsheets", "values", "clear",
        params={"spreadsheetId": SPREADSHEET_ID,
                "range": f"'{tab}'!A1:Z10000"})

gws("sheets", "spreadsheets", "values", "batchUpdate",
    params={"spreadsheetId": SPREADSHEET_ID},
    body={"valueInputOption": "RAW", "data": data_ranges})

print(f"\nDone! Normalized {date_fixed_total} dates, restored all headers.")
print(f"Sheet: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")
