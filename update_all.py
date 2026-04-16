#!/usr/bin/env python3
"""Update reviews_data/, counts.json, Google Sheet, README in one pass."""

import csv, json, re, shutil, subprocess, sys
from pathlib import Path

SPREADSHEET_ID = "1cunfXKYY8fzDvnASMRaFVpLCEff0eSaCUxbKvf-j2Vo"
MERGED_DIR = Path.home() / "amazon-review-scraper/output/merged"
REVIEWS_DIR = Path("reviews_data")

BOOKS = [
    {
        "asin": "B0DM96NVSX",
        "src_csv": "B0DM96NVSX_Evidence-Based_Guide_merged.csv",
        "dst_csv": "evidence_based_guide.csv",
        "sheet": "Evidence-Based Guide",
        "name": "Evidence-Based Guide to Kidney Renal",
        "avg_rating": 4.4,
        "total_ratings": 98,
        "dataset_id": "NanNefxJBwFh70JPM",
    },
    {
        "asin": "0692901159",
        "src_csv": "0692901159_Stopping_Kidney_Disease_merged.csv",
        "dst_csv": "stopping_kidney_disease.csv",
        "sheet": "Stopping Kidney Disease",
        "name": "Stopping Kidney Disease",
        "avg_rating": 4.3,
        "total_ratings": 1633,
        "dataset_id": "H3LIeUh9UBUjEj1aJ",
    },
    {
        "asin": "0578493624",
        "src_csv": "0578493624_Food_Guide_merged.csv",
        "dst_csv": "food_guide.csv",
        "sheet": "Food Guide",
        "name": "Stopping Kidney Disease Food Guide",
        "avg_rating": 4.3,
        "total_ratings": 2635,
        "dataset_id": "DfKJguPyRWXRSzFT0",
    },
    {
        "asin": "1734262419",
        "src_csv": "1734262419_Basics_merged.csv",
        "dst_csv": "basics.csv",
        "sheet": "Basics",
        "name": "Stopping Kidney Disease Basics",
        "avg_rating": 4.3,
        "total_ratings": 450,
        "dataset_id": "CpF3EgBraBteTaW0c",
    },
    {
        "asin": "B082S6ZRVB",
        "src_csv": "B082S6ZRVB_Transplantation_merged.csv",
        "dst_csv": "transplantation.csv",
        "sheet": "Kidney Failure & Transplant",
        "name": "Kidney Failure & Transplantation",
        "avg_rating": 4.4,
        "total_ratings": 39,
        "dataset_id": "o2zw1XVQR0iFOouzd",
    },
]

HEADERS = ["Review ID", "Date", "Rating", "Verified Purchase",
           "Helpful Votes", "Title", "Review Text", "Format"]


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
    """Strip 'Format:' prefix."""
    if not fmt:
        return ""
    return re.sub(r'^Format:\s*', '', fmt).strip()


def read_csv_rows(path):
    """Read CSV, return list of dicts. Handles both column naming conventions."""
    rows = []
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Normalize column names
            review_id = row.get('reviewId', row.get('review_id', ''))
            date = row.get('date', '')
            rating = row.get('rating', '')
            verified = row.get('verified_purchase', row.get('verified', ''))
            helpful = row.get('helpful_votes', row.get('helpfulVotes', '0'))
            title = row.get('title', '')
            text = row.get('body', row.get('text', ''))
            fmt = row.get('format', '')
            rows.append({
                'reviewId': review_id,
                'date': date,
                'rating': int(float(rating)) if rating else 0,
                'verified': verified,
                'helpfulVotes': int(helpful) if helpful else 0,
                'title': title,
                'text': text,
                'format': fmt,
            })
    return rows


# ── Step 1: Copy and rename CSVs ─────────────────────────────────────────────
print("Step 1: Copying CSVs to reviews_data/...")
all_reviews = {}
for b in BOOKS:
    src = MERGED_DIR / b["src_csv"]
    dst = REVIEWS_DIR / b["dst_csv"]
    shutil.copy2(src, dst)
    rows = read_csv_rows(src)
    all_reviews[b["asin"]] = rows
    print(f"  {b['dst_csv']}: {len(rows)} reviews")

# ── Step 2: Update counts.json ────────────────────────────────────────────────
print("\nStep 2: Updating counts.json...")
counts = {b["asin"]: len(all_reviews[b["asin"]]) for b in BOOKS}
counts["total"] = sum(counts.values())
with open(REVIEWS_DIR / "counts.json", "w") as f:
    json.dump(counts, f, indent=2)
print(f"  Total: {counts['total']} reviews")

# ── Step 3: Clear and rewrite Google Sheet ────────────────────────────────────
print("\nStep 3: Updating Google Sheet...")

# Clear all tabs
sheet_names = ["Summary"] + [b["sheet"] for b in BOOKS]
for name in sheet_names:
    print(f"  Clearing '{name}'...")
    gws("sheets", "spreadsheets", "values", "clear",
        params={"spreadsheetId": SPREADSHEET_ID, "range": f"'{name}'!A1:Z10000"})

# Build data ranges
data_ranges = []

# Summary tab
summary_rows = [
    ["KidneyHood Amazon Reviews — Scrape Summary"],
    ["Generated", "2026-04-16"],
    [],
    ["Book", "ASIN", "Avg Rating", "Total Ratings", "Reviews Scraped", "Apify Dataset ID"],
]
for b in BOOKS:
    n = len(all_reviews[b["asin"]])
    summary_rows.append([
        b["sheet"], b["asin"], b["avg_rating"],
        b["total_ratings"], n, b["dataset_id"]
    ])
summary_rows += [
    [],
    ["Total Reviews", counts["total"]],
    [],
    ["Rating Distribution"],
    ["Book", "5-star %", "4-star %", "3-star %", "2-star %", "1-star %"],
    ["Evidence-Based Guide",  "73%", "13%", "10%", "3%", "3%"],
    ["Stopping Kidney Disease", "67%", "16%", "9%", "3%", "5%"],
    ["Food Guide",             "66%", "16%", "10%", "4%", "4%"],
    ["Basics",                 "63%", "17%", "10%", "5%", "5%"],
    ["Kidney Failure & Transplant", "68%", "18%", "10%", "0%", "4%"],
]
data_ranges.append({"range": "Summary!A1", "values": summary_rows})

# Per-book tabs
for b in BOOKS:
    rows = [HEADERS]
    for r in all_reviews[b["asin"]]:
        verified_str = "Yes" if str(r["verified"]).lower() in ("true", "yes", "1") else "No"
        rows.append([
            r["reviewId"],
            r["date"],
            r["rating"],
            verified_str,
            r["helpfulVotes"],
            clean(r["title"]),
            clean(r["text"]),
            clean_format(r["format"]),
        ])
    data_ranges.append({"range": f"'{b['sheet']}'!A1", "values": rows})
    print(f"  Writing {len(rows)-1} rows to '{b['sheet']}'...")

print("  Sending batchUpdate...")
gws("sheets", "spreadsheets", "values", "batchUpdate",
    params={"spreadsheetId": SPREADSHEET_ID},
    body={"valueInputOption": "RAW", "data": data_ranges})

print(f"\nGoogle Sheet updated: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")

# ── Step 4: Update README.md ──────────────────────────────────────────────────
print("\nStep 4: Updating README.md...")
readme = Path("README.md").read_text()

# Update per-book review counts in Output section
replacements = [
    (r'\*\*Evidence-Based Guide\*\* — \d+ reviews.*',
     f'**Evidence-Based Guide** — {counts["B0DM96NVSX"]} reviews'),
    (r'\*\*Stopping Kidney Disease\*\* — \d+ reviews.*',
     f'**Stopping Kidney Disease** — {counts["0692901159"]} reviews'),
    (r'\*\*Food Guide\*\* — \d+ reviews.*',
     f'**Food Guide** — {counts["0578493624"]} reviews'),
    (r'\*\*Basics\*\* — \d+ reviews.*',
     f'**Basics** — {counts["1734262419"]} reviews'),
    (r'\*\*Kidney Failure & Transplant\*\* — \d+ reviews.*',
     f'**Kidney Failure & Transplant** — {counts["B082S6ZRVB"]} reviews'),
    (r'\*\*Total: \d+/\d+ \(\d+%\)\*\*',
     f'**Total: {counts["total"]}**'),
]
for pattern, repl in replacements:
    readme = re.sub(pattern, repl, readme)

Path("README.md").write_text(readme)
print("  README.md updated.")

print("\nAll done!")
print(f"  {counts['total']} total reviews across {len(BOOKS)} books")
