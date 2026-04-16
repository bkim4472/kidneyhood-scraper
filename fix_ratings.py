#!/usr/bin/env python3
"""Fix 38 empty-rating reviews in merged CSVs, reviews_data/, and Google Sheet."""

import csv, json, re, subprocess, sys
from pathlib import Path
from io import StringIO

SPREADSHEET_ID = "1cunfXKYY8fzDvnASMRaFVpLCEff0eSaCUxbKvf-j2Vo"
OUTPUT_DIR = Path.home() / "amazon-review-scraper/output"
MERGED_DIR = OUTPUT_DIR / "merged"
REVIEWS_DIR = Path("reviews_data")

STAR_MAP = {
    "five_star": 5, "four_star": 4, "three_star": 3,
    "two_star": 2, "one_star": 1,
}

BOOKS = [
    {"asin": "B0DM96NVSX", "merged_csv": "B0DM96NVSX_Evidence-Based_Guide_merged.csv",
     "dst_csv": "evidence_based_guide.csv", "sheet": "Evidence-Based Guide"},
    {"asin": "0692901159", "merged_csv": "0692901159_Stopping_Kidney_Disease_merged.csv",
     "dst_csv": "stopping_kidney_disease.csv", "sheet": "Stopping Kidney Disease"},
    {"asin": "0578493624", "merged_csv": "0578493624_Food_Guide_merged.csv",
     "dst_csv": "food_guide.csv", "sheet": "Food Guide"},
    {"asin": "1734262419", "merged_csv": "1734262419_Basics_merged.csv",
     "dst_csv": "basics.csv", "sheet": "Basics"},
    {"asin": "B082S6ZRVB", "merged_csv": "B082S6ZRVB_Transplantation_merged.csv",
     "dst_csv": "transplantation.csv", "sheet": "Kidney Failure & Transplant"},
]

SHEET_HEADERS = ["Review ID", "Date", "Rating", "Verified Purchase",
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
    if not fmt:
        return ""
    return re.sub(r'^Format:\s*', '', fmt).strip()


# ── Build rating lookups ──────────────────────────────────────────────────────
print("Building rating lookups...")

apify_ratings = {}
for f in OUTPUT_DIR.glob("apify_*.csv"):
    with open(f) as fh:
        for row in csv.DictReader(fh):
            rid = row.get("reviewId", "")
            rating = row.get("rating", "")
            if rid and rating:
                apify_ratings[rid] = int(float(rating))

helpful_ratings = {}
for f in OUTPUT_DIR.glob("*_helpful.csv"):
    with open(f) as fh:
        for row in csv.DictReader(fh):
            rid = row.get("reviewId", "")
            rating = row.get("rating", "")
            if rid and rating:
                helpful_ratings[rid] = int(float(rating))

print(f"  Apify lookup: {len(apify_ratings)} entries")
print(f"  US custom (helpful) lookup: {len(helpful_ratings)} entries")


def resolve_rating(row):
    """Return resolved integer rating, or None if already valid."""
    raw = row.get("rating", "")
    if raw not in ("", "0", "0.0"):
        return int(float(raw))
    rid = row.get("reviewId", "")
    sf = row.get("star_filter", "")
    if rid in apify_ratings:
        return apify_ratings[rid]
    if rid in helpful_ratings:
        return helpful_ratings[rid]
    if sf in STAR_MAP:
        return STAR_MAP[sf]
    return 0  # fallback (shouldn't happen)


# ── Fix merged CSVs and reviews_data/ CSVs ───────────────────────────────────
print("\nFixing CSVs...")
fixed_total = 0
all_book_rows = {}

for b in BOOKS:
    merged_path = MERGED_DIR / b["merged_csv"]
    dst_path = REVIEWS_DIR / b["dst_csv"]

    with open(merged_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        fieldnames = reader.fieldnames
        rows = list(reader)

    fixed_count = 0
    for row in rows:
        raw = row.get("rating", "")
        if raw in ("", "0", "0.0"):
            resolved = resolve_rating(row)
            row["rating"] = str(resolved)
            fixed_count += 1

    fixed_total += fixed_count
    all_book_rows[b["asin"]] = rows

    # Overwrite merged CSV
    buf = StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    merged_path.write_text(buf.getvalue(), encoding="utf-8")

    # Overwrite reviews_data/ CSV (same content as merged)
    dst_path.write_text(buf.getvalue(), encoding="utf-8")

    print(f"  {b['dst_csv']}: fixed {fixed_count} ratings")

print(f"  Total fixed: {fixed_total}")


# ── Update Google Sheet per-book tabs only ────────────────────────────────────
print("\nUpdating Google Sheet...")

data_ranges = []
for b in BOOKS:
    rows = all_book_rows[b["asin"]]
    sheet_rows = [SHEET_HEADERS]
    for r in rows:
        verified = r.get("verified_purchase", r.get("verified", ""))
        verified_str = "Yes" if str(verified).lower() in ("true", "yes", "1") else "No"
        sheet_rows.append([
            r.get("reviewId", ""),
            r.get("date", ""),
            int(float(r.get("rating", 0))),
            verified_str,
            int(r.get("helpful_votes", r.get("helpfulVotes", 0)) or 0),
            clean(r.get("title", "")),
            clean(r.get("body", r.get("text", ""))),
            clean_format(r.get("format", "")),
        ])
    data_ranges.append({"range": f"'{b['sheet']}'!A1", "values": sheet_rows})

    # Clear first, then write
    gws("sheets", "spreadsheets", "values", "clear",
        params={"spreadsheetId": SPREADSHEET_ID,
                "range": f"'{b['sheet']}'!A1:Z10000"})
    print(f"  Cleared and writing '{b['sheet']}' ({len(sheet_rows)-1} rows)...")

gws("sheets", "spreadsheets", "values", "batchUpdate",
    params={"spreadsheetId": SPREADSHEET_ID},
    body={"valueInputOption": "RAW", "data": data_ranges})

print(f"\nDone! Fixed {fixed_total} zero-rating reviews.")
print(f"Sheet: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")
