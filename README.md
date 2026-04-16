# KidneyHood Amazon Reviews Scraper

Scrapes all Amazon customer reviews for 5 KidneyHood titles using the [Apify](https://apify.com) platform and a custom Python scraper, then exports the data to Google Sheets via the `gws` CLI.

## Books Scraped

| Title | ASIN | Avg Rating | Total Ratings |
|-------|------|------------|---------------|
| Evidence-Based Guide to Kidney Renal | B0DM96NVSX | 4.4 | 98 |
| Stopping Kidney Disease | 0692901159 | 4.3 | 1,633 |
| Stopping Kidney Disease Food Guide | 0578493624 | 4.3 | 2,635 |
| Stopping Kidney Disease Basics | 1734262419 | 4.3 | 450 |
| Kidney Failure & Transplantation | B082S6ZRVB | 4.4 | 39 |

## Output

Reviews are exported to a [Google Sheet](https://docs.google.com/spreadsheets/d/1cunfXKYY8fzDvnASMRaFVpLCEff0eSaCUxbKvf-j2Vo/edit) with one tab per book plus a Summary tab:

- **Summary** — cross-book comparison: ASIN, average rating, total ratings, rating distribution, Apify dataset IDs
- **Evidence-Based Guide** — 22 reviews
- **Stopping Kidney Disease** — 219 reviews
- **Food Guide** — 225 reviews
- **Basics** — 48 reviews
- **Kidney Failure & Transplant** — 16 reviews
- **Total: 530**

Each book sheet contains: Review ID, Date, Rating, Verified Purchase, Helpful Votes, Title, Full Review Text, Format.

## Tools Used

- **Apify actor** — `junglee/amazon-reviews-scraper` (primary — per-star filters, helpful + recent sort, keyword filter)
- **Apify actor** — `axesso_data/amazon-reviews-scraper` (supplemental — recent sort, up to 10 pages per star tier)
- **Apify actor** — `webdatalabs/amazon-reviews-scraper` (authenticated via cookies — limited by HTML parser bug)
- **Custom Python scraper** — `requests` + `BeautifulSoup` with authenticated cookies, scraping both amazon.com (US) and amazon.co.uk (UK) marketplaces
- **Apify MCP server** — connected via Claude Code for actor invocation
- **`gws` CLI** — Google Workspace CLI for Sheets API write
- **`gh` CLI** — GitHub repo management

## Files

| File | Description |
|------|-------------|
| `export_reviews.py` | Reads `reviews_data/` CSVs and prints review counts + data integrity summary |
| `export_to_sheets.py` | Initial script that created the Google Sheet and wrote hardcoded review data |
| `fix_ratings.py` | Fixes 38 empty-rating reviews from UK scrape: matches Apify/US data or falls back to `star_filter` |
| `fix_sheet.py` | Restores missing header rows and normalizes all dates to ISO `YYYY-MM-DD` format |
| `process_reviews.py` | Fetches Apify dataset files, deduplicates, normalizes, and exports to CSV |
| `update_all.py` | Copies merged CSVs into `reviews_data/`, updates `counts.json`, rewrites the Google Sheet, and updates README |
| `reviews_data/` | Final deduplicated CSVs (one per book) + `counts.json` |
| `CLAUDE.md` | Project context and workflow for Claude Code sessions |

## Workflow

```
1. Apify actors (primary + supplemental)
   └── junglee/amazon-reviews-scraper
       └── per-star filters, helpful + recent sort
   └── axesso_data/amazon-reviews-scraper (supplemental)
   └── webdatalabs/amazon-reviews-scraper (supplemental, cookie-authenticated)

2. Custom Python scraper
   └── requests + BeautifulSoup, authenticated cookies
   └── amazon.com (US) and amazon.co.uk (UK)

3. Merge and deduplicate
   └── python3 process_reviews.py
       └── output/merged/*_merged.csv

4. Load into reviews_data/ and update Google Sheet
   └── python3 update_all.py
       └── gws sheets spreadsheets values batchUpdate

5. Data fixes (post-merge)
   └── python3 fix_ratings.py  # resolve empty ratings
   └── python3 fix_sheet.py    # normalize dates, restore headers
```

## Re-running the Export

```bash
python3 update_all.py
```

Requires `gws` to be authenticated (`gws auth login`).
