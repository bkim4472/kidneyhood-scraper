# KidneyHood Amazon Reviews Scraper

Scrapes all Amazon customer reviews for 5 KidneyHood titles using the [Apify](https://apify.com) platform, then exports the data to Google Sheets via the `gws` CLI.

## Books Scraped

| Title | ASIN | Avg Rating | Total Ratings |
|-------|------|------------|---------------|
| Evidence-Based Guide to Kidney Renal | B0DM96NVSX | 4.4 | 98 |
| Stopping Kidney Disease | 0692901159 | 4.3 | 1,633 |
| Stopping Kidney Disease Food Guide | 0578493624 | 4.3 | 2,635 |
| Stopping Kidney Disease Basics | 1734262419 | 4.3 | 450 |
| Kidney Failure & Transplantation | B082S6ZRVB | 4.4 | 39 |

## Output

Reviews are exported to a Google Sheet with one tab per book plus a Summary tab:

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
- **Apify MCP server** — connected via Claude Code for actor invocation
- **`gws` CLI** — Google Workspace CLI for Sheets API write
- **`gh` CLI** — GitHub repo management

## Files

| File | Description |
|------|-------------|
| `export_to_sheets.py` | Writes all scraped review data to Google Sheets via `gws` |
| `CLAUDE.md` | Project context and workflow for Claude Code sessions |

## Workflow

```
1. Run Apify actor for each ASIN (parallel)
   └── web_wanderer/amazon-reviews-extractor
       └── all_stars=true, sort=recent, region=amazon.com

2. Retrieve full datasets from Apify
   └── get-actor-output (fields: date, rating, verified, helpful votes, title, text)

3. Export to Google Sheets
   └── python3 export_to_sheets.py
       └── gws sheets spreadsheets create
       └── gws sheets spreadsheets values batchUpdate
```

## Re-running the Export

```bash
# Set spreadsheet ID in export_to_sheets.py, then:
python3 export_to_sheets.py
```

Requires `gws` to be authenticated (`gws auth login`).
