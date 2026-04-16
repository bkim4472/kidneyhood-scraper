# KidneyHood Amazon Reviews Scraper

## Goal
Scrape all book reviews for 5 KidneyHood titles from Amazon using Apify and a custom Python scraper, then export to Google Sheets.

## Books
1. Evidence-Based Guide to Kidney Renal (ASIN: B0DM96NVSX)
2. Stopping Kidney Disease (ASIN: 0692901159)
3. Stopping Kidney Disease Food Guide (ASIN: 0578493624)
4. Stopping Kidney Disease Basics (ASIN: 1734262419)
5. Kidney Failure & Transplantation (ASIN: B082S6ZRVB)

## Apify Actors Used
- **Primary**: `junglee/amazon-reviews-scraper` — per-star filters, helpful + recent sort, keyword filter
- **Supplemental**: `axesso_data/amazon-reviews-scraper` — recent sort, up to 10 pages per star tier
- **Supplemental**: `webdatalabs/amazon-reviews-scraper` — authenticated via cookies (limited by HTML parser bug)

## Custom Scraper
A Python scraper using `requests` + `BeautifulSoup` with authenticated cookies scraped both amazon.com (US) and amazon.co.uk (UK) marketplaces directly.

## Google Sheet
https://docs.google.com/spreadsheets/d/1cunfXKYY8fzDvnASMRaFVpLCEff0eSaCUxbKvf-j2Vo/edit

## Available Tools
- Apify MCP server for scraping (connected)
- Context7 MCP for documentation lookup (connected)
- Tavily MCP for web search (connected)
- GitHub MCP (connected)
- gws CLI for Google Sheets/Drive (run as terminal command)
- gh CLI for GitHub (run as terminal command)
- gcloud CLI available

## Workflow
1. Run Apify actors against each book ASIN (junglee primary, axesso + webdatalabs supplemental)
2. Run custom Python scraper for US + UK Amazon marketplaces
3. Merge and deduplicate into output/merged/*_merged.csv via process_reviews.py
4. Load into reviews_data/ and update Google Sheet via update_all.py
5. Apply data fixes: fix_ratings.py (empty ratings), fix_sheet.py (date normalization, headers)
6. Commit project to GitHub via gh CLI
