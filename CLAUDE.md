# KidneyHood Amazon Reviews Scraper

## Goal
Scrape all book reviews for 5 KidneyHood titles from Amazon using Apify.

## Books
1. Evidence-Based Guide to Kidney Renal (ASIN: B0DM96NVSX)
2. Stopping Kidney Disease (ASIN: 0692901159)
3. Stopping Kidney Disease Food Guide (ASIN: 0578493624)
4. Stopping Kidney Disease Basics (ASIN: 1734262419)
5. Kidney Failure & Transplantation (ASIN: B082S6ZRVB)

## Apify Actor
web_wanderer/amazon-reviews-extractor

## Available Tools
- Apify MCP server for scraping (connected)
- Context7 MCP for documentation lookup (connected)
- Tavily MCP for web search (connected)
- GitHub MCP (connected)
- gws CLI for Google Sheets/Drive (run as terminal command)
- gh CLI for GitHub (run as terminal command)
- gcloud CLI available

## Workflow
1. Run Apify actor against each book ASIN
2. Collect all reviews (ratings, text, dates, verified status)
3. Export to Google Sheets via gws CLI
4. Commit project to GitHub via gh CLI
