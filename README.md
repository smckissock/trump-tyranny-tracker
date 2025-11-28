# Trump Tyranny Tracker

Content scraper for https://trumptyrannytracker.substack.com/

## Overview

This project scrapes posts from the Trump Tyranny Tracker Substack newsletter, extracts structured data, and enriches it with article content from source URLs.

## Data Structure

### Scraper Fields (from Substack posts)
- `post_name` - Post title (e.g., "Trump Tyranny Tracker: Day 302")
- `post_author` - Post author
- `post_date` - Post publication date
- `item_section` - Section name (e.g., "🔥 In Corruption News")
- `item_what_happened` - Summary of what happened
- `item_why_it_matters` - Analysis of why it matters
- `source_name` - Source publication name
- `source_url` - URL to the source article

### Enriched Fields (from source article URLs)
- `title` - Article title from source URL
- `body` - Full article text from source URL
- `authors` - Article authors from source URL
- `image` - Article image from source URL
- `errors` - Any errors encountered during enrichment

## Setup

### Install Dependencies

```bash
uv sync
uv run playwright install chromium
```

## Usage

### 1. Scrape Posts from Substack

Scrapes all posts from the archive and saves to CSV:

```bash
uv run python scrape_all.py
```

This will:
- Fetch all posts from the archive page
- Scrape each individual post
- Extract items with sections, summaries, and source URLs
- Save to `data/scraped_posts.csv`
- Wait 10 seconds between posts (to be respectful)

**Output:** `data/scraped_posts.csv`

### 2. Import CSV to DuckDB

Imports the CSV data into a DuckDB database:

```bash
uv run python scrape/import_to_duckdb.py
```

This will:
- Create database at `data/stories.duckdb`
- Create `story` table with proper schema
- Import all rows from CSV
- Show statistics about imported data

**Output:** `data/stories.duckdb`

### 3. Enrich Stories with Article Content

Fetches full article content from source URLs and enriches the database:

```bash
# Enrich first 10 stories (default for testing)
uv run python scrape/enrich.py

# Enrich specific number of stories
uv run python scrape/enrich.py --limit 20

# Enrich all stories
uv run python scrape/enrich.py --all

# Use different database
uv run python scrape/enrich.py --db data/other.duckdb

# Login to paywalled sites (NYT, WSJ, etc.)
uv run python scrape/enrich.py --login
```

This will:
- Query database for stories with source URLs
- Fetch article content using newspaper3k
- Fall back to Playwright for sites that block simple HTTP requests
- Update `title`, `body`, `authors`, `image` fields
- Wait 2 seconds between requests
- Show progress with estimated time remaining

**Note:** The script defaults to processing 10 stories for testing. Use `--all` to process everything.

### 4. List Posts (No Scraping)

Just see what posts will be scraped without actually scraping:

```bash
uv run python scrape/list_posts.py
```

## Workflow

Typical workflow:

1. **Scrape posts:**
   ```bash
   uv run python scrape_all.py
   ```

2. **Import to database:**
   ```bash
   uv run python scrape/import_to_duckdb.py
   ```

3. **Test enrichment (10 stories):**
   ```bash
   uv run python scrape/enrich.py
   ```

4. **Enrich all stories:**
   ```bash
   uv run python scrape/enrich.py --all
   ```

## File Structure

```
trump-tyranny-tracker/
├── scrape/
│   ├── archive_scraper.py    # Scrapes archive page for post list
│   ├── post_scraper.py        # Scrapes individual post pages
│   ├── csv_writer.py          # Writes scraped data to CSV
│   ├── import_to_duckdb.py    # Imports CSV to DuckDB
│   ├── enrich.py              # Enriches stories with article content
│   └── list_posts.py          # Lists posts without scraping
├── scrape_all.py              # Main scraper script
└── data/
    ├── scraped_posts.csv      # Scraped data (CSV)
    └── stories.duckdb         # Database with all data
```

## Dependencies

- `beautifulsoup4` - HTML parsing
- `duckdb` - Database
- `lxml` - XML/HTML parser
- `lxml-html-clean` - HTML cleaning for newspaper3k
- `newspaper3k` - Article extraction
- `pandas` - Data manipulation
- `playwright` - Browser automation
- `requests` - HTTP requests

## Notes

- The scraper respects rate limits with delays between requests
- Enrichment uses newspaper3k first, falls back to Playwright for blocked sites
- You can save browser sessions for paywalled sites using `--login`
- The database schema separates scraper data from enriched article content
