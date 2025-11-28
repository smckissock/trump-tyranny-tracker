"""
Enrich stories with article content using newspaper3k.

Falls back to Playwright for sites that block simple HTTP requests.

Usage:
    python enrich.py                    # Enrich first 10 stories (default for testing)
    python enrich.py --limit 20         # Process only 20 stories
    python enrich.py --all              # Process all stories
    python enrich.py --db other.duckdb  # Use different database file
    python enrich.py --login            # Login to paywalled sites
"""

import sys
import io
import time
from pathlib import Path
from typing import Any

# Force UTF-8 encoding for stdout/stderr on Windows
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Unbuffered output
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(line_buffering=True)

import duckdb
from playwright.sync_api import sync_playwright

DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)
DEFAULT_DB = DATA_DIR / "stories.duckdb"
AUTH_STATE_FILE = DATA_DIR / "enrich_auth.json"
REQUEST_DELAY = 2  # seconds between requests (different domains, so shorter delay is fine)


def get_stories_to_enrich(conn: duckdb.DuckDBPyConnection, limit: int | None = None) -> list[tuple]:
    """
    Get stories that need enrichment.
    Includes stories with errors (to retry) and stories missing enriched fields.
    Skips stories that were successfully enriched (have title, body, authors, image).
    Returns list of (story_id, source_url).
    """
    sql = """
        SELECT id, source_url
        FROM story
        WHERE source_url IS NOT NULL 
          AND source_url != ''
          AND (
              -- Has errors (retry these)
              (errors IS NOT NULL AND errors != '')
              OR
              -- Missing enriched fields (not successfully enriched yet)
              (title IS NULL OR title = '' 
               OR body IS NULL OR body = ''
               OR authors IS NULL OR authors = ''
               OR image IS NULL OR image = '')
          )
        ORDER BY id
    """
    if limit:
        sql += f" LIMIT {limit}"
    
    return conn.execute(sql).fetchall()


def process_url_newspaper(url: str) -> dict[str, Any]:
    """
    Fetch and parse article content from URL using newspaper3k.
    Returns dict with success=True/False and content or error.
    """
    try:
        from newspaper import Article, Config
        import urllib3
        import tempfile
        import os

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        # Create temp dir for newspaper3k if needed (Windows fix)
        temp_dir = os.path.join(tempfile.gettempdir(), '.newspaper_scraper')
        os.makedirs(temp_dir, exist_ok=True)

        cfg = Config()
        cfg.browser_user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0 Safari/537.36"
        cfg.request_timeout = 10
        cfg.number_threads = 1
        cfg.memoize_articles = False
        cfg.fetch_images = True

        art = Article(url, config=cfg)
        art.download()
        art.parse()

        try:
            art.nlp()
        except Exception:
            pass

        return {
            "title": (getattr(art, "title", "") or "").strip(),
            "body": getattr(art, "text", "") or "",
            "authors": ", ".join(getattr(art, "authors", []) or []),
            "image": getattr(art, "top_image", "") or "",
            "success": True,
            "error": None,
        }
    except Exception as e:
        error_msg = str(e)
        # Check if it's a 401/403 error that we can retry with Playwright
        if "401" in error_msg or "403" in error_msg:
            return {
                "success": False,
                "error": error_msg,
                "retry_with_playwright": True,
            }
        return {
            "title": None,
            "body": None,
            "authors": None,
            "image": None,
            "success": False,
            "error": error_msg,
        }


def process_url_playwright(url: str, page) -> dict[str, Any]:
    """
    Fetch page with Playwright, then parse with newspaper3k.
    """
    try:
        from newspaper import Article
        import tempfile
        import os
        
        # Navigate and wait for content
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
        time.sleep(2)  # Let JS render
        
        # Get the page HTML
        html = page.content()
        
        # Create temp dir for newspaper3k if needed (Windows fix)
        temp_dir = os.path.join(tempfile.gettempdir(), '.newspaper_scraper')
        os.makedirs(temp_dir, exist_ok=True)
        
        # Parse with newspaper3k
        art = Article(url)
        art.set_html(html)
        art.parse()
        
        try:
            art.nlp()
        except Exception:
            pass
        
        return {
            "title": (getattr(art, "title", "") or "").strip(),
            "body": getattr(art, "text", "") or "",
            "authors": ", ".join(getattr(art, "authors", []) or []),
            "image": getattr(art, "top_image", "") or "",
            "success": True,
            "error": None,
        }
    except Exception as e:
        return {
            "title": None,
            "body": None,
            "authors": None,
            "image": None,
            "success": False,
            "error": f"Playwright: {type(e).__name__}: {e}",
        }


def update_story(conn: duckdb.DuckDBPyConnection, story_id: int, article: dict[str, Any]) -> None:
    """Update story with enriched content."""
    # Only update fields if we successfully got data (not None/empty)
    # This allows enriching even if fields already have values from CSV
    title = article.get("title")
    body = article.get("body")
    authors = article.get("authors")
    image = article.get("image")
    error = article.get("error")
    
    # Build update statement dynamically based on what we have
    updates = []
    params = []
    
    if title:
        updates.append("title = ?")
        params.append(title)
    if body:
        updates.append("body = ?")
        params.append(body)
    if authors:
        updates.append("authors = ?")
        params.append(authors)
    if image:
        updates.append("image = ?")
        params.append(image)
    
    # Always update errors field
    updates.append("errors = ?")
    params.append(error)
    
    # Add story_id for WHERE clause
    params.append(story_id)
    
    if updates:
        sql = f"""
            UPDATE story
            SET {', '.join(updates)}
            WHERE id = ?
        """
        conn.execute(sql, params)


def login_for_enrichment():
    """
    Open browser for manual login to news sites and save the session.
    """
    print("=== News Site Login ===")
    print("This will open a browser window.")
    print("Log in to any paywalled sites (NYT, WSJ, etc.), then return here.\n")
    
    playwright = sync_playwright().start()
    browser = playwright.chromium.launch(headless=False)
    context = browser.new_context()
    page = context.new_page()
    
    # Open a neutral starting page
    page.goto("https://www.nytimes.com")
    
    input("Press Enter after you've logged into the sites you need...")
    
    # Save the session
    context.storage_state(path=str(AUTH_STATE_FILE))
    print(f"Session saved to {AUTH_STATE_FILE}")
    
    context.close()
    browser.close()
    playwright.stop()


def main():
    print("Starting enrich script...", flush=True)
    db_path = DEFAULT_DB
    limit = 10  # Default to 10 for testing
    do_login = False
    
    # Parse args
    print("Parsing arguments...", flush=True)
    args = sys.argv[1:]
    if "--db" in args:
        idx = args.index("--db")
        db_path = Path(args[idx + 1])
    if "--limit" in args:
        idx = args.index("--limit")
        limit = int(args[idx + 1])
    elif "--all" in args:
        # Allow --all flag to process all stories
        limit = None
    if "--login" in args:
        do_login = True
    
    # Handle login mode
    if do_login:
        login_for_enrichment()
        return
    
    # Connect to database
    print(f"Connecting to DuckDB: {db_path}", flush=True)
    if not db_path.exists():
        print(f"ERROR: Database file not found: {db_path}", flush=True)
        return
    print("Opening database connection...", flush=True)
    conn = duckdb.connect(str(db_path))
    print("Connected to database.", flush=True)
    
    # Setup Playwright lazily (only if needed for retry)
    playwright = None
    browser = None
    context = None
    page = None
    has_auth = AUTH_STATE_FILE.exists()
    
    if has_auth:
        print(f"Auth session found. Playwright will use saved session if needed for retries.", flush=True)
    else:
        print("No saved auth session. Playwright will be used as fallback without saved cookies.", flush=True)
    
    def init_playwright():
        """Lazy initialization of Playwright."""
        nonlocal playwright, browser, context, page
        if playwright is None:
            print(f"Initializing Playwright (this may take a moment)...", flush=True)
            playwright = sync_playwright().start()
            browser = playwright.chromium.launch(headless=True)
            if has_auth:
                context = browser.new_context(storage_state=str(AUTH_STATE_FILE))
            else:
                context = browser.new_context()
            page = context.new_page()
            print("Playwright ready.", flush=True)
        return page
    
    try:
        print("Querying database for stories to enrich...", flush=True)
        stories = get_stories_to_enrich(conn, limit)
        total = len(stories)
        print(f"Found {total} stories to enrich.\n", flush=True)
        
        if total == 0:
            print("Nothing to do.")
            return
        
        processed_ok = 0
        processed_fail = 0
        playwright_used = 0
        start_time = time.time()
        
        for idx, (story_id, url) in enumerate(stories, start=1):
            # Calculate time estimates
            elapsed = time.time() - start_time
            avg_time_per_story = elapsed / idx if idx > 0 else 0
            remaining_stories = total - idx
            estimated_remaining = avg_time_per_story * remaining_stories
            
            # Format time remaining
            if estimated_remaining < 60:
                time_str = f"{estimated_remaining:.0f}s"
            elif estimated_remaining < 3600:
                time_str = f"{estimated_remaining/60:.1f}m"
            else:
                hours = int(estimated_remaining // 3600)
                minutes = int((estimated_remaining % 3600) // 60)
                time_str = f"{hours}h {minutes}m"
            
            print(f"[{idx}/{total}] Story ID={story_id} (ETA: {time_str})")
            print(f"  URL: {url[:80]}..." if len(url) > 80 else f"  URL: {url}")
            
            try:
                # First try with newspaper3k
                article = process_url_newspaper(url)
                
                # If blocked, always retry with Playwright
                if article.get("retry_with_playwright"):
                    print(f"  → Retrying with Playwright...", flush=True)
                    page = init_playwright()
                    if page:
                        article = process_url_playwright(url, page)
                        playwright_used += 1
                
                update_story(conn, story_id, article)
                
                if article.get("success"):
                    processed_ok += 1
                    title = article.get("title") or ""
                    print(f"  ✓ {title[:60]}...")
                else:
                    processed_fail += 1
                    print(f"  ✗ {article.get('error', 'Unknown error')}")
                    
            except Exception as e:
                err = f"{type(e).__name__}: {e}"
                print(f"  [error] {err}")
                
                try:
                    update_story(conn, story_id, {
                        "title": None,
                        "body": None,
                        "authors": None,
                        "image": None,
                        "error": err,
                    })
                except Exception as e2:
                    print(f"  [fatal-update] {type(e2).__name__}: {e2}")
                processed_fail += 1
            
            time.sleep(REQUEST_DELAY)
            
            if idx % 50 == 0:
                conn.execute("CHECKPOINT")
                elapsed = time.time() - start_time
                rate = idx / elapsed * 60
                success_rate = processed_ok / idx * 100 if idx > 0 else 0
                print(f"\n  === Progress: {idx}/{total} ({success_rate:.1f}% success, {rate:.1f}/min) ===\n")
        
        conn.execute("CHECKPOINT")
        elapsed = time.time() - start_time
        print(f"\nDone in {elapsed/60:.1f} minutes.")
        print(f"Success: {processed_ok}, Failed: {processed_fail}")
        if playwright_used > 0:
            print(f"Playwright fallback used: {playwright_used} times")
        
    finally:
        conn.close()
        if context:
            context.close()
        if browser:
            browser.close()
        if playwright:
            playwright.stop()


if __name__ == "__main__":
    main()

