"""Scraper to read the Substack archive page and extract post information."""

import re
import logging
import time
from typing import List, Dict
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ArchiveScraper:
    """Scraper for the Trump Tyranny Tracker Substack archive page."""
    
    ARCHIVE_URL = "https://trumptyrannytracker.substack.com/archive"
    
    def __init__(self):
        pass
    
    def fetch_archive_page(self) -> str:
        """Fetch the archive page HTML with infinite scroll handling."""
        logger.info(f"Fetching archive page with infinite scroll: {self.ARCHIVE_URL}")
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(self.ARCHIVE_URL, wait_until='networkidle')
            
            # Scroll to load all content
            logger.info("Scrolling to load all posts...")
            last_height = page.evaluate("document.body.scrollHeight")
            scroll_attempts = 0
            max_scroll_attempts = 50  # Safety limit
            
            while scroll_attempts < max_scroll_attempts:
                # Scroll to bottom
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(2)  # Wait for content to load
                
                # Check if new content loaded
                new_height = page.evaluate("document.body.scrollHeight")
                if new_height == last_height:
                    # Try scrolling a bit more and wait longer
                    page.evaluate("window.scrollBy(0, -500)")
                    time.sleep(1)
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    time.sleep(2)
                    new_height = page.evaluate("document.body.scrollHeight")
                    if new_height == last_height:
                        logger.info("No more content to load")
                        break
                
                last_height = new_height
                scroll_attempts += 1
                
                if scroll_attempts % 10 == 0:
                    logger.info(f"Scrolled {scroll_attempts} times, current height: {last_height}")
            
            html = page.content()
            browser.close()
            
            logger.info(f"Successfully fetched archive page (final height: {last_height})")
            return html
    
    def parse_archive_page(self, html: str) -> List[Dict[str, str]]:
        """Parse the archive page HTML and extract post information."""
        soup = BeautifulSoup(html, 'lxml')
        posts = []
        
        # Substack archive pages typically have post links in various structures
        # Look for links that contain post titles
        # Common patterns: links in article tags, divs with class containing "post" or "entry"
        
        # Try multiple selectors to find post links
        selectors = [
            'a[href*="/p/"]',  # Substack post links typically have /p/ in them
            'article a',
            '.post-preview a',
            '.entry a',
            '[class*="post"] a',
            '[class*="entry"] a',
        ]
        
        found_links = set()
        
        for selector in selectors:
            links = soup.select(selector)
            for link in links:
                href = link.get('href', '')
                if not href or href in found_links:
                    continue
                
                # Get the title text
                title_text = link.get_text(strip=True)
                
                # Skip if no title
                if not title_text:
                    continue
                
                # Make URL absolute
                full_url = urljoin(self.ARCHIVE_URL, href)
                
                # Check if this matches our criteria
                if self._is_valid_post(title_text):
                    found_links.add(href)
                    posts.append({
                        'title': title_text,
                        'url': full_url,
                        'relative_url': href
                    })
        
        # If we didn't find posts with the above selectors, try a more general approach
        if not posts:
            logger.warning("Standard selectors didn't find posts, trying alternative approach")
            # Look for any links that might be posts
            all_links = soup.find_all('a', href=True)
            for link in all_links:
                href = link.get('href', '')
                title_text = link.get_text(strip=True)
                
                if not title_text or href in found_links:
                    continue
                
                # Check if URL looks like a post URL
                if '/p/' in href or '/archive/' in href:
                    full_url = urljoin(self.ARCHIVE_URL, href)
                    if self._is_valid_post(title_text):
                        found_links.add(href)
                        posts.append({
                            'title': title_text,
                            'url': full_url,
                            'relative_url': href
                        })
        
        logger.info(f"Found {len(posts)} valid posts")
        return posts
    
    def _is_valid_post(self, title: str) -> bool:
        """
        Check if a post title matches our criteria.
        
        Valid posts:
        - Title matches pattern "Trump Tyranny Tracker: Day XXX"
        - Title is exactly "Trump Tyranny Tracker" (the first post without Day number)
        
        Invalid posts (to skip):
        - Contains "recording" (case-insensitive)
        - Contains "Recap" (case-insensitive)
        - Video posts
        """
        title_lower = title.lower()
        
        # Skip if it's a recording/video post
        if 'recording' in title_lower or 'recap' in title_lower:
            return False
        
        # Include the first post "Trump Tyranny Tracker" (without Day number)
        if title.strip() == "Trump Tyranny Tracker":
            return True
        
        # Check if it matches the pattern "Trump Tyranny Tracker: Day XXX"
        pattern = r'^Trump Tyranny Tracker:\s*Day\s+\d+'
        if re.match(pattern, title, re.IGNORECASE):
            return True
        
        return False
    
    def get_posts_to_parse(self) -> List[Dict[str, str]]:
        """Main method to fetch and parse the archive page."""
        html = self.fetch_archive_page()
        posts = self.parse_archive_page(html)
        return posts
    
    def log_posts(self, posts: List[Dict[str, str]]):
        """Log the titles and URLs of posts to be parsed."""
        logger.info(f"\n{'='*80}")
        logger.info(f"Found {len(posts)} posts to parse:")
        logger.info(f"{'='*80}\n")
        
        for i, post in enumerate(posts, 1):
            logger.info(f"{i}. {post['title']}")
            logger.info(f"   URL: {post['url']}\n")


def main():
    """Main entry point for the archive scraper."""
    scraper = ArchiveScraper()
    
    try:
        posts = scraper.get_posts_to_parse()
        scraper.log_posts(posts)
        
        logger.info(f"\nTotal posts to parse: {len(posts)}")
        return posts
        
    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()

