"""List posts from the archive without scraping."""

import sys
from pathlib import Path

# Add parent directory to path so we can import from scrape package
sys.path.insert(0, str(Path(__file__).parent.parent))

from scrape.archive_scraper import ArchiveScraper


def main():
    """Run the archive scraper to log posts to be parsed."""
    scraper = ArchiveScraper()
    posts = scraper.get_posts_to_parse()
    scraper.log_posts(posts)
    return posts


if __name__ == "__main__":
    main()

