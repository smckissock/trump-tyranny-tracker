"""Script to scrape all posts from the archive and save to CSV."""

import logging
import time
from scrape.archive_scraper import ArchiveScraper
from scrape.post_scraper import PostScraper
from scrape.csv_writer import CSVWriter

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Main function to scrape all posts and save to CSV."""
    # Step 1: Get all posts from archive
    logger.info("Step 1: Fetching posts from archive...")
    archive_scraper = ArchiveScraper()
    posts = archive_scraper.get_posts_to_parse()
    logger.info(f"Found {len(posts)} posts to scrape")
    
    # Step 2: Initialize scrapers and writer
    post_scraper = PostScraper()
    csv_writer = CSVWriter('data/scraped_posts.csv')
    
    # Step 3: Scrape each post
    logger.info("Step 3: Scraping individual posts...")
    for i, post in enumerate(posts, 1):
        logger.info(f"\n[{i}/{len(posts)}] Scraping: {post['title']}")
        logger.info(f"URL: {post['url']}")
        
        try:
            # Scrape the post
            post_data = post_scraper.scrape_post(post['url'])
            
            # Write to CSV
            csv_writer.write_post(post_data)
            
            logger.info(f"Successfully scraped {len(post_data.get('items', []))} items")
            
            # Be polite - add a delay between requests
            time.sleep(10)
            
        except Exception as e:
            logger.error(f"Error scraping post {post['title']}: {e}", exc_info=True)
            continue
    
    logger.info(f"\nCompleted! Data saved to {csv_writer.output_file}")


if __name__ == "__main__":
    main()

