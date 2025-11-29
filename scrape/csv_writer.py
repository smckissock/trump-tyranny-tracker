"""CSV writer for storing scraped post data."""

import csv
import logging
from pathlib import Path
from typing import List, Dict

logger = logging.getLogger(__name__)


class CSVWriter:
    """Writer for saving scraped post data to CSV."""
    
    CSV_COLUMNS = [
        'post_name',
        'post_author',
        'post_date',
        'item_section_type',
        'item_section',
        'item_what_happened',
        'item_why_it_matters',
        'source_name',
        'source_url'
    ]
    
    def __init__(self, output_file: str = 'data/scraped_posts.csv'):
        """Initialize CSV writer.
        
        Args:
            output_file: Path to the output CSV file
        """
        self.output_file = Path(output_file)
        self.output_file.parent.mkdir(parents=True, exist_ok=True)
        self._file_exists = self.output_file.exists()
    
    def write_post(self, post_data: Dict):
        """Write a single post's data to CSV.
        
        Args:
            post_data: Dictionary with keys: post_name, post_author, post_date, items
                where items is a list of item dictionaries
        """
        mode = 'a' if self._file_exists else 'w'
        
        with open(self.output_file, mode, newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self.CSV_COLUMNS)
            
            # Write header if new file
            if not self._file_exists:
                writer.writeheader()
                self._file_exists = True
            
            # Write each item as a row
            for item in post_data.get('items', []):
                row = {
                    'post_name': post_data.get('post_name', ''),
                    'post_author': post_data.get('post_author', ''),
                    'post_date': post_data.get('post_date', ''),
                    'item_section_type': item.get('item_section_type', ''),
                    'item_section': item.get('item_section', ''),
                    'item_what_happened': item.get('item_what_happened', ''),
                    'item_why_it_matters': item.get('item_why_it_matters', ''),
                    'source_name': item.get('source_name', ''),
                    'source_url': item.get('source_url', '')
                }
                writer.writerow(row)
        
        logger.info(f"Wrote {len(post_data.get('items', []))} items from post '{post_data.get('post_name', '')}' to {self.output_file}")
    
    def write_posts(self, posts_data: List[Dict]):
        """Write multiple posts' data to CSV.
        
        Args:
            posts_data: List of post data dictionaries
        """
        for post_data in posts_data:
            self.write_post(post_data)

