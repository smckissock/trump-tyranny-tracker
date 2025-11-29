"""
Export stories from DuckDB to CSV for web use.

Exports selected fields from the story table to .web/data/stories.csv
"""

import sys
import logging
from pathlib import Path
import duckdb
import pandas as pd

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent.parent / "data"
DEFAULT_DB = DATA_DIR / "stories.duckdb"
OUTPUT_DIR = Path(__file__).parent.parent / ".web" / "data"
OUTPUT_FILE = OUTPUT_DIR / "stories.csv"


def export_stories_to_csv():
    """Export stories from database to CSV file."""
    # Check database exists
    if not DEFAULT_DB.exists():
        logger.error(f"Database not found: {DEFAULT_DB}")
        sys.exit(1)
    
    logger.info(f"Connecting to database: {DEFAULT_DB}")
    conn = duckdb.connect(str(DEFAULT_DB))
    
    try:
        # Query stories with selected fields
        # For testing: only get the first post (most recent by date)
        query = """
            WITH first_post AS (
                SELECT post_name 
                FROM story 
                ORDER BY post_date DESC, id 
                LIMIT 1
            )
            SELECT 
                s.post_name,
                s.item_section_type,
                s.item_what_happened,
                s.item_why_it_matters,
                s.source_name,
                s.title,
                s.authors,
                s.image,
                s.post_date
            FROM story s
            INNER JOIN first_post fp ON s.post_name = fp.post_name
            ORDER BY s.id
        """
        
        logger.info("Querying stories from database (first post only for testing)...")
        df = conn.execute(query).df()
        
        logger.info(f"Found {len(df):,} stories to export")
        
        # Create output directory if it doesn't exist
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        
        # Write to CSV
        logger.info(f"Writing to {OUTPUT_FILE}...")
        df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
        
        logger.info(f"✓ Successfully exported {len(df):,} stories to {OUTPUT_FILE}")
        
        # Show sample of exported data
        logger.info("\nSample of exported data:")
        logger.info(f"Columns: {', '.join(df.columns)}")
        if len(df) > 0:
            logger.info(f"\nFirst row sample:")
            for col in df.columns:
                value = str(df.iloc[0][col])[:100] if pd.notna(df.iloc[0][col]) else ''
                logger.info(f"  {col}: {value}")
        
    except Exception as e:
        logger.error(f"Export failed: {e}", exc_info=True)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    export_stories_to_csv()

