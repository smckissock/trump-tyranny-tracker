"""
Database migration script to add item_section field.

This script:
1. Renames item_section to item_section_type
2. Adds new item_section column
3. Preserves all existing data including enriched fields
"""

import sys
import logging
from pathlib import Path
import duckdb

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent.parent / "data"
DEFAULT_DB = DATA_DIR / "stories.duckdb"


def migrate_database():
    """Perform the database migration."""
    if not DEFAULT_DB.exists():
        logger.error(f"Database not found: {DEFAULT_DB}")
        sys.exit(1)
    
    logger.info(f"Connecting to database: {DEFAULT_DB}")
    conn = duckdb.connect(str(DEFAULT_DB))
    
    try:
        # Check current schema
        logger.info("Checking current schema...")
        columns = conn.execute("DESCRIBE story").fetchall()
        column_names = [col[0] for col in columns]
        logger.info(f"Current columns: {', '.join(column_names)}")
        
        # Step 1: Rename item_section to item_section_type if it exists
        if 'item_section' in column_names and 'item_section_type' not in column_names:
            logger.info("Renaming item_section to item_section_type...")
            conn.execute("ALTER TABLE story RENAME COLUMN item_section TO item_section_type")
            logger.info("✓ Renamed item_section to item_section_type")
        elif 'item_section_type' in column_names:
            logger.info("✓ item_section_type already exists")
        else:
            logger.warning("item_section column not found - this might be a new database")
        
        # Step 2: Add new item_section column if it doesn't exist
        if 'item_section' not in column_names:
            logger.info("Adding new item_section column...")
            conn.execute("ALTER TABLE story ADD COLUMN item_section VARCHAR")
            logger.info("✓ Added item_section column")
        else:
            logger.info("✓ item_section column already exists")
        
        # Verify final schema
        logger.info("Verifying final schema...")
        columns = conn.execute("DESCRIBE story").fetchall()
        column_names = [col[0] for col in columns]
        logger.info(f"Final columns: {', '.join(column_names)}")
        
        # Check data preservation
        result = conn.execute("SELECT COUNT(*) FROM story").fetchone()
        total_rows = result[0] if result else 0
        logger.info(f"Total rows in story table: {total_rows:,}")
        
        # Check enriched data
        result = conn.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN title IS NOT NULL AND title != '' THEN 1 ELSE 0 END) as with_title,
                SUM(CASE WHEN body IS NOT NULL AND body != '' THEN 1 ELSE 0 END) as with_body
            FROM story
        """).fetchone()
        
        if result:
            total, with_title, with_body = result
            logger.info(f"Enriched data preservation check:")
            logger.info(f"  Stories with title: {with_title:,} ({with_title/total*100:.1f}%)")
            logger.info(f"  Stories with body: {with_body:,} ({with_body/total*100:.1f}%)")
        
        logger.info("\n✓ Migration completed successfully!")
        logger.info("You can now re-run the scraper to populate the new item_section field.")
        
    except Exception as e:
        logger.error(f"Migration failed: {e}", exc_info=True)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    migrate_database()

