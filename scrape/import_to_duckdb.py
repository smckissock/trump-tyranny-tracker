"""Script to import CSV data into DuckDB database."""

import logging
import duckdb
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_database_and_table(db_path: str = 'data/stories.duckdb'):
    """Create DuckDB database and story table.
    
    Args:
        db_path: Path to the DuckDB database file
    """
    logger.info(f"Creating database at {db_path}")
    
    # Connect to database (creates if doesn't exist)
    conn = duckdb.connect(db_path)
    
    # Create the story table
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS story (
        id INTEGER PRIMARY KEY,
        post_name VARCHAR,
        post_author VARCHAR,
        post_date VARCHAR,
        item_section VARCHAR,
        item_what_happened TEXT,
        item_why_it_matters TEXT,
        source_name VARCHAR,
        source_url VARCHAR,
        title VARCHAR,
        body TEXT,
        authors VARCHAR,
        image VARCHAR,
        errors VARCHAR
    )
    """
    
    conn.execute(create_table_sql)
    logger.info("Table 'story' created successfully")
    
    return conn


def import_csv_to_db(conn, csv_path: str = 'data/scraped_posts.csv'):
    """Import CSV data into the story table.
    
    Args:
        conn: DuckDB connection
        csv_path: Path to the CSV file
    """
    csv_file = Path(csv_path)
    
    if not csv_file.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    logger.info(f"Importing data from {csv_path}")
    
    # First, check if table has data
    result = conn.execute("SELECT COUNT(*) FROM story").fetchone()
    existing_count = result[0] if result else 0
    
    if existing_count > 0:
        logger.warning(f"Table already contains {existing_count} rows. Clearing existing data...")
        conn.execute("DELETE FROM story")
    
    # Get current max ID to continue numbering
    max_id_result = conn.execute("SELECT COALESCE(MAX(id), 0) FROM story").fetchone()
    next_id = (max_id_result[0] if max_id_result else 0) + 1
    
    # Read CSV and insert data
    # Using DuckDB's built-in CSV reading capability
    # Handle both old format (section, item_title) and new format (item_section)
    import_sql = f"""
    INSERT INTO story (
        id,
        post_name,
        post_author,
        post_date,
        item_section,
        item_what_happened,
        item_why_it_matters,
        source_name,
        source_url,
        title,
        body,
        authors,
        image,
        errors
    )
    SELECT 
        ROW_NUMBER() OVER() + {next_id - 1} as id,
        post_name,
        post_author,
        post_date,
        csv.section as item_section,
        item_what_happened,
        item_why_it_matters,
        source_name,
        source_url,
        NULL as title,
        NULL as body,
        NULL as authors,
        NULL as image,
        NULL as errors
    FROM read_csv_auto(?, header=true, auto_detect=true, null_padding=true) csv
    """
    
    conn.execute(import_sql, [str(csv_file.absolute())])
    
    # Get count of inserted rows
    result = conn.execute("SELECT COUNT(*) FROM story").fetchone()
    inserted_count = result[0] if result else 0
    
    logger.info(f"Successfully imported {inserted_count} rows into story table")
    
    return inserted_count


def main():
    """Main function to create database and import CSV."""
    db_path = 'data/stories.duckdb'
    csv_path = 'data/scraped_posts.csv'
    
    try:
        # Create database and table
        conn = create_database_and_table(db_path)
        
        # Import CSV data
        count = import_csv_to_db(conn, csv_path)
        
        # Show some sample data
        logger.info("\nSample data from story table:")
        sample = conn.execute("""
            SELECT id, post_name, item_section, source_name 
            FROM story 
            LIMIT 5
        """).fetchall()
        
        for row in sample:
            logger.info(f"  ID: {row[0]}, Post: {row[1][:50]}..., Section: {row[2][:50]}...")
        
        # Show statistics
        stats = conn.execute("""
            SELECT 
                COUNT(*) as total_stories,
                COUNT(DISTINCT post_name) as unique_posts,
                COUNT(DISTINCT item_section) as unique_sections
            FROM story
        """).fetchone()
        
        logger.info(f"\nDatabase Statistics:")
        logger.info(f"  Total stories: {stats[0]}")
        logger.info(f"  Unique posts: {stats[1]}")
        logger.info(f"  Unique sections: {stats[2]}")
        
        conn.close()
        logger.info(f"\nDatabase created successfully at {db_path}")
        
    except Exception as e:
        logger.error(f"Error importing data: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()

