"""
Fix corrupted item_section_type column by restoring from CSV.
Row N in CSV corresponds to id N in database.
"""

import csv
from pathlib import Path
import duckdb
from tqdm import tqdm

DATA_DIR = Path(__file__).parent.parent / "data"
DB_PATH = DATA_DIR / "stories.duckdb"
CSV_PATH = DATA_DIR / "scraped_posts.csv"


def main():
    if not DB_PATH.exists():
        print(f"Error: Database not found: {DB_PATH}")
        return
    
    if not CSV_PATH.exists():
        print(f"Error: CSV not found: {CSV_PATH}")
        return
    
    print(f"Reading CSV: {CSV_PATH}")
    
    # Read CSV rows
    with open(CSV_PATH, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    print(f"Found {len(rows):,} rows in CSV")
    
    # Connect to database
    print(f"Connecting to database: {DB_PATH}")
    conn = duckdb.connect(str(DB_PATH))
    
    # Update each row
    print("Updating item_section_type for each row...")
    
    updated = 0
    for i, row in enumerate(tqdm(rows, desc="Updating"), start=1):
        item_section_type = row.get('item_section_type', '')
        
        conn.execute(
            "UPDATE story SET item_section_type = ? WHERE id = ?",
            [item_section_type, i]
        )
        updated += 1
    
    conn.close()
    print(f"\n✓ Updated {updated:,} rows")


if __name__ == "__main__":
    main()

