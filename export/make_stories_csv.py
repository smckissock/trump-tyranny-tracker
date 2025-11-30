"""
Export story_view to CSV for web use.

Usage:
    uv run python export/make_stories_csv.py
"""

import sys
from pathlib import Path
import duckdb

from view_to_csv import export_view_to_csv

DATA_DIR = Path(__file__).parent.parent / "data"
DEFAULT_DB = DATA_DIR / "stories.duckdb"
OUTPUT_DIR = Path(__file__).parent.parent / "web" / "data"


def main():
    """Export story_view to CSV."""
    if not DEFAULT_DB.exists():
        print(f"Error: Database not found: {DEFAULT_DB}")
        sys.exit(1)
    
    print(f"Connecting to database: {DEFAULT_DB}")
    conn = duckdb.connect(str(DEFAULT_DB), read_only=True)
    
    try:
        row_count = export_view_to_csv(
            conn=conn,
            view_name="story_view",
            output_dir=OUTPUT_DIR
        )
        print(f"\nDone! Exported {row_count:,} stories to {OUTPUT_DIR / 'story.csv'}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
