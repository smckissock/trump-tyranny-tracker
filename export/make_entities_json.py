"""
Export top entities to JSON for search autocomplete.

Queries entity_stories_view and outputs to web/data/entities.json.gz

Usage:
    uv run python export/make_entities_json.py
"""

import sys
import json
import gzip
from pathlib import Path
import duckdb

DATA_DIR = Path(__file__).parent.parent / "data"
DEFAULT_DB = DATA_DIR / "stories.duckdb"
OUTPUT_DIR = Path(__file__).parent.parent / "web" / "data"
OUTPUT_FILE = OUTPUT_DIR / "entities.json.gz"


def main():
    if not DEFAULT_DB.exists():
        print(f"Error: Database not found: {DEFAULT_DB}")
        sys.exit(1)
    
    print(f"Connecting to database: {DEFAULT_DB}")
    conn = duckdb.connect(str(DEFAULT_DB), read_only=True)
    
    try:
        # Query the entity_stories_view
        print("Querying entity_stories_view...")
        result = conn.execute("""
            SELECT name, type, story_count, total_score, story_ids
            FROM entity_stories_view
            ORDER BY total_score DESC
        """).fetchall()
        
        print(f"Found {len(result):,} entities")
        
        # Build the JSON structure
        entities = []
        for row in result:
            name, entity_type, story_count, total_score, story_ids = row
            entities.append({
                "name": name,
                "type": entity_type,
                "storyIds": list(story_ids) if story_ids else []
            })
        
        # Ensure output directory exists
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        
        # Write gzipped JSON
        print(f"Writing to {OUTPUT_FILE}...")
        with gzip.open(OUTPUT_FILE, 'wt', encoding='utf-8') as f:
            json.dump(entities, f, separators=(',', ':'))  # Compact JSON
        
        # Also write uncompressed for debugging
        uncompressed = OUTPUT_FILE.with_suffix('')
        with open(uncompressed, 'w', encoding='utf-8') as f:
            json.dump(entities, f, indent=2)
        
        # Stats
        compressed_size = OUTPUT_FILE.stat().st_size / 1024
        uncompressed_size = uncompressed.stat().st_size / 1024
        
        print(f"\n✓ Exported {len(entities):,} entities")
        print(f"  Uncompressed: {uncompressed_size:.1f} KB")
        print(f"  Compressed:   {compressed_size:.1f} KB ({compressed_size/uncompressed_size*100:.0f}%)")
        
        # Show top 10 entities
        print(f"\nTop 10 entities by score:")
        for i, e in enumerate(entities[:10], 1):
            print(f"  {i:2}. {e['name']} ({e['type']}) - {len(e['storyIds'])} stories")
        
    finally:
        conn.close()


if __name__ == "__main__":
    main()

