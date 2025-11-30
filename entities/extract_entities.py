"""
Extract named entities from DuckDB story collection and write to database.

Creates entity and story_entity tables for client-side search functionality.

Usage:
    python entities/extract_entities.py
"""

import sys
import io
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Tuple

# Force UTF-8 encoding for stdout/stderr on Windows
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Unbuffered output
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(line_buffering=True)

import duckdb
import pandas as pd
from tqdm import tqdm

# Try to import spacy
try:
    import spacy
except ImportError:
    print("ERROR: spacy is not installed. Install it with: uv add spacy")
    print("Then download the model with: python -m spacy download en_core_web_sm")
    sys.exit(1)

DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)
DEFAULT_DB = DATA_DIR / "stories.duckdb"

# Field weights for score calculation
FIELD_WEIGHTS = {
    'item_what_happened': 4.0,
    'item_section': 3.0,       # Curated item headline
    'item_why_it_matters': 3.0,
    'title': 2.0,
    'body': 2.0
}

# Max appearances to count per field (prevents long text from dominating)
MAX_APPEARANCES = 3

# Entity types to extract and their mappings
ENTITY_TYPES = {
    'PERSON': 'person',
    'ORG': 'organization',
    'GPE': 'location',  # Geopolitical entity (countries, cities, states)
    'PRODUCT': 'product',
    'EVENT': 'event'
}

# Common false positives to exclude
FALSE_POSITIVES = {
    'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
    'of', 'with', 'by', 'from', 'as', 'is', 'was', 'are', 'were', 'be',
    'been', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would',
    'could', 'should', 'may', 'might', 'must', 'can', 'this', 'that',
    'these', 'those', 'it', 'its', 'they', 'them', 'their', 'there',
    'here', 'where', 'when', 'what', 'who', 'which', 'how', 'why',
    'today', 'yesterday', 'tomorrow', 'now', 'then', 'ago', 'later',
    'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday',
    'January', 'February', 'March', 'April', 'May', 'June', 'July',
    'August', 'September', 'October', 'November', 'December'
}


def load_spacy_model(model_name: str = "en_core_web_lg"):
    """Load spaCy model optimized for NER only."""
    try:
        # Only load components needed for NER (much faster)
        nlp = spacy.load(model_name, disable=["tok2vec", "tagger", "parser", "attribute_ruler", "lemmatizer"])
        print(f"✓ Loaded spaCy model: {model_name} (NER-only mode)", flush=True)
        return nlp
    except OSError:
        print(f"Model '{model_name}' not found.", flush=True)
        print(f"Please download it manually with:", flush=True)
        print(f"  uv pip install https://github.com/explosion/spacy-models/releases/download/{model_name}-3.8.0/{model_name}-3.8.0-py3-none-any.whl", flush=True)
        sys.exit(1)


def is_valid_entity(text: str) -> bool:
    """Check if entity passes quality filters."""
    if not text or len(text) < 2:
        return False
    
    # Must start with uppercase
    if not text[0].isupper():
        return False
    
    # Check for false positives
    text_lower = text.lower().strip()
    if text_lower in FALSE_POSITIVES:
        return False
    
    # Exclude if it's just numbers or special characters
    if text.replace(' ', '').replace('-', '').replace("'", '').isdigit():
        return False
    
    # Exclude very short words that are likely false positives
    if len(text) <= 2 and text.lower() not in ['US', 'UK', 'EU', 'UN']:
        return False
    
    return True


def extract_entities_from_text(nlp, text: str) -> Dict[str, Tuple[str, int]]:
    """
    Extract entities from a text string.
    Returns dict mapping entity text to (entity_type, count).
    """
    if not text or not isinstance(text, str):
        return {}
    
    doc = nlp(text)
    entities = {}  # entity_text -> (entity_type, count)
    
    for ent in doc.ents:
        if ent.label_ in ENTITY_TYPES:
            entity_text = ent.text.strip()
            if is_valid_entity(entity_text):
                # Map spaCy label to our type
                entity_type = ENTITY_TYPES[ent.label_]
                if entity_text in entities:
                    # Increment count, keep same type
                    old_type, old_count = entities[entity_text]
                    entities[entity_text] = (old_type, old_count + 1)
                else:
                    entities[entity_text] = (entity_type, 1)
    
    return entities


def create_tables(conn: duckdb.DuckDBPyConnection):
    """Create entity and story_entity tables (drops existing to ensure schema is current)."""
    print("Creating tables...", flush=True)
    
    # Drop and recreate to ensure schema is current
    conn.execute("DROP TABLE IF EXISTS story_entity")
    conn.execute("DROP TABLE IF EXISTS entity")
    
    # Create entity table
    conn.execute("""
        CREATE TABLE entity (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            type VARCHAR,
            count INTEGER,
            active BOOLEAN NOT NULL DEFAULT true
        )
    """)
    
    # Create story_entity table
    conn.execute("""
        CREATE TABLE story_entity (
            story_id INTEGER,
            entity_id INTEGER,
            score REAL,
            in_item_what_happened BOOLEAN,
            in_item_section BOOLEAN,
            in_item_why_it_matters BOOLEAN,
            in_title BOOLEAN,
            in_body BOOLEAN,
            PRIMARY KEY (story_id, entity_id)
        )
    """)
    
    print("✓ Tables created", flush=True)




def get_stories(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Get all stories from the database."""
    query = """
        SELECT 
            id,
            item_what_happened,
            item_section,
            item_why_it_matters,
            title,
            body
        FROM story
        WHERE (item_what_happened IS NOT NULL AND item_what_happened != '')
           OR (item_section IS NOT NULL AND item_section != '')
           OR (item_why_it_matters IS NOT NULL AND item_why_it_matters != '')
           OR (title IS NOT NULL AND title != '')
           OR (body IS NOT NULL AND body != '')
        ORDER BY id
    """
    return conn.execute(query).df()


def extract_entities(nlp, stories_df: pd.DataFrame) -> Tuple[Dict, List]:
    """
    Extract entities from stories.
    Returns:
        - entity_dict: mapping (name, type) -> entity_id (will be assigned)
        - story_entity_rows: list of (story_id, entity_name, entity_type, score, field_flags)
    """
    # Track unique entities: (name, type) -> will get entity_id
    entity_dict: Dict[Tuple[str, str], int] = {}
    next_entity_id = 1
    
    # Track entity counts
    entity_counts: Dict[Tuple[str, str], int] = defaultdict(int)
    
    # Story-entity relationships
    story_entity_rows: List[Tuple] = []
    
    total_stories = len(stories_df)
    
    print(f"\nProcessing {total_stories:,} stories...", flush=True)
    
    # Use tqdm for progress bar
    pbar = tqdm(stories_df.iterrows(), total=total_stories, desc="Extracting entities", unit="story")
    
    for idx, row in pbar:
        story_id = int(row['id'])
        
        # Update progress bar with entity count
        pbar.set_postfix({"entities": len(entity_dict)})
        
        # Extract from each field
        entities_what = {}
        if pd.notna(row['item_what_happened']):
            entities_what = extract_entities_from_text(nlp, str(row['item_what_happened']))
        
        entities_section = {}
        if pd.notna(row['item_section']):
            entities_section = extract_entities_from_text(nlp, str(row['item_section']))
        
        entities_why = {}
        if pd.notna(row['item_why_it_matters']):
            entities_why = extract_entities_from_text(nlp, str(row['item_why_it_matters']))
        
        entities_title = {}
        if pd.notna(row['title']):
            entities_title = extract_entities_from_text(nlp, str(row['title']))
        
        entities_body = {}
        if pd.notna(row['body']):
            entities_body = extract_entities_from_text(nlp, str(row['body']))
        
        # Track which entities appear in which fields for this story
        story_entities: Dict[Tuple[str, str], Dict] = {}
        
        def init_entity_flags():
            return {
                'in_item_what_happened': False,
                'in_item_section': False,
                'in_item_why_it_matters': False,
                'in_title': False,
                'in_body': False,
                'score': 0.0
            }
        
        # Process item_what_happened
        for entity_name, (entity_type, count) in entities_what.items():
            key = (entity_name, entity_type)
            if key not in story_entities:
                story_entities[key] = init_entity_flags()
            story_entities[key]['in_item_what_happened'] = True
            capped_count = min(count, MAX_APPEARANCES)
            story_entities[key]['score'] += FIELD_WEIGHTS['item_what_happened'] * capped_count
        
        # Process item_section
        for entity_name, (entity_type, count) in entities_section.items():
            key = (entity_name, entity_type)
            if key not in story_entities:
                story_entities[key] = init_entity_flags()
            story_entities[key]['in_item_section'] = True
            capped_count = min(count, MAX_APPEARANCES)
            story_entities[key]['score'] += FIELD_WEIGHTS['item_section'] * capped_count
        
        # Process item_why_it_matters
        for entity_name, (entity_type, count) in entities_why.items():
            key = (entity_name, entity_type)
            if key not in story_entities:
                story_entities[key] = init_entity_flags()
            story_entities[key]['in_item_why_it_matters'] = True
            capped_count = min(count, MAX_APPEARANCES)
            story_entities[key]['score'] += FIELD_WEIGHTS['item_why_it_matters'] * capped_count
        
        # Process title
        for entity_name, (entity_type, count) in entities_title.items():
            key = (entity_name, entity_type)
            if key not in story_entities:
                story_entities[key] = init_entity_flags()
            story_entities[key]['in_title'] = True
            capped_count = min(count, MAX_APPEARANCES)
            story_entities[key]['score'] += FIELD_WEIGHTS['title'] * capped_count
        
        # Process body
        for entity_name, (entity_type, count) in entities_body.items():
            key = (entity_name, entity_type)
            if key not in story_entities:
                story_entities[key] = init_entity_flags()
            story_entities[key]['in_body'] = True
            capped_count = min(count, MAX_APPEARANCES)
            story_entities[key]['score'] += FIELD_WEIGHTS['body'] * capped_count
        
        # Add to entity_dict and story_entity_rows
        for (entity_name, entity_type), flags in story_entities.items():
            # Assign entity_id if new
            if (entity_name, entity_type) not in entity_dict:
                entity_dict[(entity_name, entity_type)] = next_entity_id
                next_entity_id += 1
            
            entity_id = entity_dict[(entity_name, entity_type)]
            entity_counts[(entity_name, entity_type)] += 1
            
            # Add story_entity row
            story_entity_rows.append((
                story_id,
                entity_id,
                flags['score'],
                flags['in_item_what_happened'],
                flags['in_item_section'],
                flags['in_item_why_it_matters'],
                flags['in_title'],
                flags['in_body']
            ))
        
    pbar.close()
    
    return entity_dict, story_entity_rows, entity_counts


def write_to_database(conn: duckdb.DuckDBPyConnection, entity_dict: Dict, story_entity_rows: List, entity_counts: Dict):
    """Write entities and story_entity relationships to database."""
    print(f"\nWriting to database...", flush=True)
    
    # Insert entities using DataFrame (much faster than executemany)
    print(f"Inserting {len(entity_dict):,} entities...", flush=True)
    entity_data = [
        (entity_id, name, entity_type, entity_counts[(name, entity_type)], True)
        for (name, entity_type), entity_id in entity_dict.items()
    ]
    entity_df = pd.DataFrame(entity_data, columns=['id', 'name', 'type', 'count', 'active'])
    conn.execute("INSERT INTO entity (id, name, type, count, active) SELECT * FROM entity_df")
    print(f"✓ Inserted {len(entity_df):,} entities", flush=True)
    
    # Insert story_entity relationships using DataFrame (much faster)
    print(f"Inserting {len(story_entity_rows):,} story-entity relationships...", flush=True)
    se_df = pd.DataFrame(story_entity_rows, columns=[
        'story_id', 'entity_id', 'score', 
        'in_item_what_happened', 'in_item_section', 'in_item_why_it_matters', 
        'in_title', 'in_body'
    ])
    conn.execute("INSERT INTO story_entity SELECT * FROM se_df")
    print(f"✓ Inserted {len(se_df):,} relationships", flush=True)


def print_statistics(conn: duckdb.DuckDBPyConnection):
    """Print console statistics."""
    print("\n" + "=" * 80, flush=True)
    print("EXTRACTION STATISTICS", flush=True)
    print("=" * 80, flush=True)
    
    # Total entities
    result = conn.execute("SELECT COUNT(*) FROM entity").fetchone()
    total_entities = result[0] if result else 0
    print(f"\nTotal entities: {total_entities:,}", flush=True)
    
    # Breakdown by type
    result = conn.execute("""
        SELECT type, COUNT(*) as count
        FROM entity
        GROUP BY type
        ORDER BY count DESC
    """).fetchall()
    
    print(f"\nBreakdown by type:", flush=True)
    for entity_type, count in result:
        print(f"  {entity_type:15s}: {count:6,} ({count/total_entities*100:.1f}%)", flush=True)
    
    # Total relationships
    result = conn.execute("SELECT COUNT(*) FROM story_entity").fetchone()
    total_relationships = result[0] if result else 0
    print(f"\nTotal story-entity relationships: {total_relationships:,}", flush=True)
    
    # Top 20 entities by count
    result = conn.execute("""
        SELECT name, type, count
        FROM entity
        ORDER BY count DESC
        LIMIT 20
    """).fetchall()
    
    print(f"\nTop 20 entities:", flush=True)
    for i, (name, entity_type, count) in enumerate(result, 1):
        print(f"  {i:2d}. {name:40s} ({entity_type:12s}) - {count:4,} stories", flush=True)
    
    print("=" * 80, flush=True)


def main():
    """Main function."""
    print("=" * 80, flush=True)
    print("Named Entity Extraction from Story Database", flush=True)
    print("=" * 80, flush=True)
    
    # Check database exists
    if not DEFAULT_DB.exists():
        print(f"ERROR: Database not found: {DEFAULT_DB}", flush=True)
        sys.exit(1)
    
    # Load spaCy model
    nlp = load_spacy_model()
    
    # Connect to database
    print(f"\nConnecting to database: {DEFAULT_DB}", flush=True)
    conn = duckdb.connect(str(DEFAULT_DB))
    
    # Create tables
    create_tables(conn)
    
    # Get stories
    print("Fetching stories from database...", flush=True)
    stories_df = get_stories(conn)
    print(f"✓ Found {len(stories_df):,} stories to process", flush=True)
    
    # Extract entities
    entity_dict, story_entity_rows, entity_counts = extract_entities(nlp, stories_df)
    
    # Write to database
    write_to_database(conn, entity_dict, story_entity_rows, entity_counts)
    
    # Print statistics
    print_statistics(conn)
    
    conn.close()
    print("\n✓ Extraction complete!", flush=True)


if __name__ == "__main__":
    main()
