"""
Reusable function to export a DuckDB view to CSV.
"""

import csv
import gzip
import re
from pathlib import Path
from typing import Optional
import duckdb


def to_camel_case(snake_str: str) -> str:
    """Convert snake_case to camelCase."""
    components = snake_str.split('_')
    return components[0] + ''.join(x.title() for x in components[1:])


def export_view_to_csv(
    conn: duckdb.DuckDBPyConnection,
    view_name: str,
    output_dir: Path,
    filename: Optional[str] = None
) -> int:
    """
    Export a DuckDB view to CSV with camelCase column names.
    
    Args:
        conn: DuckDB connection
        view_name: Name of the view to export (e.g., 'story_view')
        output_dir: Directory to write the CSV file
        filename: Optional filename (default: view_name with '_view' stripped + '.csv')
    
    Returns:
        Number of rows exported
    """
    # Generate filename: strip '_view' suffix and add .csv
    if filename is None:
        base_name = re.sub(r'_view$', '', view_name)
        filename = f"{base_name}.csv"
    
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / filename
    gz_path = output_path.with_suffix(output_path.suffix + ".gz")
    
    # Query the view
    result = conn.execute(f"SELECT * FROM {view_name}")
    columns = [desc[0] for desc in result.description]
    js_columns = [to_camel_case(col) for col in columns]
    rows = result.fetchall()
    
    if not rows:
        print(f"No rows for view {view_name}. No files created.")
        return 0
    
    # Write both plain CSV and gzipped CSV
    with open(output_path, "w", newline="", encoding="utf-8") as f_csv, \
         gzip.open(gz_path, "wt", newline="", encoding="utf-8", compresslevel=6) as f_gz:
        w_csv = csv.writer(f_csv)
        w_gz = csv.writer(f_gz)
        w_csv.writerow(js_columns)
        w_gz.writerow(js_columns)
        for row in rows:
            w_csv.writerow(row)
            w_gz.writerow(row)
    
    row_count = len(rows)
    print(f"✓ Exported {row_count:,} rows from {view_name} to {output_path}")
    print(f"✓ Created gzipped version at {gz_path}")
    return row_count
