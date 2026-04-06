#!/usr/bin/env python3

import sys
import os
import duckdb
import pandas as pd
import yaml
from pathlib import Path

def load_config():
    with open(os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml'), 'r') as file:
        return yaml.safe_load(file)

def get_db_path():
    config = load_config()
    return Path(config['database']['path']).expanduser()

def run_query(sql):
    """Run a SQL query and display results."""
    db_path = get_db_path()
    
    if not db_path.exists():
        print(f"Database not found at {db_path}")
        return
    
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        result = conn.execute(sql).fetchall()
        columns = conn.description
        df = pd.DataFrame(result, columns=[col[0] for col in columns])
        print(df.to_string(index=False))
        print(f"\n({len(df)} rows)")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

def list_tables():
    """List all tables in the database."""
    db_path = get_db_path()
    if not db_path.exists():
        print(f"Database not found at {db_path}")
        return
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()
        print("Tables in database:")
        for table in tables:
            row_count = conn.execute(f"SELECT COUNT(*) FROM {table[0]}").fetchone()[0]
            print(f"  - {table[0]}: {row_count:,} rows")
    finally:
        conn.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python query_db.py --tables                    # List all tables")
        print("  python query_db.py --query \"SELECT ...\"       # Run a SQL query")
        print("  python query_db.py --interactive               # Interactive mode")
        sys.exit(1)
    
    if sys.argv[1] == "--tables":
        list_tables()
    elif sys.argv[1] == "--query":
        if len(sys.argv) < 3:
            print("Please provide a SQL query")
            sys.exit(1)
        sql = " ".join(sys.argv[2:])
        run_query(sql)
    elif sys.argv[1] == "--interactive":
        print("DuckDB Query Tool (type 'exit' to quit, 'tables' to list tables)")
        while True:
            try:
                sql = input("\nsql> ").strip()
                if sql.lower() == "exit":
                    break
                if sql.lower() == "tables":
                    list_tables()
                elif sql:
                    run_query(sql)
            except KeyboardInterrupt:
                break
    else:
        print(f"Unknown option: {sys.argv[1]}")
