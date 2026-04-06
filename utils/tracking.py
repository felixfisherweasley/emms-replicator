import os
import pandas as pd
import yaml
from utils.logging import setup_logging

logger = setup_logging()

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
CONFIG_PATH = os.path.join(BASE_DIR, 'config', 'config.yaml')

def load_config():
    with open(CONFIG_PATH, 'r') as file:
        return yaml.safe_load(file)

def get_tracking_dataframe():
    """Load the tracking CSV file, create if doesn't exist."""
    config = load_config()
    tracking_file = config['tracking_file']
    if not os.path.isabs(tracking_file):
        tracking_file = os.path.abspath(os.path.join(BASE_DIR, tracking_file))
    
    if os.path.exists(tracking_file):
        return pd.read_csv(tracking_file)
    else:
        # Create new tracking file with headers
        return pd.DataFrame(columns=['table_name', 'year', 'month'])

def is_loaded(table_name, year, month):
    """Check if a table/year/month combination has already been loaded."""
    df = get_tracking_dataframe()
    if df.empty:
        return False
    match = df[(df['table_name'] == table_name) & 
               (df['year'] == year) & 
               (df['month'] == month)]
    return len(match) > 0

def mark_loaded(table_name, year, month):
    """Record that a table/year/month has been successfully loaded."""
    config = load_config()
    tracking_file = config['tracking_file']
    if not os.path.isabs(tracking_file):
        tracking_file = os.path.abspath(os.path.join(BASE_DIR, tracking_file))
    os.makedirs(os.path.dirname(tracking_file), exist_ok=True)
    
    df = get_tracking_dataframe()
    
    # Check if already exists (avoid duplicates)
    if not is_loaded(table_name, year, month):
        new_row = pd.DataFrame({'table_name': [table_name], 'year': [year], 'month': [month]})
        df = pd.concat([df, new_row], ignore_index=True)
        df.to_csv(tracking_file, index=False)
        logger.info(f"Marked {table_name} {year}/{month:02d} as loaded")
    else:
        logger.debug(f"{table_name} {year}/{month:02d} already marked as loaded")
